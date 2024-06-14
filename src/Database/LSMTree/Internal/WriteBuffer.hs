{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}

-- | The in-memory LSM level 0.
--
-- === TODO
--
-- This is temporary module header documentation. The module will be
-- fleshed out more as we implement bits of it.
--
-- Related work packages: 5
--
-- This module includes in-memory parts parts for, amongst others,
--
-- * Incremental construction
--
-- * Updates (inserts, deletes, mupserts)
--
-- * Queries (lookups, range lookups)
--
-- The above list is a sketch. Functionality may move around, and the list is
-- not exhaustive.
--
module Database.LSMTree.Internal.WriteBuffer (
    WriteBuffer (..),
    writeBufferInvariant,
    empty,
    numEntries,
    sizeInBytes,
    fromMap,
    fromList,
    toList,
    addEntries,
    addEntry,
    addEntryMonoidal,
    addEntryNormal,
    lookups,
    lookup,
    lookups',
    rangeLookups,
) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.Trans.State.Strict (modify', runState)
import qualified Data.Foldable as Foldable
import           Data.List (foldl')
import qualified Data.Map.Range as Map.R
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Database.LSMTree.Internal.BlobRef (BlobRef)
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Monoidal as Monoidal
import qualified Database.LSMTree.Internal.Normal as Normal
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.Vector as V
import           Prelude hiding (lookup)

{-------------------------------------------------------------------------------
  Writebuffer type
-------------------------------------------------------------------------------}

data WriteBuffer = WB {
    writeBufferContent   :: !(Map SerialisedKey (Entry SerialisedValue SerialisedBlob))
    -- | Keeps track of the total size of keys and values in the buffer.
    -- This means reconstructing the 'WB' constructor on every update.
  , writeBufferSizeBytes :: {-# UNPACK #-} !Int
  }
  deriving stock (Eq, Show)

instance NFData WriteBuffer where
  rnf (WB m _) = rnf m

writeBufferInvariant :: WriteBuffer -> Bool
writeBufferInvariant (WB m s) =
    s == sum (sizeofKey <$> keys) + sum (sizeofValue <$> values)
  where
    keys = Map.keys m
    values = foldMap (onValue [] pure) m :: [SerialisedValue]

empty :: WriteBuffer
empty = WB Map.empty 0

numEntries :: WriteBuffer -> NumEntries
numEntries (WB m _) = NumEntries (Map.size m)

sizeInBytes :: WriteBuffer -> Int
sizeInBytes = writeBufferSizeBytes

fromMap ::
     Map SerialisedKey (Entry SerialisedValue SerialisedBlob)
  -> WriteBuffer
fromMap m = WB m (Map.foldlWithKey' (\s k e -> s + sizeofKOp k e) 0 m)
  where
    sizeofKOp k e = sizeofKey k + onValue 0 sizeofValue e

fromList ::
     (SerialisedValue -> SerialisedValue -> SerialisedValue) -- ^ merge function
  -> [(SerialisedKey, Entry SerialisedValue SerialisedBlob)]
  -> WriteBuffer
fromList f es = fromMap (Map.fromListWith (combine f) es)

-- | \( O(n) \)
toList :: WriteBuffer -> [(SerialisedKey, Entry SerialisedValue SerialisedBlob)]
toList (WB m _) = Map.assocs m

{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

-- TODO(optimisation): make sure the core looks reasonable, e.g. not allocating
-- 'WB' for every entry.
addEntries ::
     Foldable f
  => (SerialisedValue -> SerialisedValue -> SerialisedValue) -- ^ merge function
  -> f (SerialisedKey, Entry SerialisedValue SerialisedBlob)
  -> WriteBuffer
  -> WriteBuffer
addEntries f es wb = foldl' (flip (uncurry (addEntry f))) wb (Foldable.toList es)

addEntry ::
     (SerialisedValue -> SerialisedValue -> SerialisedValue) -- ^ merge function
  -> SerialisedKey
  -> Entry SerialisedValue SerialisedBlob
  -> WriteBuffer
  -> WriteBuffer
addEntry f k e (WB wb s) =
    let (!wb', !s') = runState (insert k wb) s
    in WB wb' s'
  where
    -- TODO: this seems inelegant, but we want to avoid traversing the Map twice
    insert = Map.alterF $ (fmap Just .) $ \case
        Nothing -> do
          modify' (+ (sizeofKey k + sizeofEntry e))
          return e
        Just old -> do
          let !new = combine f e old
          -- don't count key (it was already there), only count value difference
          modify' (+ (sizeofEntry new - sizeofEntry old))
          return new
    sizeofEntry = onValue 0 sizeofValue

addEntryMonoidal ::
     (SerialisedValue -> SerialisedValue -> SerialisedValue) -- ^ merge function
  -> SerialisedKey
  -> Monoidal.Update SerialisedValue
  -> WriteBuffer
  -> WriteBuffer
addEntryMonoidal f k = addEntry f k . updateToEntryMonoidal

addEntryNormal ::
     SerialisedKey
  -> Normal.Update SerialisedValue SerialisedBlob
  -> WriteBuffer
  -> WriteBuffer
addEntryNormal k = addEntry const k . updateToEntryNormal

{-------------------------------------------------------------------------------
  Querying
-------------------------------------------------------------------------------}

-- We return an 'Entry' with serialised values, so it can be properly combined
-- with the lookups in other runs. Deserialisation only occurs afterwards.
--
-- Note: the entry may be 'Delete'.
--
lookups ::
     WriteBuffer
  -> V.Vector SerialisedKey
  -> V.Vector (Maybe (Entry SerialisedValue SerialisedBlob))
lookups (WB m _) !ks = V.mapStrict (`Map.lookup` m) ks

-- | TODO: update once blob references are implemented
lookup :: WriteBuffer -> SerialisedKey -> Maybe (Entry SerialisedValue (BlobRef run))
lookup (WB m _) !k = case Map.lookup k m of
    Nothing -> Nothing
    Just x  -> Just $! errOnBlob x

-- | TODO: remove 'lookups' or 'lookups'', depending on which one we end up
-- using, once blob references are implemented
lookups' ::
     WriteBuffer
  -> V.Vector SerialisedKey
  -> V.Vector (Maybe (Entry SerialisedValue (BlobRef run)))
lookups' wb !ks = V.mapStrict (lookup wb) ks

-- | TODO: remove once blob references are implemented
errOnBlob :: Entry v blobref1 -> Entry v blobref2
errOnBlob (Insert v)           = Insert v
errOnBlob (InsertWithBlob _ _) = error "lookups: blob references not supported"
errOnBlob (Mupdate v)          = Mupdate v
errOnBlob Delete               = Delete

{-------------------------------------------------------------------------------
  RangeQueries
-------------------------------------------------------------------------------}

-- | We return 'Entry' instead of either @RangeLookupResult@,
-- so we can properly combine lookup results.
--
-- Note: 'Delete's are not filtered out.
--
rangeLookups ::
     WriteBuffer
  -> Range SerialisedKey
  -> [(SerialisedKey, Entry SerialisedValue SerialisedBlob)]
rangeLookups (WB m _) r =
    [ (k, e)
    | let (lb, ub) = convertRange r
    , (k, e) <- Map.R.rangeLookup lb ub m
    ]

convertRange :: Range k -> (Map.R.Bound k, Map.R.Bound k)
convertRange (FromToExcluding lb ub) =
    ( Map.R.Bound lb Map.R.Inclusive
    , Map.R.Bound ub Map.R.Exclusive )
convertRange (FromToIncluding lb ub) =
    ( Map.R.Bound lb Map.R.Inclusive
    , Map.R.Bound ub Map.R.Inclusive )
