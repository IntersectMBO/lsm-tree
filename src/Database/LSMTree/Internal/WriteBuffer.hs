{-# LANGUAGE RoleAnnotations #-}

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
    empty,
    numEntries,
    content,
    addEntryMonoidal,
    addEntryNormal,
    lookups,
    rangeLookups,
) where

import           Data.Bifunctor (Bifunctor (..))
import qualified Data.Map.Range as Map.R
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Common (Range (..))
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Monoidal as Monoidal
import qualified Database.LSMTree.Internal.Normal as Normal
import           Database.LSMTree.Internal.Serialise

{-------------------------------------------------------------------------------
  Writebuffer type
-------------------------------------------------------------------------------}

-- | The phantom type parameters provide some safety, enforcing that the types
-- of inserted entries are consistent.
--
-- TODO: Revisit this when using the write buffer from the table handle.
-- It would be consistent with other internal APIs (e.g. for @Run@ and
-- @CompactIndex@ to remove the type parameters here and move the responsibility
-- for these constraints and (de)serialisation to the layer above.
newtype WriteBuffer k v blob =
  WB { unWB :: Map SerialisedKey (Entry SerialisedValue SerialisedBlob) }
  deriving (Show)
type role WriteBuffer nominal nominal nominal

empty :: WriteBuffer k v blob
empty = WB Map.empty

numEntries :: WriteBuffer k v blob -> NumEntries
numEntries (WB m) = NumEntries (Map.size m)

-- | \( O(n) \)
content :: WriteBuffer k v blob ->
           [(SerialisedKey, Entry SerialisedValue SerialisedBlob)]
content (WB m) = Map.assocs m

{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

addEntryMonoidal :: (SerialiseKey k, SerialiseValue v)
  => (SerialisedValue -> SerialisedValue -> SerialisedValue) -- ^ merge function
  -> k -> Monoidal.Update v -> WriteBuffer k v blob -> WriteBuffer k v blob
addEntryMonoidal f k e (WB wb) =
    WB (Map.insertWith (combine f) (serialiseKey k) (first serialiseValue (updateToEntryMonoidal e)) wb)

addEntryNormal :: (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => k -> Normal.Update v blob -> WriteBuffer k v blob -> WriteBuffer k v blob
addEntryNormal k e (WB wb) =
    WB (Map.insert (serialiseKey k) (bimap serialiseValue serialiseBlob (updateToEntryNormal e)) wb)

{-------------------------------------------------------------------------------
  Querying
-------------------------------------------------------------------------------}

-- We return an 'Entry' with serialised values, so it can be properly combined
-- with the lookups in other runs. Deserialisation only occurs afterwards.
--
-- Note: the entry may be 'Delete'.
--
lookups :: SerialiseKey k
  => WriteBuffer k v blob
  -> [k]
  -> [(SerialisedKey, Maybe (Entry SerialisedValue SerialisedBlob))]
lookups (WB m) = fmap (f . serialiseKey)
  where
    f k = (k, Map.lookup k m)

{-------------------------------------------------------------------------------
  RangeQueries
-------------------------------------------------------------------------------}

-- | We return 'Entry' instead of either @RangeLookupResult@,
-- so we can properly combine lookup results.
--
-- Note: 'Delete's are not filtered out.
--
rangeLookups :: SerialiseKey k
  => WriteBuffer k v blob
  -> Range k
  -> [(SerialisedKey, Entry SerialisedValue SerialisedBlob)]
rangeLookups (WB m) r =
    [ (k, e)
    | let (lb, ub) = convertRange (fmap serialiseKey r)
    , (k, e) <- Map.R.rangeLookup lb ub m
    ]

convertRange :: Range k -> (Map.R.Bound k, Map.R.Bound k)
convertRange (FromToExcluding lb ub) =
    ( Map.R.Bound lb Map.R.Inclusive
    , Map.R.Bound ub Map.R.Exclusive )
convertRange (FromToIncluding lb ub) =
    ( Map.R.Bound lb Map.R.Inclusive
    , Map.R.Bound ub Map.R.Inclusive )
