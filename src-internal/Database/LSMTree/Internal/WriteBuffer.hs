{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}

-- | The in-memory LSM level 0.
module Database.LSMTree.Internal.WriteBuffer (
    WriteBuffer,
    empty,
    numEntries,
    fromMap,
    toMap,
    fromList,
    toList,
    addEntry,
    null,
    lookups,
    lookup,
    rangeLookups,
) where

import           Control.DeepSeq (NFData (..))
import qualified Data.Map.Range as Map.R
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Database.LSMTree.Internal.BlobRef (BlobSpan)
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.Vector as V
import           Prelude hiding (lookup, null)

{-------------------------------------------------------------------------------
  Writebuffer type
-------------------------------------------------------------------------------}

newtype WriteBuffer =
  WB { unWB :: Map SerialisedKey (Entry SerialisedValue BlobSpan) }
  deriving stock (Eq, Show)
  deriving newtype NFData

empty :: WriteBuffer
empty = WB Map.empty

-- | \( O(1) \)
numEntries :: WriteBuffer -> NumEntries
numEntries (WB m) = NumEntries (Map.size m)

-- | \( O(1)) \)
fromMap ::
     Map SerialisedKey (Entry SerialisedValue BlobSpan)
  -> WriteBuffer
fromMap m = WB m

-- | \( O(1) \)
toMap :: WriteBuffer -> Map SerialisedKey (Entry SerialisedValue BlobSpan)
toMap = unWB

-- | \( O(n \log n) \)
fromList ::
     (SerialisedValue -> SerialisedValue -> SerialisedValue) -- ^ merge function
  -> [(SerialisedKey, Entry SerialisedValue BlobSpan)]
  -> WriteBuffer
fromList f es = WB $ Map.fromListWith (combine f) es

-- | \( O(n) \)
toList :: WriteBuffer -> [(SerialisedKey, Entry SerialisedValue BlobSpan)]
toList (WB m) = Map.assocs m

{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

addEntry ::
     (SerialisedValue -> SerialisedValue -> SerialisedValue) -- ^ merge function
  -> SerialisedKey
  -> Entry SerialisedValue BlobSpan
  -> WriteBuffer
  -> WriteBuffer
addEntry f k e (WB wb) =
    WB (Map.insertWith (combine f) k e wb)

{-------------------------------------------------------------------------------
  Querying
-------------------------------------------------------------------------------}

null :: WriteBuffer -> Bool
null (WB m) = Map.null m

-- We return an 'Entry' with serialised values, so it can be properly combined
-- with the lookups in other runs. Deserialisation only occurs afterwards.
--
-- Note: the entry may be 'Delete'.
--
lookups ::
     WriteBuffer
  -> V.Vector SerialisedKey
  -> V.Vector (Maybe (Entry SerialisedValue BlobSpan))
lookups (WB !m) !ks = V.mapStrict (`Map.lookup` m) ks

-- | TODO: update once blob references are implemented
lookup ::
     WriteBuffer
  -> SerialisedKey
  -> Maybe (Entry SerialisedValue BlobSpan)
lookup (WB !m) !k = Map.lookup k m

{-------------------------------------------------------------------------------
  RangeQueries
-------------------------------------------------------------------------------}

-- | We return 'Entry' instead of either @QueryResult@,
-- so we can properly combine lookup results.
--
-- Note: 'Delete's are not filtered out.
--
rangeLookups ::
     WriteBuffer
  -> Range SerialisedKey
  -> [(SerialisedKey, Entry SerialisedValue BlobSpan)]
rangeLookups (WB m) r =
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
