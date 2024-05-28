{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}

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

import           Control.DeepSeq (NFData (..))
import qualified Data.Map.Range as Map.R
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Monoidal as Monoidal
import qualified Database.LSMTree.Internal.Normal as Normal
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.Vector as V

{-------------------------------------------------------------------------------
  Writebuffer type
-------------------------------------------------------------------------------}

-- | The phantom type parameters provide some safety, enforcing that the types
-- of inserted entries are consistent.
--
-- TODO: Revisit this when using the write buffer from the table handle.
-- It would be consistent with other internal APIs (e.g. for @Run@ and
-- @IndexCompact@ to remove the type parameters here and move the responsibility
-- for these constraints and (de)serialisation to the layer above.
newtype WriteBuffer =
  WB { unWB :: Map SerialisedKey (Entry SerialisedValue SerialisedBlob) }
  deriving stock Show
  deriving newtype NFData

empty :: WriteBuffer
empty = WB Map.empty

numEntries :: WriteBuffer -> NumEntries
numEntries (WB m) = NumEntries (Map.size m)

-- | \( O(n) \)
content :: WriteBuffer ->
           [(SerialisedKey, Entry SerialisedValue SerialisedBlob)]
content (WB m) = Map.assocs m

{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

addEntryMonoidal ::
     (SerialisedValue -> SerialisedValue -> SerialisedValue) -- ^ merge function
  -> SerialisedKey -> Monoidal.Update SerialisedValue -> WriteBuffer -> WriteBuffer
addEntryMonoidal f k e (WB wb) =
    WB (Map.insertWith (combine f) k (updateToEntryMonoidal e) wb)

addEntryNormal ::
     SerialisedKey
  -> Normal.Update SerialisedValue SerialisedBlob
  -> WriteBuffer
  -> WriteBuffer
addEntryNormal k e (WB wb) =
    WB (Map.insert k (updateToEntryNormal e) wb)

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
lookups (WB !m) !ks = V.mapStrict (`Map.lookup` m) ks

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
