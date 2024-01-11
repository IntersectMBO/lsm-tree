{-# LANGUAGE DeriveTraversable   #-}
{-# LANGUAGE ScopedTypeVariables #-}
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
    WriteBuffer,
    emptyWriteBuffer,
    addEntryMonoidal,
    addEntryNormal,
    lookups,
    rangeLookups,
) where

import qualified Data.Map.Range as Map.R
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Common (Range (..))
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Monoidal as Monoidal
import qualified Database.LSMTree.Internal.Normal as Normal

{-------------------------------------------------------------------------------
  Writebuffer type
-------------------------------------------------------------------------------}

newtype WriteBuffer k v blobref = WB (Map k (Entry v blobref))

emptyWriteBuffer :: WriteBuffer k v blobref
emptyWriteBuffer = WB Map.empty

{-------------------------------------------------------------------------------
  Updates
-------------------------------------------------------------------------------}

combine :: (v -> v -> v) -> Entry v blobref -> Entry v blobref -> Entry v blobref
combine _ e@Delete            _                       = e
combine _ e@Insert {}         _                       = e
combine _ e@InsertWithBlob {} _                       = e
combine _   (Mupdate u)       Delete                  = Insert u
combine f   (Mupdate u)       (Insert v)              = Insert (f u v)
combine f   (Mupdate u)       (InsertWithBlob v blob) = InsertWithBlob (f u v) blob
combine f   (Mupdate u)       (Mupdate v)             = Mupdate (f u v)

addEntryMonoidal :: Ord k
  => (v -> v -> v) -- ^ merge function
  -> k -> Monoidal.Update v -> WriteBuffer k v blobref -> WriteBuffer k v blobref
addEntryMonoidal f k e (WB wb) = WB (Map.insertWith (combine f) k (g e) wb) where
  g :: Monoidal.Update v -> Entry v blobref
  g (Monoidal.Insert v)  = Insert v
  g (Monoidal.Mupsert v) = Mupdate v
  g (Monoidal.Delete)    = Delete

addEntryNormal :: Ord k
  => k -> Normal.Update v blobref -> WriteBuffer k v blobref -> WriteBuffer k v blobref
addEntryNormal k e (WB wb) = WB (Map.insert k (g e) wb) where
  g :: Normal.Update v blobref -> Entry v blobref
  g (Normal.Insert v Nothing)     = Insert v
  g (Normal.Insert v (Just bref)) = InsertWithBlob v bref
  g Normal.Delete                 = Delete

{-------------------------------------------------------------------------------
  Querying
-------------------------------------------------------------------------------}

-- We return 'Entry', so it can be properly combined with the lookups in other
-- runs.
--
-- Note: the entry may be 'Delete'.
--
lookups :: forall k v blobref. Ord k
  => WriteBuffer k v blobref
  -> [k]
  -> [(k, Maybe (Entry v blobref))]
lookups (WB m) = fmap f where
   f :: k -> (k, Maybe (Entry v blobref))
   f k = (k, Map.lookup k m)

{-------------------------------------------------------------------------------
  RangeQueries
-------------------------------------------------------------------------------}

-- | We return 'Entry' instead of either @angeLookupResult@,
-- so we can properly combine lookup results.
--
-- Note: 'Delete's are not filtered out.
--
rangeLookups :: Ord k
  => WriteBuffer k v blobref
  -> Range k
  -> [(k, Entry v blobref)]
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
