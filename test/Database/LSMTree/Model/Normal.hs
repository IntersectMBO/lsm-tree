{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE RoleAnnotations     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}
module Database.LSMTree.Model.Normal (
    -- * Serialisation
    SerialiseKey (..)
  , SerialiseValue (..)
    -- * Tables
  , Table
  , empty
    -- * Table querying and updates
    -- ** Queries
  , Range (..)
  , LookupResult (..)
  , lookups
  , RangeLookupResult (..)
  , rangeLookup
    -- ** Updates
  , Update (..)
  , updates
  , inserts
  , deletes
    -- ** Blobs
  , BlobRef
  , retrieveBlobs
    -- * Snapshots
  , snapshot
    -- * Multiple writable table handles
  , duplicate
  ) where

import qualified Crypto.Hash.SHA256 as SHA256
import qualified Data.ByteString as BS
import           Data.Foldable (foldl')
import           Data.Kind (Type)
import           Data.Map (Map)
import qualified Data.Map.Range as Map.R
import qualified Data.Map.Strict as Map
import           Database.LSMTree.Common (Range (..), SerialiseKey (..),
                     SerialiseValue (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes)
import           Database.LSMTree.Normal (LookupResult (..),
                     RangeLookupResult (..), Update (..))
import           GHC.Exts (IsList (..))

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

type Table :: Type -> Type -> Type -> Type
data Table k v blob = Table
    { _values :: Map RawBytes (RawBytes, Maybe (BlobRef blob))
    }

type role Table nominal nominal nominal

-- | An empty table.
empty :: Table k v blob
empty = Table Map.empty

-- | This instance is for testing and debugging only.
instance
    (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
      => IsList (Table k v blob)
  where
    type Item (Table k v blob) = (k, v, Maybe blob)
    fromList xs = Table $ Map.fromList
        [ (serialiseKey k, (serialiseValue v, mkBlobRef <$> mblob))
        | (k, v, mblob) <- xs
        ]

    toList (Table m) =
        [ (deserialiseKey k, deserialiseValue v, getBlobFromRef <$> mbref)
        | (k, (v, mbref)) <- Map.toList m
        ]

-- | This instance is for testing and debugging only.
instance Show (Table k v blob) where
    showsPrec d (Table tbl) = showParen (d > 10)
        $ showString "fromList "
        . showsPrec 11 (toList (Table @BS.ByteString @BS.ByteString @BS.ByteString tbl'))
      where
        tbl' :: Map RawBytes (RawBytes, Maybe (BlobRef BS.ByteString))
        tbl' = fmap (fmap (fmap coerceBlobRef)) tbl

-- | This instance is for testing and debugging only.
deriving instance Eq (Table k v blob)

{-------------------------------------------------------------------------------
  Table querying and updates
-------------------------------------------------------------------------------}

-- | Perform a batch of lookups.
--
-- Lookups can be performed concurrently from multiple Haskell threads.
lookups ::
    (SerialiseKey k, SerialiseValue v)
  => [k]
  -> Table k v blob
  -> [LookupResult k v (BlobRef blob)]
lookups ks tbl =
    [ case Map.lookup (serialiseKey k) (_values tbl) of
        Nothing           -> NotFound k
        Just (v, Nothing) -> Found k (deserialiseValue v)
        Just (v, Just br) -> FoundWithBlob k (deserialiseValue v) br
    | k <- ks
    ]

-- | Perform a range lookup.
--
-- Range lookups can be performed concurrently from multiple Haskell threads.
rangeLookup :: forall k v blob.
     (SerialiseKey k, SerialiseValue v)
  => Range k
  -> Table k v blob
  -> [RangeLookupResult k v (BlobRef blob)]
rangeLookup r tbl =
    [ case v of
        (v', Nothing) -> FoundInRange (deserialiseKey k) (deserialiseValue v')
        (v', Just br) -> FoundInRangeWithBlob (deserialiseKey k) (deserialiseValue v') br
    | let (lb, ub) = convertRange r
    , (k, v) <- Map.R.rangeLookup lb ub (_values tbl)
    ]
  where
    convertRange :: Range k -> (Map.R.Bound RawBytes, Map.R.Bound RawBytes)
    convertRange (FromToExcluding lb ub) =
        ( Map.R.Bound (serialiseKey lb) Map.R.Inclusive
        , Map.R.Bound (serialiseKey ub) Map.R.Exclusive )
    convertRange (FromToIncluding lb ub) =
        ( Map.R.Bound (serialiseKey lb) Map.R.Inclusive
        , Map.R.Bound (serialiseKey ub) Map.R.Inclusive )

-- | Perform a mixed batch of inserts and deletes.
--
-- Updates can be performed concurrently from multiple Haskell threads.
updates :: forall k v blob.
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => [(k, Update v blob)]
  -> Table k v blob
  -> Table k v blob
updates ups tbl0 = foldl' update tbl0 ups where
    update :: Table k v blob -> (k, Update v blob) -> Table k v blob
    update tbl (k, Delete) = tbl
        { _values = Map.delete (serialiseKey k) (_values tbl) }
    update tbl (k, Insert v Nothing) = tbl
        { _values = Map.insert (serialiseKey k) (serialiseValue v, Nothing) (_values tbl) }
    update tbl (k, Insert v (Just blob)) = tbl
        { _values = Map.insert (serialiseKey k) (serialiseValue v, Just (mkBlobRef blob)) (_values tbl)
        }

-- | Perform a batch of inserts.
--
-- Inserts can be performed concurrently from multiple Haskell threads.
inserts ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => [(k, v, Maybe blob)]
  -> Table k v blob
  -> Table k v blob
inserts = updates . fmap (\(k, v, blob) -> (k, Insert v blob))

-- | Perform a batch of deletes.
--
-- Deletes can be performed concurrently from multiple Haskell threads.
deletes ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => [k]
  -> Table k v blob
  -> Table k v blob
deletes = updates . fmap (,Delete)

-- | A reference to an on-disk blob.
--
-- The blob can be retrieved based on the reference.
--
-- Blob comes from the acronym __Binary Large OBject (BLOB)__ and in many
-- database implementations refers to binary data that is larger than usual
-- values and is handled specially. In our context we will allow optionally a
-- blob associated with each value in the table.
data BlobRef blob = BlobRef
    !BS.ByteString  -- ^ digest
    !RawBytes       -- ^ actual contents
  deriving (Show)

type role BlobRef nominal

mkBlobRef :: SerialiseValue blob => blob -> BlobRef blob
mkBlobRef blob =  BlobRef (SHA256.hash bs) rb
  where
    !rb = serialiseValue blob
    !bs = deserialiseValue rb :: BS.ByteString

coerceBlobRef :: BlobRef blob -> BlobRef blob'
coerceBlobRef (BlobRef d b) = BlobRef d b

getBlobFromRef :: SerialiseValue blob => BlobRef blob -> blob
getBlobFromRef (BlobRef _ rb) = deserialiseValue rb

instance Eq (BlobRef blob) where
    BlobRef x _ == BlobRef y _ = x == y

-- | Perform a batch of blob retrievals.
--
-- This is a separate step from 'lookups' and 'rangeLookups. The result of a
-- lookup can include a 'BlobRef', which can be used to retrieve the actual
-- 'Blob'.
--
-- Blob lookups can be performed concurrently from multiple Haskell threads.
retrieveBlobs ::
     SerialiseValue blob
  => [BlobRef blob]
  -> [blob]
retrieveBlobs refs = map getBlobFromRef refs

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

snapshot ::
     Table k v blob
  -> Table k v blob
snapshot = id

{-------------------------------------------------------------------------------
  Mutiple writable table handles
-------------------------------------------------------------------------------}

duplicate ::
     Table k v blob
  -> Table k v blob
duplicate = id
