{-# LANGUAGE StandaloneKindSignatures #-}

-- TODO: remove once the API is implemented.
{-# OPTIONS_GHC -Wno-redundant-constraints #-}
{-# OPTIONS_GHC -Wno-unused-top-binds #-}

module Database.LSMTree.Common (
    -- * IOLike
    IOLike
    -- * Sessions
  , Session
  , newSession
  , closeSession
    -- * Constraints
  , SomeSerialisationConstraint (..)
  , SomeUpdateConstraint (..)
    -- * Small types
  , Range (..)
  ) where

import           Control.Concurrent.Class.MonadMVar (MonadMVar)
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadThrow)
import           Data.Bits (shiftR, (.&.))
import qualified Data.ByteString as BS
import           Data.Kind (Type)
import           Data.Word (Word64)
import           System.FS.API (FsPath, HasFS, SomeHasFS)

{-------------------------------------------------------------------------------
  IOLike
-------------------------------------------------------------------------------}

-- | Utility class for grouping @io-classes@ constraints.
class (MonadMVar m, MonadThrow m, MonadCatch m) => IOLike m where
instance IOLike IO

{-------------------------------------------------------------------------------
  Sessions
-------------------------------------------------------------------------------}

-- | Context shared across multiple table handles, like counters and filesystem
-- information.
--
-- For one, this is necessary if we want to be able to manipulate and query
-- table handles, duplicate table handles, and load snapshots, all at the same
-- time in the same directory.
--
-- Different types of tables can be live in the same session, but operations
-- like 'merge' only work for __compatible__ tables: tables that belong to the
-- same session, store the same key and value types, and have the same
-- configuration parameters.
type Session :: (Type -> Type) -> Type
data Session m = Session {
    sessionRoot  :: !FsPath
  , sessionHasFS :: !(SomeHasFS m)
  }

newSession ::
     IOLike m
  => HasFS m h
  -> FsPath -- ^ Path to an empty directory.
  -> m (Session m)
newSession = undefined

closeSession :: IOLike m => Session m -> m ()
closeSession = undefined

{-------------------------------------------------------------------------------
  Serialization constraints
-------------------------------------------------------------------------------}

-- | A placeholder class for (de)serialisation constraints.
--
-- TODO: Should be replaced with whatever (de)serialisation class we eventually
-- want to use. Some prerequisites:
-- *  Serialisation/deserialisation should preserve ordering.
class SomeSerialisationConstraint a where
    serialise :: a -> BS.ByteString

    -- Note: cannot fail.
    deserialise :: BS.ByteString -> a

instance SomeSerialisationConstraint BS.ByteString where
    serialise = id
    deserialise = id

-- | A placeholder class for constraints on 'Update's.
--
-- TODO: should be replaced by the actual constraints we want to use. Some
-- prerequisites:
-- * Combining/merging/resolving 'Update's should be associative.
-- * Should include a function that determines whether it is safe to remove an
--   'Update' from the last level of an LSM tree.
--
class SomeUpdateConstraint a where
    merge :: a -> a -> a

instance SomeUpdateConstraint BS.ByteString where
    merge = (<>)

-- | MSB, so order is preserved.
instance SomeSerialisationConstraint Word64 where
    -- TODO: optimize me when SomeSerialisationConstraint is replaced with its
    -- final version
    serialise w = BS.pack [b1,b2,b3,b4,b5,b6,b7,b8] where
        b8 = fromIntegral $        w    .&. 0xff
        b7 = fromIntegral $ shiftR w  8 .&. 0xff
        b6 = fromIntegral $ shiftR w 16 .&. 0xff
        b5 = fromIntegral $ shiftR w 24 .&. 0xff
        b4 = fromIntegral $ shiftR w 32 .&. 0xff
        b3 = fromIntegral $ shiftR w 40 .&. 0xff
        b2 = fromIntegral $ shiftR w 48 .&. 0xff
        b1 = fromIntegral $ shiftR w 56 .&. 0xff

    -- TODO: optimize me when SomeSerialisationConstraint is replaced with its
    -- final version
    deserialise = BS.foldl' (\acc d -> acc * 0x100 + fromIntegral d) 0

{-------------------------------------------------------------------------------
  Small auxiliary types
-------------------------------------------------------------------------------}

-- | A range of keys.
--
-- TODO: consider adding key prefixes to the range type.
data Range k =
    -- | Inclusive lower bound, exclusive upper bound
    FromToExcluding k k
    -- | Inclusive lower bound, inclusive upper bound
  | FromToIncluding k k
