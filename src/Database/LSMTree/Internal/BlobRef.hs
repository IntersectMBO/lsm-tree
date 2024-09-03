{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}

module Database.LSMTree.Internal.BlobRef (
    BlobRef (..)
  , BlobSpan (..)
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.RefCount (RefCounter)
import           Data.Word (Word32, Word64)

-- | A handle-like reference to an on-disk blob. The blob can be retrieved based
-- on the reference.
--
-- See 'Database.LSMTree.Common.BlobRef' for more info.
data BlobRef m h = BlobRef {
      blobRefFile  :: !h
    , blobRefCount :: {-# UNPACK #-} !(RefCounter m)
    , blobRefSpan  :: {-# UNPACK #-} !BlobSpan
    }
  deriving stock (Eq, Show)

instance NFData h => NFData (BlobRef m h) where
  rnf (BlobRef a b c) = rnf a `seq` rnf b `seq` rnf c

-- | Location of a blob inside a blob file.
data BlobSpan = BlobSpan {
    blobSpanOffset :: {-# UNPACK #-} !Word64
  , blobSpanSize   :: {-# UNPACK #-} !Word32
  }
  deriving stock (Show, Eq)

instance NFData BlobSpan where
  rnf (BlobSpan a b) = rnf a `seq` rnf b
