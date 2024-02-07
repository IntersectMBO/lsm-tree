{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}

module Database.LSMTree.Internal.BlobRef (
    BlobRef (..)
  , BlobSpan (..)
  ) where

import           Control.DeepSeq (NFData)
import           Data.Word (Word32, Word64)
import           GHC.Generics (Generic)

-- | A reference to an on-disk blob.
--
-- The blob can be retrieved based on the reference.
--
-- Blob comes from the acronym __Binary Large OBject (BLOB)__ and in many
-- database implementations refers to binary data that is larger than usual
-- values and is handled specially. In our context we will allow optionally a
-- blob associated with each value in the table.
data BlobRef blob = BlobRef

-- | Location of a blob inside a blob file.
data BlobSpan = BlobSpan {
    blobSpanOffset :: !Word64
  , blobSpanSize   :: !Word32
  }
  deriving stock (Show, Eq, Generic)
  deriving anyclass NFData
