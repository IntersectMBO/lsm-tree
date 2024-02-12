{-# LANGUAGE DeriveAnyClass           #-}
{-# LANGUAGE DeriveGeneric            #-}
{-# LANGUAGE DerivingStrategies       #-}
{-# LANGUAGE StandaloneKindSignatures #-}

module Database.LSMTree.Internal.BlobRef (
    BlobRef (..)
  , BlobSpan (..)
  ) where

import           Control.DeepSeq (NFData)
import           Data.Kind (Type)
import           Data.Word (Word32, Word64)
import           GHC.Generics (Generic)

-- | A handle-like reference to an on-disk blob. The blob can be retrieved based
-- on the reference.
--
-- Blob comes from the acronym __Binary Large OBject (BLOB)__ and in many
-- database implementations refers to binary data that is larger than usual
-- values and is handled specially. In our context we will allow optionally a
-- blob associated with each value in the table.
--
-- Though blob references are handle-like, they /do not/ keep files open. As
-- such, when a blob reference is returned by a lookup, modifying the
-- corresponding table handle (or session) /may/ cause the blob reference to be
-- invalidated (i.e.., the blob has gone missing because the blob file was
-- removed). These operations include:
--
-- * Updates (e.g., inserts, deletes, mupserts)
-- * Closing table handles
-- * Closing sessions
--
-- An invalidated blob reference will throw an exception when used to look up a
-- blob. Note that table operations such as snapshotting and duplication do
-- /not/ invalidate blob references. These operations do not modify the logical
-- contents or state of an existing table.
--
-- [Blob reference validity] as long as the table handle that the blob reference
-- originated from is not updated or closed, the blob reference will be valid.
type BlobRef :: (Type -> Type) -> Type -> Type
data BlobRef m blob = BlobRef {
      -- TODO: add a reference to the run that contains the actual blob
      blobRefOffset :: !Word64
    , blobRefSize   :: !Word32
    }

-- | Location of a blob inside a blob file.
data BlobSpan = BlobSpan {
    blobSpanOffset :: !Word64
  , blobSpanSize   :: !Word32
  }
  deriving stock (Show, Eq, Generic)
  deriving anyclass NFData
