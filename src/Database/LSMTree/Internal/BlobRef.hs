{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{- HLINT ignore "Use unless" -}

module Database.LSMTree.Internal.BlobRef (
    BlobRef (..)
  , BlobSpan (..)
  , blobRefSpanSize
  , WeakBlobRef (..)
  , withWeakBlobRef
  , withWeakBlobRefs
  , deRefWeakBlobRef
  , deRefWeakBlobRefs
  , WeakBlobRefInvalid (..)
  , removeReference
  , removeReferences
  , readBlob
  , readBlobIOOp
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad (when)
import           Control.Monad.Class.MonadThrow (Exception, MonadMask,
                     MonadThrow (..), bracket, throwIO)
import           Control.Monad.Primitive
import           Control.RefCount (RefCounter)
import qualified Control.RefCount as RC
import           Data.Coerce (coerce)
import qualified Data.Primitive.ByteArray as P (MutableByteArray,
                     newPinnedByteArray, unsafeFreezeByteArray)
import qualified Data.Vector as V
import           Data.Word (Word32, Word64)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..))
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS

-- | A handle-like reference to an on-disk blob. The blob can be retrieved based
-- on the reference.
--
-- See 'Database.LSMTree.Common.BlobRef' for more info.
data BlobRef m h = BlobRef {
      blobRefFile  :: !h
    , blobRefCount :: {-# UNPACK #-} !(RefCounter m)
    , blobRefSpan  :: {-# UNPACK #-} !BlobSpan
    }
  deriving stock (Show)

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

blobRefSpanSize :: BlobRef m h -> Int
blobRefSpanSize = fromIntegral . blobSpanSize . blobRefSpan

-- | A 'WeakBlobRef' is a weak reference to a blob file. These are the ones we
-- can return in the public API and can outlive their parent table. They do not
-- keep the file open using a reference count. So when we want to use our weak
-- reference we have to dereference them to obtain a normal strong reference
-- while we do the I\/O to read the blob. This ensures the file is not closed
-- under our feet.
--
newtype WeakBlobRef m h = WeakBlobRef (BlobRef m h)
  deriving newtype (Show, NFData)

-- | The 'WeakBlobRef' now points to a blob that is no longer available.
newtype WeakBlobRefInvalid = WeakBlobRefInvalid Int
  deriving stock (Show)
  deriving anyclass (Exception)

{-# SPECIALISE withWeakBlobRef ::
     WeakBlobRef IO h
  -> (BlobRef IO h -> IO a)
  -> IO a #-}
-- | 'WeakBlobRef's are weak references. They do not keep the blob file open.
-- Dereference a 'WeakBlobRef' to a strong 'BlobRef' to allow I\/O using
-- 'readBlob' or 'readBlobIOOp'. Use 'removeReference' when the 'BlobRef' is
-- no longer needed.
--
-- Throws 'WeakBlobRefInvalid' if the weak reference has become invalid.
--
withWeakBlobRef ::
     (MonadMask m, PrimMonad m)
  => WeakBlobRef m h
  -> (BlobRef m h -> m a)
  -> m a
withWeakBlobRef wref = bracket (deRefWeakBlobRef wref) removeReference

{-# SPECIALISE withWeakBlobRefs ::
     V.Vector (WeakBlobRef IO h)
  -> (V.Vector (BlobRef IO h) -> IO a)
  -> IO a #-}
-- | The same as 'withWeakBlobRef' but for many references in one go.
--
withWeakBlobRefs ::
     (MonadMask m, PrimMonad m)
  => V.Vector (WeakBlobRef m h)
  -> (V.Vector (BlobRef m h) -> m a)
  -> m a
withWeakBlobRefs wrefs = bracket (deRefWeakBlobRefs wrefs) removeReferences

{-# SPECIALISE deRefWeakBlobRef ::
     WeakBlobRef IO h
  -> IO (BlobRef IO h) #-}
deRefWeakBlobRef ::
     (MonadThrow m, PrimMonad m)
  => WeakBlobRef m h
  -> m (BlobRef m h)
deRefWeakBlobRef (WeakBlobRef ref) = do
    ok <- RC.upgradeWeakReference (blobRefCount ref)
    when (not ok) $ throwIO (WeakBlobRefInvalid 0)
    pure ref

{-# SPECIALISE deRefWeakBlobRefs ::
     V.Vector (WeakBlobRef IO h)
  -> IO (V.Vector (BlobRef IO h)) #-}
deRefWeakBlobRefs ::
    forall m h.
     (MonadMask m, PrimMonad m)
  => V.Vector (WeakBlobRef m h)
  -> m (V.Vector (BlobRef m h))
deRefWeakBlobRefs wrefs = do
    let refs :: V.Vector (BlobRef m h)
        refs = coerce wrefs -- safely coerce away the newtype wrappers
    V.iforM_ wrefs $ \i (WeakBlobRef ref) -> do
      ok <- RC.upgradeWeakReference (blobRefCount ref)
      when (not ok) $ do
        -- drop refs on the previous ones taken successfully so far
        V.mapM_ removeReference (V.take i refs)
        throwIO (WeakBlobRefInvalid i)
    pure refs

{-# SPECIALISE removeReference :: BlobRef IO h -> IO () #-}
removeReference :: (MonadMask m, PrimMonad m) => BlobRef m h -> m ()
removeReference = RC.removeReference . blobRefCount

{-# SPECIALISE removeReferences :: V.Vector (BlobRef IO h) -> IO () #-}
removeReferences :: (MonadMask m, PrimMonad m) => V.Vector (BlobRef m h) -> m ()
removeReferences = V.mapM_ removeReference

{-# SPECIALISE readBlob ::
     HasFS IO h
  -> BlobRef IO (FS.Handle h)
  -> IO SerialisedBlob #-}
readBlob ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> BlobRef m (FS.Handle h)
  -> m SerialisedBlob
readBlob fs BlobRef {
              blobRefFile,
              blobRefSpan = BlobSpan {blobSpanOffset, blobSpanSize}
            } = do
    let off = FS.AbsOffset blobSpanOffset
        len :: Int
        len = fromIntegral blobSpanSize
    mba <- P.newPinnedByteArray len
    _ <- FS.hGetBufExactlyAt fs blobRefFile mba 0
                             (fromIntegral len :: FS.ByteCount) off
    ba <- P.unsafeFreezeByteArray mba
    let !rb = RB.fromByteArray 0 len ba
    return (SerialisedBlob rb)

readBlobIOOp ::
     P.MutableByteArray s -> Int
  -> BlobRef m (FS.Handle h)
  -> FS.IOOp s h
readBlobIOOp buf bufoff
             BlobRef {
               blobRefFile,
               blobRefSpan = BlobSpan {blobSpanOffset, blobSpanSize}
             } =
    FS.IOOpRead
      blobRefFile
      (fromIntegral blobSpanOffset :: FS.FileOffset)
      buf (FS.BufferOffset bufoff)
      (fromIntegral blobSpanSize :: FS.ByteCount)
