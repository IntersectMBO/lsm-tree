{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{- HLINT ignore "Use unless" -}

module Database.LSMTree.Internal.BlobRef (
    RawBlobRef (..)
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
import           Database.LSMTree.Internal.BlobFile (BlobSpan (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..))
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS


-- | A raw blob reference is a reference to a blob within a blob file.
--
-- The \"raw\" means that it does no reference counting, so does not maintain
-- ownership of the 'BlobFile'. Thus these are only safe to use in the context
-- of code that already (directly or indirectly) owns the blob file that the
-- blob ref uses (such as within run merging).
--
-- Thus these cannot be handed out via the API. Use 'WeakBlobRef' for that.
--
data RawBlobRef m h = RawBlobRef {
      blobRefFile  :: !(FS.Handle h)
    , blobRefCount :: {-# UNPACK #-} !(RefCounter m)
    , blobRefSpan  :: {-# UNPACK #-} !BlobSpan
    }
  deriving stock (Show)

instance NFData h => NFData (RawBlobRef m h) where
  rnf (RawBlobRef a b c) = rnf a `seq` rnf b `seq` rnf c

blobRefSpanSize :: RawBlobRef m h -> Int
blobRefSpanSize = fromIntegral . blobSpanSize . blobRefSpan

-- | A \"weak\" reference to a blob within a blob file. These are the ones we
-- can return in the public API and can outlive their parent table.
--
-- They are weak references in that they do not keep the file open using a
-- reference count. So when we want to use our weak reference we have to
-- dereference them to obtain a normal strong reference while we do the I\/O
-- to read the blob. This ensures the file is not closed under our feet.
--
-- See 'Database.LSMTree.Common.BlobRef' for more info.
--
newtype WeakBlobRef m h = WeakBlobRef (RawBlobRef m h)
  deriving newtype (Show, NFData)

-- | The 'WeakBlobRef' now points to a blob that is no longer available.
newtype WeakBlobRefInvalid = WeakBlobRefInvalid Int
  deriving stock (Show)
  deriving anyclass (Exception)

{-# SPECIALISE withWeakBlobRef ::
     WeakBlobRef IO h
  -> (RawBlobRef IO h -> IO a)
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
  -> (RawBlobRef m h -> m a)
  -> m a
withWeakBlobRef wref = bracket (deRefWeakBlobRef wref) removeReference

{-# SPECIALISE withWeakBlobRefs ::
     V.Vector (WeakBlobRef IO h)
  -> (V.Vector (RawBlobRef IO h) -> IO a)
  -> IO a #-}
-- | The same as 'withWeakBlobRef' but for many references in one go.
--
withWeakBlobRefs ::
     (MonadMask m, PrimMonad m)
  => V.Vector (WeakBlobRef m h)
  -> (V.Vector (RawBlobRef m h) -> m a)
  -> m a
withWeakBlobRefs wrefs = bracket (deRefWeakBlobRefs wrefs) removeReferences

{-# SPECIALISE deRefWeakBlobRef ::
     WeakBlobRef IO h
  -> IO (RawBlobRef IO h) #-}
deRefWeakBlobRef ::
     (MonadThrow m, PrimMonad m)
  => WeakBlobRef m h
  -> m (RawBlobRef m h)
deRefWeakBlobRef (WeakBlobRef ref) = do
    ok <- RC.upgradeWeakReference (blobRefCount ref)
    when (not ok) $ throwIO (WeakBlobRefInvalid 0)
    pure ref

{-# SPECIALISE deRefWeakBlobRefs ::
     V.Vector (WeakBlobRef IO h)
  -> IO (V.Vector (RawBlobRef IO h)) #-}
deRefWeakBlobRefs ::
    forall m h.
     (MonadMask m, PrimMonad m)
  => V.Vector (WeakBlobRef m h)
  -> m (V.Vector (RawBlobRef m h))
deRefWeakBlobRefs wrefs = do
    let refs :: V.Vector (RawBlobRef m h)
        refs = coerce wrefs -- safely coerce away the newtype wrappers
    V.iforM_ wrefs $ \i (WeakBlobRef ref) -> do
      ok <- RC.upgradeWeakReference (blobRefCount ref)
      when (not ok) $ do
        -- drop refs on the previous ones taken successfully so far
        V.mapM_ removeReference (V.take i refs)
        throwIO (WeakBlobRefInvalid i)
    pure refs

{-# SPECIALISE removeReference :: RawBlobRef IO h -> IO () #-}
removeReference :: (MonadMask m, PrimMonad m) => RawBlobRef m h -> m ()
removeReference = RC.removeReference . blobRefCount

{-# SPECIALISE removeReferences :: V.Vector (RawBlobRef IO h) -> IO () #-}
removeReferences :: (MonadMask m, PrimMonad m) => V.Vector (RawBlobRef m h) -> m ()
removeReferences = V.mapM_ removeReference

{-# SPECIALISE readBlob ::
     HasFS IO h
  -> RawBlobRef IO h
  -> IO SerialisedBlob #-}
readBlob ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> RawBlobRef m h
  -> m SerialisedBlob
readBlob fs RawBlobRef {
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
  -> RawBlobRef m h
  -> FS.IOOp s h
readBlobIOOp buf bufoff
             RawBlobRef {
               blobRefFile,
               blobRefSpan = BlobSpan {blobSpanOffset, blobSpanSize}
             } =
    FS.IOOpRead
      blobRefFile
      (fromIntegral blobSpanOffset :: FS.FileOffset)
      buf (FS.BufferOffset bufoff)
      (fromIntegral blobSpanSize :: FS.ByteCount)
