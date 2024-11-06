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
  , rawToWeakBlobRef
  , removeReference
  , removeReferences
  , readRawBlobRef
  , readWeakBlobRef
  , readBlobIOOp
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad (when)
import           Control.Monad.Class.MonadThrow (Exception, MonadMask,
                     MonadThrow (..), bracket, throwIO)
import           Control.Monad.Primitive
import qualified Control.RefCount as RC
import qualified Data.Primitive.ByteArray as P (MutableByteArray)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import           Database.LSMTree.Internal.BlobFile (BlobFile (..), BlobSpan (..))
import qualified Database.LSMTree.Internal.BlobFile as BlobFile
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
      rawBlobRefFile :: {-# NOUNPACK #-} !(BlobFile m h)
    , rawBlobRefSpan :: {-# UNPACK #-}   !BlobSpan
    }
  deriving stock (Show)

instance NFData h => NFData (RawBlobRef m h) where
  rnf (RawBlobRef a b) = rnf a `seq` rnf b

blobRefSpanSize :: StrongBlobRef m h -> Int
blobRefSpanSize = fromIntegral . blobSpanSize . strongBlobRefSpan

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
data WeakBlobRef m h = WeakBlobRef {
      weakBlobRefFile :: {-# NOUNPACK #-} !(BlobFile m h)
    , weakBlobRefSpan :: {-# UNPACK #-}   !BlobSpan
    }
  deriving stock (Show)

-- | A \"strong\" reference to a blob within a blob file. The blob file remains
-- open while the strong reference is live. Thus it is safe to do I\/O to
-- retrieve the blob based on the reference. Strong references must be released
-- using 'releaseBlobRef' when no longer in use (e.g. after completing I\/O).
--
data StrongBlobRef m h = StrongBlobRef {
      strongBlobRefFile :: {-# NOUNPACK #-} !(BlobFile m h)
    , strongBlobRefSpan :: {-# UNPACK #-}   !BlobSpan
    }
  deriving stock (Show)

-- | Convert a 'RawBlobRef' to a 'WeakBlobRef'.
rawToWeakBlobRef :: RawBlobRef m h -> WeakBlobRef m h
rawToWeakBlobRef RawBlobRef {rawBlobRefFile, rawBlobRefSpan} =
    -- This doesn't need to really do anything, becuase the raw version
    -- does not maintain an independent ref count, and the weak one does
    -- not either.
    WeakBlobRef {
      weakBlobRefFile = rawBlobRefFile,
      weakBlobRefSpan = rawBlobRefSpan
    }

-- | The 'WeakBlobRef' now points to a blob that is no longer available.
newtype WeakBlobRefInvalid = WeakBlobRefInvalid Int
  deriving stock (Show)
  deriving anyclass (Exception)

{-# SPECIALISE withWeakBlobRef ::
     WeakBlobRef IO h
  -> (StrongBlobRef IO h -> IO a)
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
  -> (StrongBlobRef m h -> m a)
  -> m a
withWeakBlobRef wref = bracket (deRefWeakBlobRef wref) removeReference

{-# SPECIALISE withWeakBlobRefs ::
     V.Vector (WeakBlobRef IO h)
  -> (V.Vector (StrongBlobRef IO h) -> IO a)
  -> IO a #-}
-- | The same as 'withWeakBlobRef' but for many references in one go.
--
withWeakBlobRefs ::
     (MonadMask m, PrimMonad m)
  => V.Vector (WeakBlobRef m h)
  -> (V.Vector (StrongBlobRef m h) -> m a)
  -> m a
withWeakBlobRefs wrefs = bracket (deRefWeakBlobRefs wrefs) removeReferences

{-# SPECIALISE deRefWeakBlobRef ::
     WeakBlobRef IO h
  -> IO (StrongBlobRef IO h) #-}
deRefWeakBlobRef ::
     (MonadThrow m, PrimMonad m)
  => WeakBlobRef m h
  -> m (StrongBlobRef m h)
deRefWeakBlobRef WeakBlobRef{weakBlobRefFile, weakBlobRefSpan} = do
    ok <- RC.upgradeWeakReference (blobFileRefCounter weakBlobRefFile)
    when (not ok) $ throwIO (WeakBlobRefInvalid 0)
    return StrongBlobRef{
      strongBlobRefFile = weakBlobRefFile,
      strongBlobRefSpan = weakBlobRefSpan
    }

{-# SPECIALISE deRefWeakBlobRefs ::
     V.Vector (WeakBlobRef IO h)
  -> IO (V.Vector (StrongBlobRef IO h)) #-}
deRefWeakBlobRefs ::
    forall m h.
     (MonadMask m, PrimMonad m)
  => V.Vector (WeakBlobRef m h)
  -> m (V.Vector (StrongBlobRef m h))
deRefWeakBlobRefs wrefs = do
    refs <- VM.new (V.length wrefs)
    V.iforM_ wrefs $ \i WeakBlobRef {weakBlobRefFile, weakBlobRefSpan} -> do
      ok <- RC.upgradeWeakReference (blobFileRefCounter weakBlobRefFile)
      if ok
        then VM.write refs i StrongBlobRef {
               strongBlobRefFile = weakBlobRefFile,
               strongBlobRefSpan = weakBlobRefSpan
             }
        else do
          -- drop refs on the previous ones taken successfully so far
          VM.mapM_ removeReference (VM.take i refs)
          throwIO (WeakBlobRefInvalid i)
    V.unsafeFreeze refs

{-# SPECIALISE removeReference :: StrongBlobRef IO h -> IO () #-}
removeReference :: (MonadMask m, PrimMonad m) => StrongBlobRef m h -> m ()
removeReference = BlobFile.removeReference . strongBlobRefFile

{-# SPECIALISE removeReferences :: V.Vector (StrongBlobRef IO h) -> IO () #-}
removeReferences :: (MonadMask m, PrimMonad m) => V.Vector (StrongBlobRef m h) -> m ()
removeReferences = V.mapM_ removeReference

{-# INLINE readRawBlobRef #-}
readRawBlobRef ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> RawBlobRef m h
  -> m SerialisedBlob
readRawBlobRef fs RawBlobRef {rawBlobRefFile, rawBlobRefSpan} =
    BlobFile.readBlobFile fs rawBlobRefFile rawBlobRefSpan

{-# SPECIALISE readWeakBlobRef :: HasFS IO h -> WeakBlobRef IO h -> IO SerialisedBlob #-}
readWeakBlobRef ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> WeakBlobRef m h
  -> m SerialisedBlob
readWeakBlobRef fs wref =
    bracket (deRefWeakBlobRef wref) removeReference $
      \StrongBlobRef {strongBlobRefFile, strongBlobRefSpan} ->
        BlobFile.readBlobFile fs strongBlobRefFile strongBlobRefSpan

readBlobIOOp ::
     P.MutableByteArray s -> Int
  -> StrongBlobRef m h
  -> FS.IOOp s h
readBlobIOOp buf bufoff
             StrongBlobRef {
               strongBlobRefFile = BlobFile {blobFileHandle},
               strongBlobRefSpan = BlobSpan {blobSpanOffset, blobSpanSize}
             } =
    FS.IOOpRead
      blobFileHandle
      (fromIntegral blobSpanOffset :: FS.FileOffset)
      buf (FS.BufferOffset bufoff)
      (fromIntegral blobSpanSize :: FS.ByteCount)
