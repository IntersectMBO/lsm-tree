{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DeriveGeneric      #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE TypeFamilies       #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.BlobRef (
    BlobSpan (..)
  , RawBlobRef (..)
  , WeakBlobRef (..)
  , WeakBlobRefInvalid (..)
  , mkRawBlobRef
  , mkWeakBlobRef
  , rawToWeakBlobRef
  , readRawBlobRef
  , readWeakBlobRef
  , readWeakBlobRefs
  ) where

import           Control.Monad.Class.MonadThrow (Exception, MonadMask,
                     MonadThrow (..), bracket, throwIO)
import           Control.Monad.Primitive
import           Control.RefCount
import qualified Data.Primitive.ByteArray as P (newPinnedByteArray,
                     unsafeFreezeByteArray)
import qualified Data.Vector as V
import qualified Data.Vector.Mutable as VM
import           Database.LSMTree.Internal.BlobFile (BlobFile (..),
                     BlobSpan (..))
import qualified Database.LSMTree.Internal.BlobFile as BlobFile
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedBlob (..))
import qualified System.FS.API as FS
import           System.FS.API (HasFS)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)


-- | A raw blob reference is a reference to a blob within a blob file.
--
-- The \"raw\" means that it does not maintain ownership of the 'BlobFile' to
-- keep it open. Thus these are only safe to use in the context of code that
-- already (directly or indirectly) owns the blob file that the blob ref uses
-- (such as within run merging).
--
-- Thus these cannot be handed out via the API. Use 'WeakBlobRef' for that.
--
data RawBlobRef m h = RawBlobRef {
      rawBlobRefFile :: {-# NOUNPACK #-} !(BlobFile m h)
    , rawBlobRefSpan :: {-# UNPACK #-}   !BlobSpan
    }
  deriving stock (Show)

-- | A \"weak\" reference to a blob within a blob file. These are the ones we
-- can return in the public API and can outlive their parent table.
--
-- They are weak references in that they do not keep the file open using a
-- reference. So when we want to use our weak reference we have to dereference
-- them to obtain a normal strong reference while we do the I\/O to read the
-- blob. This ensures the file is not closed under our feet.
--
-- See 'Database.LSMTree.BlobRef' for more info.
--
data WeakBlobRef m h = WeakBlobRef {
      weakBlobRefFile :: {-# NOUNPACK #-} !(WeakRef (BlobFile m h))
    , weakBlobRefSpan :: {-# UNPACK #-}   !BlobSpan
    }
  deriving stock (Show)

-- | A \"strong\" reference to a blob within a blob file. The blob file remains
-- open while the strong reference is live. Thus it is safe to do I\/O to
-- retrieve the blob based on the reference. Strong references must be released
-- using 'releaseBlobRef' when no longer in use (e.g. after completing I\/O).
--
data StrongBlobRef m h = StrongBlobRef {
      strongBlobRefFile :: {-# NOUNPACK #-} !(Ref (BlobFile m h))
    , strongBlobRefSpan :: {-# UNPACK #-}   !BlobSpan
    }
  deriving stock (Show)

-- | Convert a 'RawBlobRef' to a 'WeakBlobRef'.
rawToWeakBlobRef :: RawBlobRef m h -> WeakBlobRef m h
rawToWeakBlobRef RawBlobRef {rawBlobRefFile, rawBlobRefSpan} =
    -- This doesn't need to really do anything, because the raw version
    -- does not maintain an independent ref count, and the weak one does
    -- not either.
    WeakBlobRef {
      weakBlobRefFile = mkWeakRefFromRaw rawBlobRefFile,
      weakBlobRefSpan = rawBlobRefSpan
    }

mkRawBlobRef :: Ref (BlobFile m h) -> BlobSpan -> RawBlobRef m h
mkRawBlobRef (DeRef blobfile) blobspan =
    RawBlobRef {
      rawBlobRefFile = blobfile,
      rawBlobRefSpan = blobspan
    }

mkWeakBlobRef :: Ref (BlobFile m h) -> BlobSpan -> WeakBlobRef m h
mkWeakBlobRef blobfile blobspan =
    WeakBlobRef {
      weakBlobRefFile = mkWeakRef blobfile,
      weakBlobRefSpan = blobspan
    }

-- | The 'WeakBlobRef' now points to a blob that is no longer available.
newtype WeakBlobRefInvalid = WeakBlobRefInvalid Int
  deriving stock (Show)
  deriving anyclass (Exception)

{-# SPECIALISE deRefWeakBlobRef ::
     WeakBlobRef IO h
  -> IO (StrongBlobRef IO h) #-}
deRefWeakBlobRef ::
     (MonadThrow m, PrimMonad m)
  => WeakBlobRef m h
  -> m (StrongBlobRef m h)
deRefWeakBlobRef WeakBlobRef{weakBlobRefFile, weakBlobRefSpan} = do
    mstrongBlobRefFile <- deRefWeak weakBlobRefFile
    case mstrongBlobRefFile of
      Just strongBlobRefFile ->
        pure StrongBlobRef {
          strongBlobRefFile,
          strongBlobRefSpan = weakBlobRefSpan
        }
      Nothing -> throwIO (WeakBlobRefInvalid 0)

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
      mstrongBlobRefFile <- deRefWeak weakBlobRefFile
      case mstrongBlobRefFile of
        Just strongBlobRefFile ->
          VM.write refs i StrongBlobRef {
            strongBlobRefFile,
            strongBlobRefSpan = weakBlobRefSpan
          }
        Nothing -> do
          -- drop refs on the previous ones taken successfully so far
          VM.mapM_ releaseBlobRef (VM.take i refs)
          throwIO (WeakBlobRefInvalid i)
    V.unsafeFreeze refs

{-# INLINE releaseBlobRef #-}
releaseBlobRef :: (MonadMask m, PrimMonad m) => StrongBlobRef m h -> m ()
releaseBlobRef = releaseRef . strongBlobRefFile

{-# INLINE readRawBlobRef #-}
readRawBlobRef ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> RawBlobRef m h
  -> m SerialisedBlob
readRawBlobRef fs RawBlobRef {rawBlobRefFile, rawBlobRefSpan} =
    BlobFile.readBlobRaw fs rawBlobRefFile rawBlobRefSpan

{-# SPECIALISE readWeakBlobRef :: HasFS IO h -> WeakBlobRef IO h -> IO SerialisedBlob #-}
readWeakBlobRef ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> WeakBlobRef m h
  -> m SerialisedBlob
readWeakBlobRef fs wref =
    bracket (deRefWeakBlobRef wref) releaseBlobRef $
      \StrongBlobRef {strongBlobRefFile, strongBlobRefSpan} ->
        BlobFile.readBlob fs strongBlobRefFile strongBlobRefSpan

{-# SPECIALISE readWeakBlobRefs :: HasBlockIO IO h -> V.Vector (WeakBlobRef IO h) -> IO (V.Vector SerialisedBlob) #-}
readWeakBlobRefs ::
     (MonadMask m, PrimMonad m)
  => HasBlockIO m h
  -> V.Vector (WeakBlobRef m h)
  -> m (V.Vector SerialisedBlob)
readWeakBlobRefs hbio wrefs =
    bracket (deRefWeakBlobRefs wrefs) (V.mapM_ releaseBlobRef) $ \refs -> do
      -- Prepare the IOOps:
      -- We use a single large memory buffer, with appropriate offsets within
      -- the buffer.
      let bufSize :: Int
          !bufSize = V.sum (V.map blobRefSpanSize refs)

          {-# INLINE bufOffs #-}
          bufOffs :: V.Vector Int
          bufOffs = V.scanl (+) 0 (V.map blobRefSpanSize refs)
      buf <- P.newPinnedByteArray bufSize

      -- Submit the IOOps all in one go:
      _ <- FS.submitIO hbio $
             V.zipWith
               (\bufoff
                 StrongBlobRef {
                   strongBlobRefFile = DeRef BlobFile {blobFileHandle},
                   strongBlobRefSpan = BlobSpan {blobSpanOffset, blobSpanSize}
                 } ->
                 FS.IOOpRead
                   blobFileHandle
                   (fromIntegral blobSpanOffset :: FS.FileOffset)
                   buf (FS.BufferOffset bufoff)
                   (fromIntegral blobSpanSize :: FS.ByteCount))
               bufOffs refs
      -- We do not need to inspect the results because IO errors are
      -- thrown as exceptions, and the result is just the read length
      -- which is already known. Short reads can't happen here.

      -- Construct the SerialisedBlobs results:
      -- This is just the different offsets within the shared buffer.
      ba <- P.unsafeFreezeByteArray buf
      pure $! V.zipWith
                (\off len -> SerialisedBlob (RB.fromByteArray off len ba))
                bufOffs
                (V.map blobRefSpanSize refs)
  where
    blobRefSpanSize = fromIntegral . blobSpanSize . strongBlobRefSpan
