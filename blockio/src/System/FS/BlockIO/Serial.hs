module System.FS.BlockIO.Serial (
    serialHasBlockIO
  ) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Monad (unless)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad, PrimState, RealWorld)
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           System.FS.API
import qualified System.FS.BlockIO.API as API
import           System.FS.BlockIO.API (IOOp (..), IOResult (..), LockMode (..))

{-# SPECIALISE serialHasBlockIO ::
     Eq h
  => (Handle h -> Bool -> IO ())
  -> (Handle h -> API.FileOffset -> API.FileOffset -> API.Advice -> IO ())
  -> (Handle h -> API.FileOffset -> API.FileOffset -> IO ())
  -> (FsPath -> LockMode -> IO (Maybe (API.LockFileHandle IO)))
  -> (Handle h -> IO ())
  -> (FsPath -> IO ())
  -> (FsPath -> FsPath -> IO ())
  -> HasFS IO h
  -> IO (API.HasBlockIO IO h)
  #-}
-- | IO instantiation of 'HasBlockIO', using an existing 'HasFS'. Thus this
-- implementation does not take advantage of parallel I/O.
serialHasBlockIO ::
     (MonadThrow m, MonadMVar m, PrimMonad m, Eq h)
  => (Handle h -> Bool -> m ())
  -> (Handle h -> API.FileOffset -> API.FileOffset -> API.Advice -> m ())
  -> (Handle h -> API.FileOffset -> API.FileOffset -> m ())
  -> (FsPath -> LockMode -> m (Maybe (API.LockFileHandle m)))
  -> (Handle h -> m ())
  -> (FsPath -> m ())
  -> (FsPath -> FsPath -> m ())
  -> HasFS m h
  -> m (API.HasBlockIO m h)
serialHasBlockIO hSetNoCache hAdvise hAllocate tryLockFile hSynchronise synchroniseDirectory createHardLink hfs = do
  ctx <- initIOCtx (SomeHasFS hfs)
  pure $ API.HasBlockIO {
      API.close = close ctx
    , API.submitIO = submitIO hfs ctx
    , API.hSetNoCache
    , API.hAdvise
    , API.hAllocate
    , API.tryLockFile
    , API.hSynchronise
    , API.synchroniseDirectory
    , API.createHardLink
    }

data IOCtx m = IOCtx { ctxFS :: SomeHasFS m, openVar :: MVar m Bool }

{-# SPECIALISE guardIsOpen :: IOCtx IO -> IO () #-}
guardIsOpen :: (MonadMVar m, MonadThrow m) => IOCtx m -> m ()
guardIsOpen ctx = readMVar (openVar ctx) >>= \b ->
    unless b $ throwIO (API.mkClosedError (ctxFS ctx) "submitIO")

{-# SPECIALISE initIOCtx :: SomeHasFS IO -> IO (IOCtx IO) #-}
initIOCtx :: MonadMVar m => SomeHasFS m -> m (IOCtx m)
initIOCtx someHasFS = IOCtx someHasFS <$> newMVar True

{-# SPECIALISE close :: IOCtx IO -> IO () #-}
close :: MonadMVar m => IOCtx m -> m ()
close ctx = modifyMVar_ (openVar ctx) $ const (pure False)

{-# SPECIALISE submitIO ::
     HasFS IO h
  -> IOCtx IO -> V.Vector (IOOp RealWorld h)
  -> IO (VU.Vector IOResult) #-}
submitIO ::
     (MonadMVar m, MonadThrow m, PrimMonad m)
  => HasFS m h
  -> IOCtx m
  -> V.Vector (IOOp (PrimState m) h)
  -> m (VU.Vector IOResult)
submitIO hfs ctx ioops = do
    guardIsOpen ctx
    hmapM (ioop hfs) ioops

{-# SPECIALISE ioop :: HasFS IO h -> IOOp RealWorld h -> IO IOResult #-}
-- | Perform the IOOp using synchronous I\/O.
ioop ::
     MonadThrow m
  => HasFS m h
  -> IOOp (PrimState m) h
  -> m IOResult
ioop hfs (IOOpRead h off buf bufOff c) =
    IOResult <$> hGetBufExactlyAt hfs h buf bufOff c (fromIntegral off)
ioop hfs (IOOpWrite h off buf bufOff c) =
    IOResult <$> hPutBufExactlyAt hfs h buf bufOff c (fromIntegral off)

{-# SPECIALISE hmapM ::
     VUM.Unbox b
  => (a -> IO b)
  -> V.Vector a
  -> IO (VU.Vector b) #-}
-- | Heterogeneous blend of 'V.mapM' and 'VU.mapM'.
--
-- The @vector@ package does not provide functions that take distinct vector
-- containers as arguments, so we write it by hand to prevent having to convert
-- one vector type to the other.
hmapM ::
     forall m a b. (PrimMonad m, VUM.Unbox b)
  => (a -> m b)
  -> V.Vector a
  -> m (VU.Vector b)
hmapM f v = do
    res <- VUM.unsafeNew n
    loop res 0
  where
    !n = V.length v
    loop !res !i
      | i == n = VU.unsafeFreeze res
      | otherwise = do
          let !x = v `V.unsafeIndex` i
          !z <- f x
          VUM.unsafeWrite res i z
          loop res (i+1)
