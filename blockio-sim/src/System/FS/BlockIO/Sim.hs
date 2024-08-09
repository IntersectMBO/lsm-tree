module System.FS.BlockIO.Sim (
    fromHasFS
  , simHasBlockIO
  , simHasBlockIO'
  , simErrorHasBlockIO
  , simErrorHasBlockIO'
  ) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad)
import           System.FS.API
import           System.FS.BlockIO.API (HasBlockIO (..))
import           System.FS.BlockIO.Serial
import           System.FS.Sim.Error
import           System.FS.Sim.MockFS
import           System.FS.Sim.STM

fromHasFS ::
     (MonadThrow m, MonadMVar m, PrimMonad m)
  => HasFS m HandleMock
  -> m (HasBlockIO m HandleMock)
fromHasFS = serialHasBlockIO hSetNoCache hAdvise hAllocate
  where
    -- TODO: It should be possible for the implementations and simulation to
    -- throw an FsError when doing file I/O with misaligned byte arrays after
    -- hSetNoCache. Maybe they should? However, to do that we'd have to be able
    -- to move hSetNoCache into fs-api and fs-sim because we'd need access to
    -- the internals.
    hSetNoCache _h _b = pure ()
    hAdvise _ _ _ _ = pure ()
    hAllocate _ _ _ = pure ()

simHasBlockIO ::
     (MonadThrow m, MonadMVar m, PrimMonad m, MonadSTM m)
  => StrictTMVar m MockFS
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simHasBlockIO var = do
    let hfs = simHasFS var
    hbio <- fromHasFS hfs
    pure (hfs, hbio)

simHasBlockIO' ::
     (MonadThrow m, MonadMVar m, PrimMonad m, MonadSTM m)
  => MockFS
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simHasBlockIO' mockFS = do
    hfs <- simHasFS' mockFS
    hbio <- fromHasFS hfs
    pure (hfs, hbio)

simErrorHasBlockIO ::
     forall m. (MonadThrow m, MonadMVar m, PrimMonad m, MonadSTM m)
  => StrictTMVar m MockFS
  -> StrictTVar m Errors
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simErrorHasBlockIO fsVar errorsVar = do
    let hfs = simErrorHasFS fsVar errorsVar
    hbio <- fromHasFS hfs
    pure (hfs, hbio)

simErrorHasBlockIO' ::
     (MonadThrow m, MonadMVar m, PrimMonad m, MonadSTM m)
  => MockFS
  -> Errors
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simErrorHasBlockIO' mockFS errs = do
    hfs <- simErrorHasFS' mockFS errs
    hbio <- fromHasFS hfs
    pure (hfs, hbio)
