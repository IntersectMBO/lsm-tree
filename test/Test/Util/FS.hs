{-# OPTIONS_GHC -Wno-unrecognised-pragmas #-}

{- HLINT ignore "Redundant if" -}

module Test.Util.FS (
    withTempIOHasFS
  , withTempIOHasBlockIO
  , withSimHasFS
  , withSimHasBlockIO
  , propNoOpenHandles
  , assertNoOpenHandles
  , assertNumOpenHandles
  ) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (assert)
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadThrow)
import           Control.Monad.Primitive (PrimMonad)
import           GHC.Stack
import           System.FS.API
import           System.FS.BlockIO.API
import           System.FS.BlockIO.IO
import           System.FS.BlockIO.Sim (fromHasFS)
import           System.FS.IO
import qualified System.FS.Sim.MockFS as MockFS
import           System.FS.Sim.MockFS
import           System.FS.Sim.STM
import           System.IO.Temp
import           Test.QuickCheck
import           Text.Printf

withTempIOHasFS :: FilePath -> (HasFS IO HandleIO -> IO a) -> IO a
withTempIOHasFS path action = withSystemTempDirectory path $ \dir -> do
    let hfs = ioHasFS (MountPoint dir)
    action hfs

withTempIOHasBlockIO :: FilePath -> (HasFS IO HandleIO -> HasBlockIO IO HandleIO -> IO a) -> IO a
withTempIOHasBlockIO path action =
    withTempIOHasFS path $ \hfs -> do
      withIOHasBlockIO hfs defaultIOCtxParams (action hfs)

{-# INLINABLE withSimHasFS #-}
withSimHasFS :: (MonadSTM m, MonadThrow m, PrimMonad m) => (MockFS -> Property) -> (HasFS m HandleMock -> m Property) -> m Property
withSimHasFS post k = do
    var <- newTMVarIO MockFS.empty
    let hfs = simHasFS var
    x <- k hfs
    fs <- atomically $ readTMVar var
    pure (x .&&. post fs)

{-# INLINABLE withSimHasBlockIO #-}
withSimHasBlockIO :: (MonadMVar m, MonadSTM m, MonadCatch m, PrimMonad m) => (MockFS -> Property) -> (HasFS m HandleMock -> HasBlockIO m HandleMock -> m Property) -> m Property
withSimHasBlockIO post k = do
    withSimHasFS post $ \hfs -> do
      hbio <- fromHasFS hfs
      k hfs hbio

{-# INLINABLE propNoOpenHandles #-}
propNoOpenHandles :: MockFS -> Property
propNoOpenHandles fs = counterexample ("Expected 0 open handles, but found " <> show n) $ n == 0
  where n = numOpenHandles fs

assertNoOpenHandles :: HasCallStack => MockFS -> a -> a
assertNoOpenHandles fs = assertNumOpenHandles fs 0

assertNumOpenHandles :: HasCallStack => MockFS -> Int -> a -> a
assertNumOpenHandles fs m =
    assert $
      if n /= m then
        error (printf "Expected %d open handles, but found %d" m n)
      else
        True
  where n = numOpenHandles fs
