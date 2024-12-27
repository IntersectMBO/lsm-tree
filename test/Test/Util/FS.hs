
{- HLINT ignore "Redundant if" -}

module Test.Util.FS (
    -- * Real file system
    withTempIOHasFS
  , withTempIOHasBlockIO
    -- * Simulated file system
  , withSimHasFS
  , withSimHasBlockIO
    -- * Simulated file system with errors
  , withSimErrorHasFS
  , withSimErrorHasFS'
  , withSimErrorHasBlockIO
  , withSimErrorHasBlockIO'
    -- * Simulated file system properties
  , propNumOpenHandles
  , propNoOpenHandles
  , propNumDirEntries
  , propNoDirEntries
  , assertNoOpenHandles
  , assertNumOpenHandles
    -- * Equality
  , approximateEqStream
  ) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (assert)
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadThrow)
import           Control.Monad.IOSim (runSimOrThrow)
import           Control.Monad.Primitive (PrimMonad)
import qualified Data.Set as Set
import           GHC.Stack
import           System.FS.API as FS
import           System.FS.BlockIO.API
import           System.FS.BlockIO.IO
import           System.FS.BlockIO.Sim (fromHasFS)
import           System.FS.IO
import           System.FS.Sim.Error
import qualified System.FS.Sim.MockFS as MockFS
import           System.FS.Sim.MockFS
import           System.FS.Sim.STM
import           System.FS.Sim.Stream (InternalInfo (..), Stream (..))
import           System.IO.Temp
import           Test.QuickCheck
import           Text.Printf

{-------------------------------------------------------------------------------
  Real file system
-------------------------------------------------------------------------------}

withTempIOHasFS :: FilePath -> (HasFS IO HandleIO -> IO a) -> IO a
withTempIOHasFS path action = withSystemTempDirectory path $ \dir -> do
    let hfs = ioHasFS (MountPoint dir)
    action hfs

withTempIOHasBlockIO :: FilePath -> (HasFS IO HandleIO -> HasBlockIO IO HandleIO -> IO a) -> IO a
withTempIOHasBlockIO path action =
    withTempIOHasFS path $ \hfs -> do
      withIOHasBlockIO hfs defaultIOCtxParams (action hfs)

{-------------------------------------------------------------------------------
  Simulated file system
-------------------------------------------------------------------------------}

{-# INLINABLE withSimHasFS #-}
withSimHasFS ::
     (MonadSTM m, MonadThrow m, PrimMonad m)
  => (MockFS -> Property)
  -> (HasFS m HandleMock -> m Property)
  -> m Property
withSimHasFS post k = do
    var <- newTMVarIO MockFS.empty
    let hfs = simHasFS var
    x <- k hfs
    fs <- atomically $ readTMVar var
    pure (x .&&. post fs)

{-# INLINABLE withSimHasBlockIO #-}
withSimHasBlockIO ::
     (MonadMVar m, MonadSTM m, MonadCatch m, PrimMonad m)
  => (MockFS -> Property)
  -> (HasFS m HandleMock -> HasBlockIO m HandleMock -> m Property)
  -> m Property
withSimHasBlockIO post k = do
    withSimHasFS post $ \hfs -> do
      hbio <- fromHasFS hfs
      k hfs hbio

{-------------------------------------------------------------------------------
  Simulated file system with errors
-------------------------------------------------------------------------------}

{-# INLINABLE withSimErrorHasFS #-}
withSimErrorHasFS ::
     (MonadSTM m, MonadThrow m, PrimMonad m, Testable prop1, Testable prop2)
  => (MockFS -> prop1)
  -> MockFS
  -> Errors
  -> (  HasFS m HandleMock
     -> StrictTMVar m MockFS
     -> StrictTVar m Errors
     -> m prop2
     )
  -> m Property
withSimErrorHasFS post fs errs k = do
    fsVar <- newTMVarIO fs
    errVar <- newTVarIO errs
    let hfs = simErrorHasFS fsVar errVar
    x <- k hfs fsVar errVar
    fs' <- atomically $ readTMVar fsVar
    pure (x .&&. post fs')

{-# INLINABLE withSimErrorHasFS' #-}
withSimErrorHasFS' ::
     (MonadSTM m, MonadThrow m, PrimMonad m, Testable prop1, Testable prop2)
  => (MockFS -> prop1)
  -> MockFS
  -> Errors
  -> (HasFS m HandleMock -> m prop2)
  -> m Property
withSimErrorHasFS' post fs errs k = do
    fsVar <- newTMVarIO fs
    errVar <- newTVarIO errs
    let hfs = simErrorHasFS fsVar errVar
    x <- k hfs
    fs' <- atomically $ readTMVar fsVar
    pure (x .&&. post fs')

{-# INLINABLE withSimErrorHasBlockIO #-}
withSimErrorHasBlockIO ::
     ( MonadSTM m, MonadCatch m, MonadMVar m, PrimMonad m
     , Testable prop1, Testable prop2
     )
  => (MockFS -> prop1)
  -> Errors
  -> (  HasFS m HandleMock
     -> HasBlockIO m HandleMock
     -> StrictTMVar m MockFS
     -> StrictTVar m Errors
     -> m prop2
     )
  -> m Property
withSimErrorHasBlockIO post errs k = do
    fsVar <- newTMVarIO MockFS.empty
    errVar <- newTVarIO errs
    let hfs = simErrorHasFS fsVar errVar
    hbio <- fromHasFS hfs
    x <- k hfs hbio fsVar errVar
    fs <- atomically $ readTMVar fsVar
    pure (x .&&. post fs)

{-# INLINABLE withSimErrorHasBlockIO' #-}
withSimErrorHasBlockIO' ::
     ( MonadSTM m, MonadCatch m, MonadMVar m, PrimMonad m
     , Testable prop1, Testable prop2
     )
  => (MockFS -> prop1)
  -> Errors
  -> (HasFS m HandleMock -> HasBlockIO m HandleMock -> m prop2)
  -> m Property
withSimErrorHasBlockIO' post errs k = do
    fsVar <- newTMVarIO MockFS.empty
    errVar <- newTVarIO errs
    let hfs = simErrorHasFS fsVar errVar
    hbio <- fromHasFS hfs
    x <- k hfs hbio
    fs <- atomically $ readTMVar fsVar
    pure (x .&&. post fs)

{-------------------------------------------------------------------------------
  Simulated file system properties
-------------------------------------------------------------------------------}

{-# INLINABLE propNumOpenHandles #-}
propNumOpenHandles :: MockFS -> Int -> Property
propNumOpenHandles fs expected =
    counterexample (printf "Expected %d open handles, but found %d" expected actual) $
    counterexample ("Open handles: " <> show (openHandles fs)) $
    printMockFSOnFailure fs $
    expected == actual
  where actual = numOpenHandles fs

{-# INLINABLE propNoOpenHandles #-}
propNoOpenHandles :: MockFS -> Property
propNoOpenHandles fs = propNumOpenHandles fs 0

{-# INLINABLE propNumDirEntries #-}
propNumDirEntries :: MockFS -> Int -> Property
propNumDirEntries fs expected =
    counterexample
      (printf "Expected %d entries in the directory, but found %d" expected actual) $
    printMockFSOnFailure fs $
    expected === actual
  where
    actual =
      let (contents, _) = runSimOrThrow $
            runSimFS fs $ \hfs ->
              FS.listDirectory hfs (mkFsPath [])
      in  Set.size contents

{-# INLINABLE propNoDirEntries #-}
propNoDirEntries :: MockFS -> Property
propNoDirEntries fs = propNumDirEntries fs 0

printMockFSOnFailure :: Testable prop => MockFS -> prop -> Property
printMockFSOnFailure fs = counterexample ("Mocked file system: " <> pretty fs)

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

{-------------------------------------------------------------------------------
  Equality
-------------------------------------------------------------------------------}

-- | Approximate equality for streams.
--
-- Equality is checked as follows:
-- * Infinite streams are equal: any infinity is as good as another infinity
-- * Finite streams are are checked for pointwise equality on their elements.
-- * Other streams are trivially unequal: they do not have matching finiteness
--
-- This approximate equality satisfies the __Reflexivity__, __Symmetry__,
-- __Transitivity__ and __Negation__ laws for the 'Eq' class, but not
-- __Substitutivity.
--
-- TODO: upstream to fs-sim
approximateEqStream :: Eq a => Stream a -> Stream a -> Bool
approximateEqStream (UnsafeStream infoXs xs) (UnsafeStream infoYs ys) =
    case (infoXs, infoYs) of
      (Infinite, Infinite) -> True
      (Finite, Finite)     ->  xs == ys
      (_, _)               -> False
