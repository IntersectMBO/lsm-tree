module Test.Database.LSMTree.Internal.RunBuilder.FS (tests) where

import           Control.ActionRegistry
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (SomeException (..))
import           Control.Monad.Class.MonadThrow
import           Data.Typeable
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import qualified Database.LSMTree.Internal.RunBuilder as RunBuilder
import           Database.LSMTree.Internal.RunNumber
import           System.FS.API
import           System.FS.Sim.Error
import           System.FS.Sim.MockFS as MockFS
import           System.FS.Sim.Stream as S
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.RunBuilder.FS" [
      testProperty "prop_fault_new" prop_fault_new
    ]

prop_fault_new :: TestErrors -> Property
prop_fault_new (TestErrors gmode@GenMode{..} errs) =
    ioProperty $
    withSim $ \hfs hbio fsVar errsVar -> do
      res <-
        withErrors errsVar errs $
          try @_ @SomeException $ new hfs hbio

      fs <- atomically $ readTMVar fsVar

      pure $ tabulate "GenMode" [show gmode] $ case res of
        Left e ->
            tabulate "Result" [showExceptionType e]
          $ propAllowedError e .&&. propCleanup fs
        Right _rb ->
            tabulate "Result" ["Success"]
          $ propNumOpenHandles 4 fs .&&. propNumDirEntries (mkFsPath []) 4 fs
  where
    withSim = withSimErrorHasBlockIO (\_ -> True) MockFS.empty emptyErrors

    new hfs hbio = RunBuilder.new hfs hbio rfsp (NumEntries 0) (RunAllocFixed 10)
    rfsp = RunFsPaths (mkFsPath []) (RunNumber 17)

    showExceptionType :: SomeException -> String
    showExceptionType (SomeException e) = show $ typeOf e

    propAllowedError :: SomeException -> Property
    propAllowedError e
      | Just FsError{} <- fromException e
      = property True
      | Just AbortActionRegistryError{} <- fromException e
      = property True
      | otherwise
      = counterexample ("Not an allowed error: " <> show e) (property False)

    -- | Check that handles and files are cleaned up properly if there was an
    -- exception.
    propCleanup :: MockFS -> Property
    propCleanup fs = conjoin [
          -- If no errors are generated for 'hClose', then all handles should be
          -- cleaned up properly.
          if not genHCloseE
            then propNoOpenHandles fs
            else property True
          -- If no errors are generated for 'hClose' and 'removeFile', then all
          -- files should be cleaned up properly
        , if not genRemoveFileE && not genHCloseE
            then propNoDirEntries (mkFsPath []) fs
            else property True
        ]

{-------------------------------------------------------------------------------
  Generators
-------------------------------------------------------------------------------}

data TestErrors = TestErrors GenMode Errors
  deriving stock Show

instance Arbitrary TestErrors where
  arbitrary = do
      gmode <- arbitrary
      errs <- arbitrary
      pure (TestErrors gmode (applyGenMode gmode errs))
  shrink (TestErrors gmode errs) =
      [ TestErrors gmode' errs'
      | gmode' <- shrink gmode
      , let errs' = applyGenMode gmode' errs
      ] <> [
        TestErrors gmode errs'
      | errs' <- shrink errs
      ]

-- | Different modes for generating 'Errors'
data GenMode = GenMode {
    -- | Do or do not generate errors for 'hClose'
    genHCloseE     :: Bool
    -- | Do or do not generate errors for 'removeFile'
  , genRemoveFileE :: Bool
  }
  deriving stock Show

instance Arbitrary GenMode where
  arbitrary = GenMode <$> arbitrary <*> arbitrary
  shrink (GenMode x y) = [GenMode x' y' | (x', y') <- shrink (x, y)]

applyGenMode :: GenMode -> Errors -> Errors
applyGenMode GenMode{..} = noRemoveFileE . noHCloseE
  where
    noHCloseE errs =
      if genHCloseE then
        errs
      else
        errs { hCloseE = S.empty }

    noRemoveFileE errs =
      if genRemoveFileE then
        errs
      else
        errs { removeFileE = S.empty }
