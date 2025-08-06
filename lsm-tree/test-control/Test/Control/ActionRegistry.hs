module Test.Control.ActionRegistry (tests) where

import           Control.ActionRegistry
import           Control.Monad.Class.MonadThrow
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Control.ActionRegistry" [
      testProperty "prop_commitActionRegistryError" prop_commitActionRegistryError
    , testProperty "prop_abortActionRegistryError" prop_abortActionRegistryError
    ]

-- | An example where an exception happens while an action registry is being
-- committed.
prop_commitActionRegistryError :: Property
prop_commitActionRegistryError = once $ ioProperty $ do
    eith <-
      try @_ @CommitActionRegistryError $
        withActionRegistry $ \reg -> do
          delayedCommit reg
            (throwIO (userError "delayed action failed"))
    pure $ case eith of
      Left e   ->
        tabulate "displayException" [displayExceptionNewline e] $ property True
      Right () -> property False

-- | An example where an exception happens while an action registry is being
-- aborted.
prop_abortActionRegistryError :: Property
prop_abortActionRegistryError = once $ ioProperty $ do
    eith <-
      try @_ @AbortActionRegistryError $
        withActionRegistry $ \reg -> do
          withRollback reg
            (pure ())
            (\_ -> throwIO (userError "rollback action failed"))
          throwIO (userError "error in withActionRegistry scope")
    pure $ case eith of
      Left e   ->
        tabulate "displayException" [displayExceptionNewline e] $ property True
      Right () -> property False

displayExceptionNewline :: Exception e => e -> String
displayExceptionNewline e = '\n':displayException e
