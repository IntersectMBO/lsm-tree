module Test.Control.ActionRegistry (tests) where

import           Control.ActionRegistry
import           Control.Monad.Class.MonadThrow
import           Data.IORef
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Text.Printf (printf)

tests :: TestTree
tests = testGroup "Test.Control.ActionRegistry" [
      testProperty "prop_commitActionRegistryError" prop_commitActionRegistryError
    , testProperty "prop_abortActionRegistryError" prop_abortActionRegistryError
      -- * Modify mutable state
    , testProperty "prop_modifyWithActionRegistry" prop_modifyWithActionRegistry
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

{-------------------------------------------------------------------------------
  Modify mutable state
-------------------------------------------------------------------------------}

data Handle = Handle {
    handleID    :: Int
  , handleState :: IORef Bool
  }

openHandle :: Int -> IO Handle
openHandle x = Handle x  <$> newIORef True

closeHandle :: Handle -> IO ()
closeHandle h = do
    isOpen <- readIORef (handleState h)
    if isOpen then
      writeIORef (handleState h) False
    else
      error $ printf "Handle %d is already closed" (handleID h)

type Errors = [Bool]

nextError :: IORef Errors -> IO Bool
nextError errsVar = do
    bs <- readIORef errsVar
    case bs of
      [] -> pure False
      (b:bs') -> do
        writeIORef errsVar bs'
        pure b

withErrors :: IORef Errors -> IO a -> IO a
withErrors errsVar k = do
    b <- nextError errsVar
    if b then error "oops" else k


prop_modifyWithActionRegistry :: Errors -> Property
prop_modifyWithActionRegistry errs = ioProperty $ do
    errsVar <- newIORef errs
    st <- openHandle 0 >>= newIORef
    e <- try @_ @SomeException $
      modifyWithActionRegistry
        (readIORef st)
        (writeIORef st)
        $ \reg h -> do
            h' <- withRollback reg
                    (withErrors errsVar (openHandle 1))
                    (withErrors errsVar . closeHandle)
            delayedCommit reg (withErrors errsVar (closeHandle h))
            withErrors errsVar $ pure (h', 'a')
    h <- readIORef st
    isOpen <- readIORef (handleState h)
    pure $ case e of
      Left{}  -> handleID h === 0 .&&. isOpen
      Right{} -> handleID h === 1 .&&. isOpen
