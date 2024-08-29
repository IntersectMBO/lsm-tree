{-# LANGUAGE CPP #-}

module Test.Database.LSMTree.Internal.RefCount (tests) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Exception (AssertionFailed (..))
import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Database.LSMTree.Internal.RefCount
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RefCount" [
      testProperty "prop_refCount" prop_refCount
    ]

prop_refCount :: Property
prop_refCount = once $ ioProperty $ do
    obj <- newMVar False
    ref <- mkRefCounter1 $ Just (void $ modifyMVar_ obj (\x -> pure (not x)) )

    addReference ref
    n1 <- readRefCount ref -- 2
    b1 <- readMVar obj -- False

    removeReference ref
    n2 <- readRefCount ref -- 1
    b2 <- readMVar obj -- False

    removeReference ref
    n3 <- readRefCount ref -- 0
    b3 <- readMVar obj -- True, finaliser ran

    e4 <- try (removeReference ref) -- error
    n4 <- readRefCount ref -- -1
    b4 <- readMVar obj -- True, finaliser did not run again

    e5 <- try (addReference ref) -- error
    n5 <- readRefCount ref -- 0
    b5 <- readMVar obj -- True, finaliser did not run again

    pure $
        counterexample "n1" (n1 == RefCount 2) .&&.
        counterexample "b1" (not b1) .&&.

        counterexample "n2" (n2 == RefCount 1) .&&.
        counterexample "b2" (not b2) .&&.

        counterexample "n3" (n3 == RefCount 0) .&&.
        counterexample "b3" b3 .&&.

        counterexample "e4" (check e4) .&&.
        counterexample "n4" (n4 == RefCount (-1)) .&&.
        counterexample "b4" b4 .&&.

        counterexample "e5" (check e5) .&&.
        counterexample "n5" (n5 == RefCount 0) .&&.
        counterexample "b5" b5
  where
#ifdef NO_IGNORE_ASSERTS
    check = \case Left (AssertionFailed _) -> True; Right () -> False
#else
    check = \case Left (AssertionFailed _) -> False; Right () -> True
#endif
