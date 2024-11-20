{-# LANGUAGE CPP          #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Control.RefCount (tests) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Exception
import           Control.Monad
import           Control.RefCount
import           Data.Primitive.PrimVar
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck

#ifdef NO_IGNORE_ASSERTS
import           Data.IORef
#endif

tests :: TestTree
tests = testGroup "Control.RefCount" [
      testProperty "prop_RefCounter"          prop_RefCounter
#ifdef NO_IGNORE_ASSERTS
      -- All of these tests below are checking that the debug implementation of
      -- Ref does indeed detect all the violations (double-free, use-after-free,
      -- never-free). But this obviously depends on the debug implementation
      -- being in use. Hence only tested when NO_IGNORE_ASSERTS.
    , testProperty "prop_ref_double_free"     prop_ref_double_free
    , testProperty "prop_ref_use_after_free"  prop_ref_use_after_free
    , testProperty "prop_ref_never_released0" prop_ref_never_released0
    , testProperty "prop_ref_never_released1" prop_ref_never_released1
    , testProperty "prop_ref_never_released2" prop_ref_never_released2
#endif
    ]

-- | Test for the low level RefCounter API
prop_RefCounter :: Property
prop_RefCounter = once $ ioProperty $ do
    obj <- newMVar False
    ref <- newRefCounter (void $ modifyMVar_ obj (\x -> pure (not x)) )

    incrementRefCounter ref
    n1 <- readRefCount ref -- 2
    b1 <- readMVar obj -- False

    decrementRefCounter ref
    n2 <- readRefCount ref -- 1
    b2 <- readMVar obj -- False

    e2w <- tryIncrementRefCounter ref -- ok, True
    n2w <- readRefCount ref -- 2
    b2w <- readMVar obj -- False
    decrementRefCounter ref

    decrementRefCounter ref
    n3 <- readRefCount ref -- 0
    b3 <- readMVar obj -- True, finaliser ran

    e4 <- try (decrementRefCounter ref) -- error
    n4 <- readRefCount ref -- -1
    b4 <- readMVar obj -- True, finaliser did not run again

    e5 <- try (incrementRefCounter ref) -- error
    n5 <- readRefCount ref -- 0
    b5 <- readMVar obj -- True, finaliser did not run again

    e6 <- tryIncrementRefCounter ref
    n6 <- readRefCount ref -- 0
    b6 <- readMVar obj -- True, finaliser did not run again

    pure $
        counterexample "n1" (n1 == 2) .&&.
        counterexample "b1" (not b1) .&&.

        counterexample "n2" (n2 == 1) .&&.
        counterexample "b2" (not b2) .&&.

        counterexample "e2w" (e2w == True) .&&.
        counterexample "n2w" (n2w == 2) .&&.
        counterexample "b2w" (not b2w) .&&.

        counterexample "n3" (n3 == 0) .&&.
        counterexample "b3" b3 .&&.

        counterexample "e4" (check e4) .&&.
        counterexample "n4" (n4 == (-1)) .&&.
        counterexample "b4" b4 .&&.

        counterexample "e5" (check e5) .&&.
        counterexample "n5" (n5 == 0) .&&.
        counterexample "b5" b5 .&&.

        counterexample "e6" (e6 == False) .&&.
        counterexample "n6" (n6 == 0) .&&.
        counterexample "b6" b6
  where
#ifdef NO_IGNORE_ASSERTS
    check = \case Left (AssertionFailed _) -> True; Right () -> False
#else
    check = \case Left (AssertionFailed _) -> False; Right () -> True
#endif

#ifdef NO_IGNORE_ASSERTS
data TestObject = TestObject !(RefCounter IO)

instance RefCounted TestObject where
    type FinaliserM TestObject = IO
    getRefCounter (TestObject rc) = rc

data TestObject2 = TestObject2 (Ref TestObject)

instance RefCounted TestObject2 where
    type FinaliserM TestObject2 = IO
    getRefCounter (TestObject2 (DeRef to1)) = getRefCounter to1

prop_ref_double_free :: Property
prop_ref_double_free = once $ ioProperty $ do
    finalised <- newIORef False
    ref <- newRef (writeIORef finalised True) TestObject
    releaseRef ref
    True <- readIORef finalised
    Left RefDoubleRelease{} <- try $ releaseRef ref
    checkForgottenRefs

prop_ref_use_after_free :: Property
prop_ref_use_after_free = once $ ioProperty $ do
    finalised <- newIORef False
    ref <- newRef (writeIORef finalised True) TestObject
    releaseRef ref
    True <- readIORef finalised
    Left RefUseAfterRelease{} <- try $ withRef ref return
    Left RefUseAfterRelease{} <- try $ case ref of DeRef _ -> return ()
    Left RefUseAfterRelease{} <- try $ dupRef ref
    checkForgottenRefs

prop_ref_never_released0 :: Property
prop_ref_never_released0 = once $ ioProperty $ do
    finalised <- newIORef False
    ref <- newRef (writeIORef finalised True) TestObject
    _ <- case ref of DeRef _ -> return ()
    checkForgottenRefs
    -- ref is still being used, so check should not fail
    _ <- case ref of DeRef _ -> return ()
    releaseRef ref

prop_ref_never_released1 :: Property
prop_ref_never_released1 =
    once $ ioProperty $
    handle expectRefNeverReleased $ do
      finalised <- newIORef False
      ref <- newRef (writeIORef finalised True) TestObject
      _ <- withRef ref return
      _ <- case ref of DeRef _ -> return ()
      -- ref is never released, so should fail
      checkForgottenRefs
      return (counterexample "no forgotten refs detected" $ property False)

prop_ref_never_released2 :: Property
prop_ref_never_released2 =
    once $ ioProperty $
    handle expectRefNeverReleased $ do
      finalised <- newIORef False
      ref  <- newRef (writeIORef finalised True) TestObject
      ref2 <- dupRef ref
      releaseRef ref
      _ <- withRef ref2 return
      _ <- case ref2 of DeRef _ -> return ()
      -- ref2 is never released, so should fail
      checkForgottenRefs
      return (counterexample "no forgotten refs detected" $ property False)

expectRefNeverReleased :: RefException -> IO Property
expectRefNeverReleased RefNeverReleased{} = return (property True)
expectRefNeverReleased e =
    return (counterexample (displayException e) $ property False)
#endif

