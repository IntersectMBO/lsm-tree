{-# LANGUAGE CPP          #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Control.RefCount (tests) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Exception (AssertionFailed (..))
import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Control.Monad.IOSim (IOSim, runSimOrThrow)
import           Control.Monad.Primitive
import           Control.RefCount
import           Data.Primitive.PrimVar
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck

#ifdef NO_IGNORE_ASSERTS
import           Data.Primitive
#endif

tests :: TestTree
tests = testGroup "Test.Control.RefCount" [
      testProperty "prop_RefCounter @IO" $
        ioPropertyOnce prop_RefCounter
#ifndef NO_IGNORE_ASSERTS
      -- prop_RefCounter throws and catches AssertionFailed exceptions, but we
      -- can only catch these exceptions in IO. In IOSim, these uncaught
      -- exceptions will lead to property failures. So, we only run the property
      -- in IOSim if assertions are turned off.
    , testProperty "prop_RefCounter @IOSim" $
        ioSimPropertyOnce prop_RefCounter
#endif
#ifdef NO_IGNORE_ASSERTS
      -- All of these tests below are checking that the debug implementation of
      -- Ref does indeed detect all the violations (double-free, use-after-free,
      -- never-free). But this obviously depends on the debug implementation
      -- being in use. Hence only tested when NO_IGNORE_ASSERTS.
    , testGroup "IO" [
          testProperty "prop_ref_double_free" $
            ioPropertyOnce prop_ref_double_free
        , testProperty "prop_ref_use_after_free"  $
            ioPropertyOnce $ prop_ref_use_after_free True
        , testProperty "prop_ref_never_released0" $
            ioPropertyOnce prop_ref_never_released0
        , testProperty "prop_ref_never_released1" $
            ioPropertyOnce prop_ref_never_released1
        , testProperty "prop_ref_never_released2" $
            ioPropertyOnce prop_ref_never_released2
        , testProperty "prop_release_ref_exception" $
            ioPropertyOnce prop_release_ref_exception
        ]
    , testGroup "IOSim" [
          testProperty "prop_ref_double_free" $
            ioSimPropertyOnce prop_ref_double_free
        , testProperty "prop_ref_use_after_free"  $
            -- Exceptions thrown by the DeRef pattern can only be caught from
            -- IO, so we do not test the DeRef pattern in IOSim.
            ioSimPropertyOnce $ prop_ref_use_after_free False
        , testProperty "prop_ref_never_released0" $
            ioSimPropertyOnce prop_ref_never_released0
        , testProperty "prop_ref_never_released1" $
            ioSimPropertyOnce prop_ref_never_released1
        , testProperty "prop_ref_never_released2" $
            ioSimPropertyOnce prop_ref_never_released2
        , testProperty "prop_release_ref_exception" $
            ioSimPropertyOnce prop_release_ref_exception
        ]
#endif
    ]

ioPropertyOnce :: Testable prop => IO prop -> Property
ioPropertyOnce p = once $ ioProperty p

ioSimPropertyOnce :: Testable prop => (forall s. IOSim s prop) -> Property
ioSimPropertyOnce p = once $ property $ runSimOrThrow p

-- | Test for the low level RefCounter API
prop_RefCounter ::
     (MonadMVar m, PrimMonad m, MonadMask m)
  => m Property
prop_RefCounter = do
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

-- | Warning: reading the current reference count is inherently racy as there
-- is no way to reliably act on the information. It is only useful for tests or
-- debugging.
--
readRefCount :: PrimMonad m => RefCounter m -> m Int
readRefCount (RefCounter countVar _) = readPrimVar countVar

#ifdef NO_IGNORE_ASSERTS
data TestObject m = TestObject !(RefCounter m)

instance RefCounted m (TestObject m) where
    getRefCounter (TestObject rc) = rc

data TestObject2 m = TestObject2 (Ref (TestObject m))

instance RefCounted m (TestObject2 m) where
    getRefCounter (TestObject2 (DeRef to1)) = getRefCounter to1

prop_ref_double_free ::
     (PrimMonad m, MonadMask m, MonadFail m)
  => m Property
prop_ref_double_free = withRefCtx $ \refCtx -> do
    finalised <- newMutVar False
    ref <- newRef refCtx (writeMutVar finalised True) TestObject
    releaseRef ref
    True <- readMutVar finalised
    Left e@RefDoubleRelease{} <- try $ releaseRef ref
    checkForgottenRefs refCtx
    -- Print the displayed exception as an example
    pure $ tabulate "displayException" [displayException e] ()

prop_ref_use_after_free ::
     (PrimMonad m, MonadMask m, MonadFail m)
  => Bool -- ^ Test the DeRef pattern
  -> m Property
prop_ref_use_after_free testDeRef = withRefCtx $ \refCtx -> do
    finalised <- newMutVar False
    ref <- newRef refCtx (writeMutVar finalised True) TestObject
    releaseRef ref
    True <- readMutVar finalised
    Left e@RefUseAfterRelease{} <- try $ withRef ref return
    when testDeRef $ do
      Left RefUseAfterRelease{} <- try $ case ref of DeRef _ -> pure ()
      pure ()
    Left RefUseAfterRelease{} <- try $ dupRef ref
    checkForgottenRefs refCtx
    -- Print the displayed exception as an example
    pure $ tabulate "displayException" [displayException e] ()

prop_ref_never_released0 ::
     (PrimMonad m, MonadMask m)
  => m ()
prop_ref_never_released0 = withRefCtx $ \refCtx -> do
    finalised <- newMutVar False
    ref <- newRef refCtx (writeMutVar finalised True) TestObject
    _ <- case ref of DeRef _ -> pure ()
    checkForgottenRefs refCtx
    -- ref is still being used, so check should not fail
    _ <- case ref of DeRef _ -> pure ()
    releaseRef ref

prop_ref_never_released1 ::
     (PrimMonad m, MonadMask m)
  => m Property
prop_ref_never_released1 = withRefCtx $ \refCtx -> do
    handle expectRefNeverReleased $ do
      finalised <- newMutVar False
      ref <- newRef refCtx (writeMutVar finalised True) TestObject
      _ <- withRef ref return
      _ <- case ref of DeRef _ -> pure ()
      -- ref is never released, so should fail
      checkForgottenRefs refCtx
      pure (counterexample "no forgotten refs detected" $ property False)

prop_ref_never_released2 ::
     (PrimMonad m, MonadMask m)
  => m Property
prop_ref_never_released2 = withRefCtx $ \refCtx -> do
    handle expectRefNeverReleased $ do
      finalised <- newMutVar False
      ref  <- newRef refCtx (writeMutVar finalised True) TestObject
      ref2 <- dupRef ref
      releaseRef ref
      _ <- withRef ref2 return
      _ <- case ref2 of DeRef _ -> pure ()
      -- ref2 is never released, so should fail
      checkForgottenRefs refCtx
      pure (counterexample "no forgotten refs detected" $ property False)

expectRefNeverReleased :: Monad m => RefException -> m Property
expectRefNeverReleased e@RefNeverReleased{} =
    -- Print the displayed exception as an example
    pure (tabulate "displayException" [displayException e] (property True))
expectRefNeverReleased e =
    pure (counterexample (displayException e) $ property False)

-- | If a finaliser throws an exception, then the 'RefTracker' is still released
prop_release_ref_exception ::
     (PrimMonad m, MonadMask m)
  => m ()
prop_release_ref_exception = withRefCtx $ \refCtx -> do
    finalised <- newMutVar False
    ref  <- newRef refCtx (writeMutVar finalised True >> throwIO (userError "oops")) TestObject
    _ <- try @_ @SomeException (releaseRef ref)
    checkForgottenRefs refCtx
#endif

