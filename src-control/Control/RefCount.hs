{-# LANGUAGE CPP                    #-}
{-# LANGUAGE DefaultSignatures      #-}
{-# LANGUAGE FunctionalDependencies #-}
{-# LANGUAGE MagicHash              #-}
{-# LANGUAGE PatternSynonyms        #-}
{-# LANGUAGE TypeFamilies           #-}

module Control.RefCount (
    -- * Using references
    Ref(DeRef)
  , releaseRef
  , withRef
  , dupRef
  , RefException (..)
    -- ** Weak references
  , WeakRef (..)
  , mkWeakRef
  , mkWeakRefFromRaw
  , deRefWeak
    -- * Implementing objects with finalisers
  , RefCounted (..)
  , newRef
    -- ** Low level reference counts
  , RefCounter (RefCounter)
  , newRefCounter
  , incrementRefCounter
  , decrementRefCounter
  , tryIncrementRefCounter

  -- * Test API
  , checkForgottenRefs
  ) where

import           Control.DeepSeq
import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Primitive.PrimVar
import           GHC.Stack (CallStack, prettyCallStack)

#ifdef NO_IGNORE_ASSERTS
import           Control.Concurrent (yield)
import qualified Control.Exception
import           Data.IORef
import           GHC.Stack (HasCallStack, callStack)
import           System.IO.Unsafe (unsafeDupablePerformIO, unsafePerformIO)
import           System.Mem.Weak hiding (deRefWeak)
#if MIN_VERSION_base(4,20,0)
import           System.Mem (performBlockingMajorGC)
#else
import           System.Mem (performMajorGC)
#endif
#endif


-------------------------------------------------------------------------------
-- Low level RefCounter API
--

-- | A reference counter with an optional finaliser action. Once the reference
-- count reaches @0@, the finaliser will be run.
data RefCounter m = RefCounter {
    countVar  :: !(PrimVar (PrimState m) Int)
  , finaliser :: !(m ())
  }

instance Show (RefCounter m) where
  show _ = "<RefCounter>"

-- | NOTE: Only strict in the variable and not the referenced value.
instance NFData (RefCounter m) where
  rnf RefCounter{countVar, finaliser} =
      rwhnf countVar `seq` rwhnf finaliser

{-# SPECIALISE newRefCounter :: IO () -> IO (RefCounter IO) #-}
-- | Make a reference counter with initial value @1@.
--
-- The given finaliser is run when the reference counter reaches @0@. The
-- finaliser is run with async exceptions masked.
--
newRefCounter :: PrimMonad m => m () -> m (RefCounter m)
newRefCounter finaliser = do
    countVar <- newPrimVar 1
    return $! RefCounter { countVar, finaliser }

{-# SPECIALISE incrementRefCounter :: RefCounter IO -> IO () #-}
-- | Increase the reference counter by one.
--
-- The count must be known (from context) to be non-zero already. Typically
-- this will be because the caller has a reference already and is handing out
-- another reference to some other code.
incrementRefCounter :: PrimMonad m => RefCounter m -> m ()
incrementRefCounter RefCounter{countVar} = do
    prevCount <- fetchAddInt countVar 1
    assert (prevCount > 0) $ pure ()

{-# SPECIALISE decrementRefCounter :: RefCounter IO -> IO () #-}
-- | Decrease the reference counter by one.
--
-- The count must be known (from context) to be non-zero. Typically this will
-- be because the caller has a reference already (that they took out themselves
-- or were given).
decrementRefCounter :: (PrimMonad m, MonadMask m) => RefCounter m -> m ()
decrementRefCounter RefCounter{countVar, finaliser} =
    --TODO: remove mask and require all uses to run with exceptions mask.
    mask_ $ do
      prevCount <- fetchSubInt countVar 1
      assert (prevCount > 0) $ pure ()
      when (prevCount == 1) finaliser

{-# SPECIALISE tryIncrementRefCounter :: RefCounter IO -> IO Bool #-}
-- | Try to turn a \"weak\" reference on something into a proper reference.
-- This is by analogy with @deRefWeak :: Weak v -> IO (Maybe v)@, but for
-- reference counts.
--
-- This amounts to trying to increase the reference count, but if it is already
-- zero then this will fail. And unlike with 'addReference' where such failure
-- would be a programmer error, this corresponds to the case when the thing the
-- reference count is tracking has been closed already.
--
-- The result is @True@ when a strong reference has been obtained and @False@
-- when upgrading fails.
--
tryIncrementRefCounter :: PrimMonad m => RefCounter m -> m Bool
tryIncrementRefCounter RefCounter{countVar} = do
    prevCount <- atomicReadInt countVar
    casLoop prevCount
  where
    -- A classic lock-free CAS loop.
    -- Check the value before is non-zero, return failure or continue.
    -- Atomically write the new (incremented) value if the old value is
    -- unchanged, and return the old value (either way).
    -- If no other thread changed the old value, we succeed.
    -- Otherwise we go round the loop again.
    casLoop prevCount
      | prevCount <= 0 = return False
      | otherwise      = do
          prevCount' <- casInt countVar prevCount (prevCount+1)
          if prevCount' == prevCount
            then return True
            else casLoop prevCount'


-------------------------------------------------------------------------------
-- Ref API
--

-- | A reference to an object of type @a@. Use references to support prompt
-- finalisation of object resources.
--
-- Rules of use:
--
-- * Each 'Ref' must eventually be released /exactly/ once with 'releaseRef'.
-- * Use 'withRef', or 'DeRef' to (temporarily) obtain the underlying
--   object.
-- * After calling 'releaseRef', the operations 'withRef' and pattern 'DeRef'
--   must /not/ be used.
-- * After calling 'releaseRef', an object obtained previously from
--   'DeRef' must /not/ be used. For this reason, it is advisable to use
--   'withRef' where possible, and be careful with use of 'DeRef'.
-- * A 'Ref' may be duplicated using 'dupRef' to produce an independent
--   reference (which must itself be released with 'releaseRef').
--
-- All of these operations are thread safe. They are not async-exception safe
-- however: the operations that allocate or deallocate must be called with
-- async exceptions masked. This includes 'newRef', 'dupRef' and 'releaseRef'.
--
-- Provided that all these rules are followed, this guarantees that the
-- object's finaliser will be run exactly once, promptly, when the final
-- reference is released.
--
-- In debug mode (when using CPP define @NO_IGNORE_ASSERTS@), adherence to
-- these rules are checked dynamically. These dynamic checks are however not
-- thread safe, so it is not guaranteed that all violations are always detected.
--
#ifndef NO_IGNORE_ASSERTS
newtype Ref obj = Ref { refobj :: obj }
#else
data    Ref obj = Ref { refobj :: !obj, reftracker :: !RefTracker }
#endif

instance Show obj => Show (Ref obj) where
  showsPrec d Ref{refobj} =
    showParen (d > 10) $
      showString "Ref " . showsPrec 11 refobj

instance NFData obj => NFData (Ref obj) where
  rnf Ref{refobj} = rnf refobj

-- | Class of objects which support 'Ref'.
--
-- For objects in this class the guarantee is that (when the 'Ref' rules are
-- followed) the object's finaliser is called exactly once.
--
class RefCounted m obj | obj -> m where
  getRefCounter :: obj -> RefCounter m

#ifdef NO_IGNORE_ASSERTS
#define HasCallStackIfDebug HasCallStack
#else
#define HasCallStackIfDebug ()
#endif

{-# SPECIALISE
    newRef ::
         RefCounted IO obj
      => IO ()
      -> (RefCounter IO -> obj)
      -> IO (Ref obj)
  #-}
-- | Make a new reference.
--
-- The given finaliser is run when the last reference is released. The
-- finaliser is run with async exceptions masked.
--
{-# SPECIALISE
  newRef ::
      RefCounted IO obj
    => IO ()
    -> (RefCounter IO -> obj)
    -> IO (Ref obj)
  #-}
newRef ::
     (RefCounted m obj, PrimMonad m)
  => HasCallStackIfDebug
  => m ()
  -> (RefCounter m -> obj)
  -> m (Ref obj)
newRef finaliser mkObject = do
    rc <- newRefCounter finaliser
    let !obj = mkObject rc
    assert (countVar (getRefCounter obj) == countVar rc) $
      newRefWithTracker obj

-- | Release a reference to an object that will no longer be used (via this
-- reference).
--
{-# SPECIALISE
  releaseRef ::
       RefCounted IO obj
    => Ref obj
    -> IO ()
  #-}
releaseRef ::
     (RefCounted m obj, PrimMonad m, MonadMask m)
  => HasCallStackIfDebug
  => Ref obj
  -> m ()
releaseRef ref@Ref{refobj} = do
    assertNoDoubleRelease ref
    releaseRefTracker ref
    decrementRefCounter (getRefCounter refobj)

{-# COMPLETE DeRef #-}
#if MIN_VERSION_GLASGOW_HASKELL(9,0,0,0)
{-# INLINE DeRef #-}
#endif
-- | Get the object in a 'Ref'. Be careful with retaining the object for too
-- long, since the object must not be used after 'releaseRef' is called.
--
pattern DeRef :: obj -> Ref obj
#ifndef NO_IGNORE_ASSERTS
pattern DeRef obj <- Ref obj
#else
pattern DeRef obj <- (deRef -> !obj) -- So we get assertion checking

deRef :: HasCallStack => Ref obj -> obj
deRef ref@Ref{refobj} =
          unsafeDupablePerformIO (assertNoUseAfterRelease ref)
    `seq` refobj
#endif

{-# SPECIALISE
  withRef ::
       Ref obj
    -> (obj -> IO a)
    -> IO a
  #-}
{-# INLINE withRef #-}
-- | Use the object in a 'Ref'. Do not retain the object after the scope of
-- the body. If you cannot use scoped \"with\" style, use pattern 'DeRef'.
--
withRef ::
     forall m obj a.
     PrimMonad m
  => HasCallStackIfDebug
  => Ref obj
  -> (obj -> m a)
  -> m a
withRef ref@Ref{refobj} f = do
    assertNoUseAfterRelease ref
    f refobj

{-# SPECIALISE
  dupRef ::
       RefCounted IO obj
    => Ref obj
    -> IO (Ref obj)
  #-}
-- | Duplicate an existing reference, to produce a new reference.
--
dupRef ::
     (RefCounted m obj, PrimMonad m)
  => HasCallStackIfDebug
  => Ref obj
  -> m (Ref obj)
dupRef ref@Ref{refobj} = do
    assertNoUseAfterRelease ref
    incrementRefCounter (getRefCounter refobj)
    newRefWithTracker refobj

-- | A \"weak\" reference to an object: that is, a reference that does not
-- guarantee to keep the object alive. If however the object is still alive
-- (due to other normal references still existing) then it can be converted
-- back into a normal reference with 'deRefWeak'.
--
-- Weak references do not themselves need to be released.
--
newtype WeakRef a = WeakRef a
  deriving stock Show

-- | Given an existing normal reference, create a new weak reference.
--
mkWeakRef :: Ref obj -> WeakRef obj
mkWeakRef Ref {refobj} = WeakRef refobj

-- | Given an existing raw reference, create a new weak reference.
--
mkWeakRefFromRaw :: obj -> WeakRef obj
mkWeakRefFromRaw obj = WeakRef obj

{-# SPECIALISE
  deRefWeak ::
       RefCounted IO obj
    => WeakRef obj
    -> IO (Maybe (Ref obj))
  #-}
-- | If the object is still alive, obtain a /new/ normal reference. The normal
-- rules for 'Ref' apply, including the need to eventually call 'releaseRef'.
--
deRefWeak ::
     (RefCounted m obj, PrimMonad m)
  => HasCallStackIfDebug
  => WeakRef obj
  -> m (Maybe (Ref obj))
deRefWeak (WeakRef obj) = do
    success <- tryIncrementRefCounter (getRefCounter obj)
    if success then Just <$> newRefWithTracker obj
               else return Nothing

{-# INLINE newRefWithTracker #-}
#ifndef NO_IGNORE_ASSERTS
newRefWithTracker :: PrimMonad m => obj -> m (Ref obj)
newRefWithTracker obj =
    return $! Ref obj
#else
newRefWithTracker :: (PrimMonad m, HasCallStack) => obj -> m (Ref obj)
newRefWithTracker obj = do
    reftracker' <- newRefTracker callStack
    return $! Ref obj reftracker'
#endif

data RefException =
       RefUseAfterRelease RefId
     | RefDoubleRelease RefId
     | RefNeverReleased RefId CallStack
       -- ^ With call stack for Ref allocation site

newtype RefId = RefId Int
  deriving stock (Show, Eq, Ord)

instance Show RefException where
  --Sigh. QuickCheck still uses 'show' rather than 'displayException.
  show = displayException

instance Exception RefException where
  displayException (RefUseAfterRelease refid) = "RefUseAfterRelease " ++ show refid
  displayException (RefDoubleRelease   refid) = "RefDoubleRelease " ++ show refid
  displayException (RefNeverReleased refid allocsite) =
      "RefNeverReleased " ++ show refid
   ++ "\nAllocation site: " ++ prettyCallStack allocsite

#ifndef NO_IGNORE_ASSERTS

{-# INLINE releaseRefTracker #-}
releaseRefTracker :: PrimMonad m => Ref a -> m ()
releaseRefTracker _ = return ()

{-# INLINE assertNoUseAfterRelease #-}
assertNoUseAfterRelease :: PrimMonad m => Ref a -> m ()
assertNoUseAfterRelease _ = return ()

{-# INLINE assertNoDoubleRelease #-}
assertNoDoubleRelease :: PrimMonad m => Ref a -> m ()
assertNoDoubleRelease _ = return ()

#else

-- | A weak pointer to an outer IORef, containing an inner IORef with a bool
-- to indicate if the ref has been explicitly released.
--
-- The finaliser for the outer weak pointer is given access to the inner IORef
-- so that it can tell if the reference has become garbage without being
-- explicitly released.
--
-- The outer IORef is also stored directly. This ensures the weak pointer to
-- the same is not garbage collected until the RefTracker itself (and thus the
-- parent Ref) is itself garbage collected.
--
-- The inner IORef is mutated when explicitly released. The outer IORef is
-- never modified, but we use an IORef to ensure the weak pointer is reliable.
--
data RefTracker = RefTracker !RefId
                             !(Weak (IORef (IORef Bool)))
                             !(IORef (IORef Bool))

{-# NOINLINE globalRefIdSupply #-}
globalRefIdSupply :: PrimVar RealWorld Int
globalRefIdSupply = unsafePerformIO $ newPrimVar 0

{-# NOINLINE globalForgottenRef #-}
globalForgottenRef :: IORef (Maybe (RefId, CallStack))
globalForgottenRef = unsafePerformIO $ newIORef Nothing

newRefTracker :: PrimMonad m => CallStack -> m RefTracker
newRefTracker callsite = unsafeIOToPrim $ do
    inner <- newIORef False
    outer <- newIORef inner
    refid <- fetchAddInt globalRefIdSupply 1
    weak  <- mkWeakIORef outer $
               finaliserRefTracker inner (RefId refid) callsite
    return (RefTracker (RefId refid) weak outer)

releaseRefTracker :: PrimMonad m => Ref a -> m ()
releaseRefTracker Ref { reftracker =  RefTracker _refid _weak outer } =
  unsafeIOToPrim $ do
    inner <- readIORef outer
    writeIORef inner True

finaliserRefTracker :: IORef Bool -> RefId -> CallStack -> IO ()
finaliserRefTracker inner refid callsite = do
    released <- readIORef inner
    when (not released) $ do
      -- Uh oh! Forgot a reference without releasing!
      -- Add it to a global var which we can poll elsewhere.
      mref <- readIORef globalForgottenRef
      case mref of
        -- Just keep one, but keep the last allocated one.
        -- The reason for last is that when there are nested structures with
        -- refs then the last allocated is likely to be the outermost, which
        -- is the best place to start hunting for ref leaks. Otherwise one can
        -- go on a wild goose chase tracking down inner refs that were only
        -- forgotten due to an outer ref being forgotten.
        Just (refid', _) | refid < refid' -> return ()
        _ -> writeIORef globalForgottenRef (Just (refid, callsite))

assertNoForgottenRefs :: IO ()
assertNoForgottenRefs = do
    mrefs <- readIORef globalForgottenRef
    case mrefs of
      Nothing                -> return ()
      Just (refid, callsite) -> do
        -- Clear the var so we don't assert again.
        writeIORef globalForgottenRef Nothing
        throwIO (RefNeverReleased refid callsite)

assertNoUseAfterRelease :: (PrimMonad m, HasCallStack) => Ref a -> m ()
assertNoUseAfterRelease Ref { reftracker = RefTracker refid _weak outer } =
  unsafeIOToPrim $ do
    released <- readIORef =<< readIORef outer
    when released $ Control.Exception.throwIO (RefUseAfterRelease refid)
    assertNoForgottenRefs
#if !(MIN_VERSION_base(4,20,0))
  where
    _unused = callStack
#endif

assertNoDoubleRelease :: (PrimMonad m, HasCallStack) => Ref a -> m ()
assertNoDoubleRelease Ref { reftracker = RefTracker refid _weak outer } =
  unsafeIOToPrim $ do
    released <- readIORef =<< readIORef outer
    when released $ Control.Exception.throwIO (RefDoubleRelease refid)
    assertNoForgottenRefs
#if !(MIN_VERSION_base(4,20,0))
  where
    _unused = callStack
#endif

#endif

-- | Run a GC to try and see if any refs have been forgotten without being
-- released. If so, this will throw a synchronous exception.
--
-- Note however that this is not the only place where 'RefNeverReleased'
-- exceptions can be thrown. All Ref operations poll for forgotten refs.
--
checkForgottenRefs :: IO ()
checkForgottenRefs = do
#ifndef NO_IGNORE_ASSERTS
    return ()
#else
    -- The hope is that by combining `performMajorGC` with `yield` that the
    -- former starts the finalizer threads for all dropped weak references and
    -- the latter suspends the current process and puts it at the end of the
    -- thread queue, such that when the current process resumes the finalizer
    -- threads for all dropped weak references have finished.
    -- Unfortunately, this relies on the implementation of the GHC scheduler,
    -- not on any Haskell specification, and is therefore both non-portable and
    -- presumably rather brittle. Therefore, for good measure, we do it twice.
    performMajorGCWithBlockingIfAvailable
    yield
    performMajorGCWithBlockingIfAvailable
    yield
    assertNoForgottenRefs
  where
#endif

#ifdef NO_IGNORE_ASSERTS
performMajorGCWithBlockingIfAvailable :: IO ()
performMajorGCWithBlockingIfAvailable = do
#if MIN_VERSION_base(4,20,0)
    performBlockingMajorGC
#else
    performMajorGC
#endif
#endif
