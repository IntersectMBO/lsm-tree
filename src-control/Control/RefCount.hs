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
  , ignoreForgottenRefs
  , enableForgottenRefChecks
  , disableForgottenRefChecks
  ) where

import           Control.DeepSeq
import           Control.Exception (assert)
import           Control.Monad (void, when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Primitive.PrimVar
import           GHC.Show (appPrec)
import           GHC.Stack (CallStack, prettyCallStack)

#ifdef NO_IGNORE_ASSERTS
import           Control.Concurrent (yield)
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
    pure $! RefCounter { countVar, finaliser }

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
      | prevCount <= 0 = pure False
      | otherwise      = do
          prevCount' <- casInt countVar prevCount (prevCount+1)
          if prevCount' == prevCount
            then pure True
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
    => HasCallStackIfDebug
    => IO ()
    -> (RefCounter IO -> obj)
    -> IO (Ref obj)
  #-}
-- | Make a new reference.
--
-- The given finaliser is run when the last reference is released. The
-- finaliser is run with async exceptions masked.
--
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
    => HasCallStackIfDebug
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
    assertNoForgottenRefs
    releaseRefTracker ref
    decrementRefCounter (getRefCounter refobj)

{-# COMPLETE DeRef #-}
{-# INLINE DeRef #-}
-- | Get the object in a 'Ref'. Be careful with retaining the object for too
-- long, since the object must not be used after 'releaseRef' is called.
--
pattern DeRef :: HasCallStackIfDebug => obj -> Ref obj
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
       HasCallStackIfDebug
    => Ref obj
    -> (obj -> IO a)
    -> IO a
  #-}
{-# INLINE withRef #-}
-- | Use the object in a 'Ref'. Do not retain the object after the scope of
-- the body. If you cannot use scoped \"with\" style, use pattern 'DeRef'.
--
withRef ::
     forall m obj a.
     (PrimMonad m, MonadThrow m)
  => HasCallStackIfDebug
  => Ref obj
  -> (obj -> m a)
  -> m a
withRef ref@Ref{refobj} f = do
    assertNoUseAfterRelease ref
    assertNoForgottenRefs
    f refobj
#ifndef NO_IGNORE_ASSERTS
  where
    _unused = throwIO @m @SomeException
#endif

{-# SPECIALISE
  dupRef ::
       RefCounted IO obj
    => HasCallStackIfDebug
    => Ref obj
    -> IO (Ref obj)
  #-}
-- | Duplicate an existing reference, to produce a new reference.
--
dupRef ::
     forall m obj. (RefCounted m obj, PrimMonad m, MonadThrow m)
  => HasCallStackIfDebug
  => Ref obj
  -> m (Ref obj)
dupRef ref@Ref{refobj} = do
    assertNoUseAfterRelease ref
    assertNoForgottenRefs
    incrementRefCounter (getRefCounter refobj)
    newRefWithTracker refobj
#ifndef NO_IGNORE_ASSERTS
  where
    _unused = throwIO @m @SomeException
#endif

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
    => HasCallStackIfDebug
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
               else pure Nothing

{-# INLINE newRefWithTracker #-}
#ifndef NO_IGNORE_ASSERTS
newRefWithTracker :: PrimMonad m => obj -> m (Ref obj)
newRefWithTracker obj =
    pure $! Ref obj
#else
newRefWithTracker :: (PrimMonad m, HasCallStack) => obj -> m (Ref obj)
newRefWithTracker obj = do
    reftracker' <- newRefTracker callStack
    pure $! Ref obj reftracker'
#endif

data RefException =
       RefUseAfterRelease RefId
        CallStack -- ^ Allocation site
        CallStack -- ^ Release site
        CallStack -- ^ Use site
     | RefDoubleRelease RefId
        CallStack -- ^ Allocation site
        CallStack -- ^ First release site
        CallStack -- ^ Second release site
     | RefNeverReleased RefId
        CallStack -- ^ Allocation site

newtype RefId = RefId Int
  deriving stock (Show, Eq, Ord)

instance Show RefException where
  --Sigh. QuickCheck still uses 'show' rather than 'displayException'.
  showsPrec d x = showParen (d > appPrec) $ showString (displayException x)

instance Exception RefException where
  displayException (RefUseAfterRelease refid allocSite releaseSite useSite) =
      "Reference is used after release: " ++ show refid
    ++ "\nAllocation site: " ++ prettyCallStack allocSite
    ++ "\nRelease site: " ++ prettyCallStack releaseSite
    ++ "\nUse site: " ++ prettyCallStack useSite
  displayException (RefDoubleRelease refid allocSite releaseSite1 releaseSite2) =
      "Reference is released twice: " ++ show refid
    ++ "\nAllocation site: " ++ prettyCallStack allocSite
    ++ "\nFirst release site: " ++ prettyCallStack releaseSite1
    ++ "\nSecond release site: " ++ prettyCallStack releaseSite2
  displayException (RefNeverReleased refid allocSite) =
      "Reference is never released: " ++ show refid
   ++ "\nAllocation site: " ++ prettyCallStack allocSite

#ifndef NO_IGNORE_ASSERTS

{-# INLINE releaseRefTracker #-}
releaseRefTracker :: PrimMonad m => Ref a -> m ()
releaseRefTracker _ = pure ()

{-# INLINE assertNoForgottenRefs #-}
assertNoForgottenRefs :: PrimMonad m => m ()
assertNoForgottenRefs = pure ()

{-# INLINE assertNoUseAfterRelease #-}
assertNoUseAfterRelease :: PrimMonad m => Ref a -> m ()
assertNoUseAfterRelease _ = pure ()

{-# INLINE assertNoDoubleRelease #-}
assertNoDoubleRelease :: PrimMonad m => Ref a -> m ()
assertNoDoubleRelease _ = pure ()

#else

-- | A weak pointer to an outer IORef, containing an inner IORef with a maybe to
-- indicate if the ref has been explicitly released.
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
-- The inner IORef also tracks the call stack for the site where the reference
-- (tracker) is released. This call stack is used in exceptions for easier
-- debugging.
data RefTracker = RefTracker !RefId
                             !(Weak (IORef (IORef (Maybe CallStack))))
                             !(IORef (IORef (Maybe CallStack))) -- ^ Release site
                             !CallStack -- ^ Allocation site

{-# NOINLINE globalRefIdSupply #-}
globalRefIdSupply :: PrimVar RealWorld Int
globalRefIdSupply = unsafePerformIO $ newPrimVar 0

data Enabled a = Enabled !a | Disabled

{-# NOINLINE globalForgottenRef #-}
globalForgottenRef :: IORef (Enabled (Maybe (RefId, CallStack)))
globalForgottenRef = unsafePerformIO $ newIORef (Enabled Nothing)

-- | This version of 'unsafeIOToPrim' is strict in the result of the argument
-- action.
--
-- Without strictness it seems that some IO side effects are not happening at
-- the right time, like clearing the @globalForgottenRef@ in
-- @assertNoForgottenRefs@.
unsafeIOToPrimStrict :: PrimMonad m => IO a -> m a
unsafeIOToPrimStrict k = do
    !x <- unsafeIOToPrim k
    pure x

newRefTracker :: PrimMonad m => CallStack -> m RefTracker
newRefTracker allocSite = unsafeIOToPrimStrict $ do
    inner <- newIORef Nothing
    outer <- newIORef inner
    refid <- fetchAddInt globalRefIdSupply 1
    weak  <- mkWeakIORef outer $
               finaliserRefTracker inner (RefId refid) allocSite
    pure (RefTracker (RefId refid) weak outer allocSite)

releaseRefTracker :: (HasCallStack, PrimMonad m) => Ref a -> m ()
releaseRefTracker Ref { reftracker =  RefTracker _refid _weak outer _ } =
  unsafeIOToPrimStrict $ do
    inner <- readIORef outer
    let releaseSite = callStack
    writeIORef inner (Just releaseSite)

finaliserRefTracker :: IORef (Maybe CallStack) -> RefId -> CallStack -> IO ()
finaliserRefTracker inner refid allocSite = do
    released <- readIORef inner
    case released of
      Just _releaseSite -> pure ()
      Nothing -> do
        -- Uh oh! Forgot a reference without releasing!
        -- Add it to a global var which we can poll elsewhere.
        mref <- readIORef globalForgottenRef
        case mref of
          Disabled -> pure ()
          -- Just keep one, but keep the last allocated one.
          -- The reason for last is that when there are nested structures with
          -- refs then the last allocated is likely to be the outermost, which
          -- is the best place to start hunting for ref leaks. Otherwise one can
          -- go on a wild goose chase tracking down inner refs that were only
          -- forgotten due to an outer ref being forgotten.
          Enabled (Just (refid', _)) | refid < refid' -> pure ()
          Enabled _ -> writeIORef globalForgottenRef (Enabled (Just (refid, allocSite)))

assertNoForgottenRefs :: (PrimMonad m, MonadThrow m) => m ()
assertNoForgottenRefs = do
    mrefs <- unsafeIOToPrimStrict $ readIORef globalForgottenRef
    case mrefs of
      Disabled      -> pure ()
      Enabled Nothing -> pure ()
      Enabled (Just (refid, allocSite)) -> do
        -- Clear the var so we don't assert again.
        --
        -- Using the strict version is important here: if @m ~ IOSim s@, then
        -- using the non-strict version will lead to @RefNeverReleased@
        -- exceptions.
        unsafeIOToPrimStrict $ writeIORef globalForgottenRef (Enabled Nothing)
        throwIO (RefNeverReleased refid allocSite)


assertNoUseAfterRelease :: (PrimMonad m, MonadThrow m, HasCallStack) => Ref a -> m ()
assertNoUseAfterRelease Ref { reftracker = RefTracker refid _weak outer allocSite } = do
    released <- unsafeIOToPrimStrict (readIORef =<< readIORef outer)
    case released of
      Nothing -> pure ()
      Just releaseSite -> do
        -- The site where the reference is used after release
        let useSite = callStack
        throwIO (RefUseAfterRelease refid allocSite releaseSite useSite)
#if !(MIN_VERSION_base(4,20,0))
  where
    _unused = callStack
#endif

assertNoDoubleRelease :: (PrimMonad m, MonadThrow m, HasCallStack) => Ref a -> m ()
assertNoDoubleRelease Ref { reftracker = RefTracker refid _weak outer allocSite } = do
    released <- unsafeIOToPrimStrict (readIORef =<< readIORef outer)
    case released of
      Nothing -> pure ()
      Just releaseSite1 -> do
        -- The second release site
        let releaseSite2 = callStack
        throwIO (RefDoubleRelease refid allocSite releaseSite1 releaseSite2)
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
checkForgottenRefs :: forall m. (PrimMonad m, MonadThrow m) => m ()
checkForgottenRefs = do
#ifndef NO_IGNORE_ASSERTS
    pure ()
#else
    -- The hope is that by combining `performMajorGC` with `yield` that the
    -- former starts the finalizer threads for all dropped weak references and
    -- the latter suspends the current process and puts it at the end of the
    -- thread queue, such that when the current process resumes the finalizer
    -- threads for all dropped weak references have finished.
    -- Unfortunately, this relies on the implementation of the GHC scheduler,
    -- not on any Haskell specification, and is therefore both non-portable and
    -- presumably rather brittle. Therefore, for good measure, we do it twice.
    unsafeIOToPrimStrict $ do
      performMajorGCWithBlockingIfAvailable
      yield
      performMajorGCWithBlockingIfAvailable
      yield
    assertNoForgottenRefs
#endif
  where
    _unused = throwIO @m @SomeException

-- | Ignore and reset the state of forgotten reference tracking. This ensures
-- that any stale forgotten references are not reported later.
--
-- This is especillay important in QC tests with shrinking which otherwise
-- leads to confusion.
ignoreForgottenRefs :: (PrimMonad m, MonadCatch m) => m ()
ignoreForgottenRefs = void $ try @_ @SomeException $ checkForgottenRefs

#ifdef NO_IGNORE_ASSERTS
performMajorGCWithBlockingIfAvailable :: IO ()
performMajorGCWithBlockingIfAvailable = do
#if MIN_VERSION_base(4,20,0)
    performBlockingMajorGC
#else
    performMajorGC
#endif
#endif

-- | Enable forgotten reference checks.
enableForgottenRefChecks :: IO ()

-- | Disable forgotten reference checks. This will error if there are already
-- forgotten references while we are trying to disable the checks.
disableForgottenRefChecks :: IO ()

#ifdef NO_IGNORE_ASSERTS
enableForgottenRefChecks =
    modifyIORef globalForgottenRef $ \case
      Disabled -> Enabled Nothing
      Enabled _  -> error "enableForgottenRefChecks: already enabled"

disableForgottenRefChecks =
    modifyIORef globalForgottenRef $ \case
      Disabled -> error "disableForgottenRefChecks: already disabled"
      Enabled Nothing -> Disabled
      Enabled _  -> error "disableForgottenRefChecks: can not disable when there are forgotten references"
#else
enableForgottenRefChecks = pure ()
disableForgottenRefChecks = pure ()
#endif
