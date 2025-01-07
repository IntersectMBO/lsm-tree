{-# LANGUAGE CPP #-}

-- | Registry of monadic actions supporting rollback actions and delayed actions
-- in the presence of (a-)synchronous exceptions.
--
-- This module is heavily inspired by:
--
-- * [resource-registry](https://github.com/IntersectMBO/io-classes-extra/blob/main/resource-registry/src/Control/ResourceRegistry.hs)
--
-- * [resourcet](https://hackage.haskell.org/package/resourcet)
module Control.ActionRegistry (
    -- * Modify mutable state #modifyMutableState#
    -- $modify-mutable-state
    modifyWithActionRegistry
  , modifyWithActionRegistry_
    -- * Action registry  #actionRegistry#
    -- $action-registry
  , ActionRegistry
  , ActionError
    -- * Runners
  , withActionRegistry
  , unsafeNewActionRegistry
  , unsafeFinaliseActionRegistry
  , CommitActionRegistryError
  , AbortActionRegistryError
  , AbortActionRegistryReason
    -- * Registering actions #registeringActions#
    -- $registering-actions
  , withRollback
  , withRollback_
  , withRollbackMaybe
  , withRollbackEither
  , withRollbackFun
  , delayedCommit
  ) where

import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Kind
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Primitive.MutVar

#ifdef NO_IGNORE_ASSERTS
import           GHC.Stack
#endif

-- TODO: add tests using fs-sim/io-sim to make sure exception safety is
-- guaranteed.

-- TODO: add assertions that allocated resources end up in the final state, and
-- that temporarily freed resources are removed from the final state.

-- TODO: could we statically disallow using a resource after it is freed using
-- @delayedCommit@, for example through data abstraction?

-- Call stack instrumentation is enabled if assertions are enabled.
#ifdef NO_IGNORE_ASSERTS
#define HasCallStackIfDebug HasCallStack
#else
#define HasCallStackIfDebug ()
#endif

{-------------------------------------------------------------------------------
  Modify mutable state
-------------------------------------------------------------------------------}

{- $modify-mutable-state

  When a piece of mutable state holding system resources is updated, then it is
  important to guarantee in the presence of (a-)synchronous exceptions that:

  1. Allocated resources end up in the state
  2. Freed resources are removed from the state

  Consider the example program below. We have some mutable @State@ that holds a
  file handle/descriptor. We want to mutate this state by closing the current
  handle, and replacing it by a newly opened handle. Using the tools at our
  disposal in "Control.ActionRegistry", we guarantee (1) and (2).

  @
    type State = MVar Handle

    example :: State -> IO ()
    example st =
      'modifyWithActionRegistry_'
        (takeMVar st)
        (putMVar st)
        $ \\reg h -> do
          h' <- 'withRollback' reg
                  (openFile  "file.txt" ReadWriteMode)
                  hClose
          'delayedCommit' reg (hClose h)
          pure h'
  @

  What is also nice about this examples is that it is atomic: other threads will
  not be able to see the updated @State@ until 'modifyWithActionRegistry_' has
  exited and the necessary side effects have been performed. Of course, another
  thread *could* observe that the @file.txt@ was created before
  'modifyWithActionRegistry_' has exited, but the assumption is that the threads
  in our program are cooperative. It is up to the user to ensure that actions
  that are performed as part of the state update do not conflict with other
  actions.
-}

{-# SPECIALISE modifyWithActionRegistry ::
     IO st
  -> (st -> IO ())
  -> (ActionRegistry IO -> st -> IO (st, a))
  -> IO a
  #-}
-- | Modify a piece piece of state given a fresh action registry.
modifyWithActionRegistry ::
     (PrimMonad m, MonadCatch m)
  => m st -- ^ Get the state
  -> (st -> m ()) -- ^ Store a state
  -> (ActionRegistry m -> st -> m (st, a)) -- ^ Modify the state
  -> m a
modifyWithActionRegistry getSt putSt action =
    snd . fst <$> generalBracket acquire release (uncurry action)
  where
    acquire = (,) <$> unsafeNewActionRegistry <*> getSt
    release (reg, oldSt) ec = do
        case ec of
          ExitCaseSuccess (newSt, _) -> putSt newSt
          ExitCaseException _        -> putSt oldSt
          ExitCaseAbort              -> putSt oldSt
        unsafeFinaliseActionRegistry reg ec

{-# SPECIALISE modifyWithActionRegistry_ ::
     IO st
  -> (st -> IO ())
  -> (ActionRegistry IO -> st -> IO st)
  -> IO ()
  #-}
-- | Like 'modifyWithActionRegistry', but without a return value.
modifyWithActionRegistry_ ::
     (PrimMonad m, MonadCatch m)
  => m st -- ^ Get the state
  -> (st -> m ()) -- ^ Store a state
  -> (ActionRegistry m -> st -> m st)
  -> m ()
modifyWithActionRegistry_ getSt putSt action =
    modifyWithActionRegistry getSt putSt (\reg content -> (,()) <$> action reg content)

{-------------------------------------------------------------------------------
  Action registry
-------------------------------------------------------------------------------}

{- $action-registry

  An 'ActionRegistry' is a registry of monadic actions to support working with
  resources and mutable state in the presence of (a)synchronous exceptions. It
  works analogously to database transactions: within the \"transaction\" scope
  we can perform actions (such as resource allocations and state changes) and we
  can register delayed (commit) and rollback actions. The delayed actions are
  all executed at the end if the transaction scope is exited successfully, but
  if an exception is thrown (sync or async) then the rollback actions are
  executed instead, and the exception is propagated.

  * Rollback actions are executed in the reverse order in which they were
  registered, which is the natural nesting order when considered as bracketing.

  * Delayed actions are executed in the same order in which they are registered.
-}

-- | Registry of monadic actions supporting rollback actions and delayed actions
-- in the presence of (a-)synchronous exceptions.
--
-- See [Action registry](#g:actionRegistry) for more information.
--
-- An action registry should be short-lived, and it is not thread-safe.
data ActionRegistry m = ActionRegistry {
      -- | Registered rollback actions. Use 'consAction' when modifying this
      -- variable.
      --
      -- INVARIANT: actions are stored in LIFO order.
      --
      -- INVARIANT: the contents of this variable are in NF.
      registryRollback :: !(MutVar (PrimState m) [Action m])

      -- | Registered, delayed actions. Use 'consAction' when modifying this
      -- variable.
      --
      -- INVARIANT: actions are stored in LIFO order.
      --
      -- INVARIANT: the contents of this variable are in NF.
    , registryDelay    :: !(MutVar (PrimState m) [Action m])
    }

{-# SPECIALISE consAction :: Action IO -> MutVar RealWorld [Action IO] -> IO () #-}
-- | Cons an action onto the contents of an actions variable.
--
-- Both the action and the resulting variable contents are evaluated to WHNF. If
-- the contents of the variable were already in NF, then the result will also be
-- in NF.
consAction :: PrimMonad m => Action m -> MutVar (PrimState m) [Action m] -> m ()
consAction !a var = modifyMutVar' var $ \as -> a `consStrict` as
  where consStrict !x xs = x : xs

-- | Monadic computations that (may) produce side effects
type Action :: (Type -> Type) -> Type

-- | An action failed with an exception
type ActionError :: Type

mkAction :: HasCallStackIfDebug => m () -> Action m
mkActionError :: SomeException -> Action m -> ActionError

#ifdef NO_IGNORE_ASSERTS
data Action m = Action {
    runAction       :: !(m ())
  , actionCallStack :: !CallStack
  }

data ActionError = ActionError SomeException CallStack
  deriving stock Show
  deriving anyclass Exception

mkAction a = Action a callStack

mkActionError e a = ActionError e (actionCallStack a)
#else
newtype Action m = Action {
    runAction :: m ()
  }

newtype ActionError = ActionError SomeException
  deriving stock Show
  deriving anyclass Exception

mkAction a = Action a

mkActionError e _ = ActionError e
#endif

{-------------------------------------------------------------------------------
  Runners
-------------------------------------------------------------------------------}

{-# SPECIALISE withActionRegistry :: (ActionRegistry IO -> IO a) -> IO a #-}
-- | Run code with a new 'ActionRegistry'.
--
-- (A-)synchronous exception safety is only guaranteed within the scope of
-- 'withActionRegistry' (and only for properly registered actions). As soon as
-- we leave this scope, all bets are off. If, for example, a newly allocated
-- file handle escapes the scope, then that file handle can be leaked. If such
-- is the case, then it is highly likely that you should be using
-- 'modifyWithActionRegistry' instead.
--
-- If the code was interrupted due to an exception for example, then the
-- registry is aborted, which performs registered rollback actions. If the code
-- succesfully terminated, then the registry is committed, in which case
-- registered, delayed actions will be performed.
--
-- Registered actions are run in LIFO order, whether they be rollback actions or
-- delayed actions.
withActionRegistry ::
     (PrimMonad m, MonadCatch m)
  => (ActionRegistry m -> m a)
  -> m a
withActionRegistry k = fst <$> generalBracket acquire release k
  where
    acquire = unsafeNewActionRegistry
    release reg ec = unsafeFinaliseActionRegistry reg ec

{-# SPECIALISE unsafeNewActionRegistry :: IO (ActionRegistry IO) #-}
-- | This function is considered unsafe. Preferably, use 'withActionRegistry'
-- instead.
--
-- If this function is used directly, use 'generalBracket' to pair
-- 'unsafeNewActionRegistry' with an 'unsafeFinaliseActionRegistry'.
unsafeNewActionRegistry :: PrimMonad m => m (ActionRegistry m)
unsafeNewActionRegistry = do
    registryRollback <- newMutVar $! []
    registryDelay <- newMutVar $! []
    pure $! ActionRegistry {..}

{-# SPECIALISE unsafeFinaliseActionRegistry :: ActionRegistry IO -> ExitCase a -> IO () #-}
-- | This function is considered unsafe. See 'unsafeNewActionRegistry'.
--
-- This commits the action registry on 'ExitCaseSuccess', and otherwise aborts
-- the action registry.
unsafeFinaliseActionRegistry ::
     (PrimMonad m, MonadCatch m)
  => ActionRegistry m
  -> ExitCase a
  -> m ()
unsafeFinaliseActionRegistry reg ec = case ec of
    ExitCaseSuccess{}   -> unsafeCommitActionRegistry reg
    ExitCaseException e -> unsafeAbortActionRegistry reg (ReasonExitCaseException e)
    ExitCaseAbort       -> unsafeAbortActionRegistry reg ReasonExitCaseAbort

{-# SPECIALISE unsafeCommitActionRegistry :: ActionRegistry IO -> IO () #-}
-- | Perform delayed actions, but not rollback actions.
unsafeCommitActionRegistry :: (PrimMonad m, MonadCatch m) => ActionRegistry m -> m ()
unsafeCommitActionRegistry reg = do
    as <- readMutVar (registryDelay reg)
    -- Run actions in FIFO order
    r <- runActions (reverse as)
    case NE.nonEmpty r of
      Nothing         -> pure ()
      Just exceptions -> throwIO (CommitActionRegistryError exceptions)

data CommitActionRegistryError = CommitActionRegistryError (NonEmpty ActionError)
  deriving stock Show
  deriving anyclass Exception

{-# SPECIALISE unsafeAbortActionRegistry ::
     ActionRegistry IO
  -> AbortActionRegistryReason
  -> IO () #-}
-- | Perform rollback actions, but not delayed actions
unsafeAbortActionRegistry ::
     (PrimMonad m, MonadCatch m)
  => ActionRegistry m
  -> AbortActionRegistryReason
  -> m ()
unsafeAbortActionRegistry reg reason = do
    as <- readMutVar (registryRollback reg)
    -- Run actions in LIFO order
    r <- runActions as
    case NE.nonEmpty r of
      Nothing         -> pure ()
      Just exceptions -> throwIO (AbortActionRegistryError reason exceptions)

-- | Reasons why an action registry was aborted.
data AbortActionRegistryReason =
    -- | The action registry was aborted because the code that it scoped over
    -- threw an exception (see 'ExitCaseException').
    ReasonExitCaseException SomeException
    -- | The action registry was aborted because the code that it scoped over
    -- aborted (see 'ExitCaseAbort').
  | ReasonExitCaseAbort
  deriving stock Show

data AbortActionRegistryError =
    AbortActionRegistryError AbortActionRegistryReason (NonEmpty ActionError)
  deriving stock Show
  deriving anyclass Exception

{-# SPECIALISE runActions :: [Action IO] -> IO [ActionError] #-}
-- | Run all actions even if previous actions threw exceptions.
runActions :: MonadCatch m => [Action m] -> m [ActionError]
runActions = go []
  where
    go es [] = pure (reverse es)
    go es (a:as) = do
      eith <- try @_ @SomeException (runAction a)
      case eith of
        Left e  -> go (mkActionError e a : es) as
        Right _ -> go es as

{-------------------------------------------------------------------------------
  Registering actions
-------------------------------------------------------------------------------}

{- $registering-actions

  /Actions/ are monadic computations that (may) produce side effects. Such side
  effects can include opening or closing a file handle, but also modifying a
  mutable variable.

  We make a distinction between three types of actions:

  * An /immediate action/ is performed immediately, as the name suggests.

  * A /rollback action/ is an action that is registered in an action registry,
    and it is performed precisely when the corresponding action registry is
    aborted. See 'withRollback' for examples.

  * A /delayed action/ is an action that is registered in an action registry,
    and it is performed precisely when the corresponding action registry is
    committed. See 'delayedCommit' for examples.

  Immediate actions are run with asynchronous exceptions masked to guarantee
  that the rollback action is registered after the immediate action has returned
  successfully. This means that all the usual masking caveats apply for the
  immediate acion.

  Rollback actions and delayed actions are performed /precisely/ when aborting
  or committing an action registry respectively (see [Action
  registry](#g:actionRegistry)). To achieve this, finalisation of the action
  registry happens in the same masked state as runnning the registered actions.
  This means all the usual masking caveats apply for the registered actions.
-}

{-# SPECIALISE withRollback ::
     HasCallStackIfDebug
  => ActionRegistry IO
  -> IO a
  -> (a -> IO ())
  -> IO a #-}
-- | Perform an immediate action and register a rollback action.
--
-- See [Registering actions](#g:registeringActions) for more information about
-- the different types of actions.
--
-- A typical use case for 'withRollback' is to allocate a resource as the
-- immediate action, and to release said resource as the rollback action. In
-- that sense, 'withRollback' is similar to 'bracketOnError', but 'withRollback'
-- offers stronger guarantees.
--
-- Note that the following two expressions are /not/ equivalent. The former is
-- correct in the presence of asynchronous exceptions, while the latter is not!
--
-- @
--    withRollback reg acquire free
-- =/=
--    acquire >>= \x -> withRollback reg free (pure x)
-- @
withRollback ::
     (PrimMonad m, MonadMask m)
  => HasCallStackIfDebug
  => ActionRegistry m
  -> m a
  -> (a -> m ())
  -> m a
withRollback reg acquire release =
    withRollbackFun reg Just acquire release

{-# SPECIALISE withRollback_ ::
     HasCallStackIfDebug
  => ActionRegistry IO
  -> IO a
  -> IO ()
  -> IO a #-}
-- | Like 'withRollback', but the rollback action does not get access to the
-- result of the immediate action.
--
withRollback_ ::
     (PrimMonad m, MonadMask m)
  => HasCallStackIfDebug
  => ActionRegistry m
  -> m a
  -> m ()
  -> m a
withRollback_ reg acquire release =
    withRollbackFun reg Just acquire (\_ -> release)

{-# SPECIALISE withRollbackMaybe ::
     HasCallStackIfDebug
  => ActionRegistry IO
  -> IO (Maybe a)
  -> (a -> IO ())
  -> IO (Maybe a)
  #-}
-- | Like 'withRollback', but the immediate action may fail with a 'Nothing'.
-- The rollback action will only be registered if 'Just'.
--
withRollbackMaybe ::
     (PrimMonad m, MonadMask m)
  => HasCallStackIfDebug
  => ActionRegistry m
  -> m (Maybe a)
  -> (a -> m ())
  -> m (Maybe a)
withRollbackMaybe reg acquire release =
    withRollbackFun reg id acquire release

{-# SPECIALISE withRollbackEither ::
     HasCallStackIfDebug
  => ActionRegistry IO
  -> IO (Either e a)
  -> (a -> IO ())
  -> IO (Either e a)
  #-}
-- | Like 'withRollback', but the immediate action may fail with a 'Left'. The
-- rollback action will only be registered if 'Right'.
--
withRollbackEither ::
     (PrimMonad m, MonadMask m)
  => HasCallStackIfDebug
  => ActionRegistry m
  -> m (Either e a)
  -> (a -> m ())
  -> m (Either e a)
withRollbackEither reg acquire release =
    withRollbackFun reg fromEither acquire release
  where
    fromEither :: Either e a -> Maybe a
    fromEither (Left _)  = Nothing
    fromEither (Right x) = Just x

{-# SPECIALISE withRollbackFun ::
     HasCallStackIfDebug
  => ActionRegistry IO
  -> (a -> Maybe b)
  -> IO a
  -> (b -> IO ())
  -> IO a
  #-}
-- | Like 'withRollback', but the immediate action may fail in some general
-- way. The rollback function will only be registered if the @(a -> Maybe b)@
-- function returned 'Just'.
--
-- 'withRollbackFun' is the most general form in the 'withRollback*' family of
-- functions. All 'withRollback*' functions can be defined in terms of
-- 'withRollBackFun'.
--
withRollbackFun ::
     (PrimMonad m, MonadMask m)
  => HasCallStackIfDebug
  => ActionRegistry m
  -> (a -> Maybe b)
  -> m a
  -> (b -> m ())
  -> m a
withRollbackFun reg extract acquire release = do
    mask_ $ do
      x <- acquire
      case extract x of
        Nothing -> pure x
        Just y -> do
          consAction (mkAction (release y)) (registryRollback reg)
          pure x

{-# SPECIALISE delayedCommit ::
     HasCallStackIfDebug
  => ActionRegistry IO
  -> IO ()
  -> IO () #-}
-- | Register a delayed action.
--
-- See [Registering actions](#g:registeringActions) for more information about
-- the different types of actions.
--
-- A typical use case for 'delayedCommit' is to delay destructive actions until
-- they are safe to be performed. For example, a destructive action such as
-- removing a file can often not be rolled back without jumping through
-- additional hoops.
--
-- If you can think of a sensible rollback action for the action you want to
-- delay then 'withRollback' might be a more suitable fit than 'delayedCommit'.
-- For example, incrementing a thread-safe mutable variable can easily be rolled
-- back by decrementing the same variable again.
--
delayedCommit ::
     PrimMonad m
  => HasCallStackIfDebug
  => ActionRegistry m
  -> m ()
  -> m ()
delayedCommit reg action = consAction (mkAction action) (registryDelay reg)
