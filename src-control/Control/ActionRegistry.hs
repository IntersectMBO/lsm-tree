-- | Registry of monadic actions supporting rollback actions and delayed actions
-- in the presence of (a-)synchronous exceptions.
--
-- This module is heavily inspired by:
--
-- * [resource-registry](https://github.com/IntersectMBO/io-classes-extra/blob/main/resource-registry/src/Control/ResourceRegistry.hs)
--
-- * [resourcet](https://hackage.haskell.org/package/resourcet)
module Control.ActionRegistry (
    -- * Modify mutable state
    -- $modify-mutable-state #modify-mutable-state#
    modifyWithActionRegistry
  , modifyWithActionRegistry_
    -- * Action registry
  , ActionRegistry
    -- * Runners
  , withActionRegistry
  , unsafeNewActionRegistry
  , unsafeFinaliseActionRegistry
    -- * Registering actions
    -- $registering-actions #registering-actions#
  , withRollback
  , withRollback_
  , withRollbackMaybe
  , withRollbackEither
  , withRollbackFun
  , delay
  ) where

import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Either (lefts)
import           Data.List.NonEmpty as NE
import           Data.Primitive.MutVar

-- TODO: replace TempRegistry by ActionRegistry

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
          'delay' reg (hClose h)
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

-- TODO: add assertions that allocated resources end up in the final state, and
-- that temporarily freed resources are removed from the final state.

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

-- | Registry of monadic actions supporting rollback actions and delayed actions
-- in the presence of (a-)synchronous exceptions.
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

-- | Cons an action onto the contents of an actions variable.
--
-- Both the action and the resulting variable contents are evaluated to WHNF. If
-- the contents of the variable were already in NF, then the result will also be
-- in NF.
consAction :: PrimMonad m => Action m -> MutVar (PrimState m) [Action m] -> m ()
consAction !a var = modifyMutVar' var $ \as -> a `consStrict` as
  where consStrict !x xs = x : xs

-- | Monadic computations that (may) produce side effects
newtype Action m = Action {
    runAction :: m ()
  }

{-------------------------------------------------------------------------------
  Runners
-------------------------------------------------------------------------------}

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

-- | This function is considered unsafe. Preferably, use 'withActionRegistry
-- instead'.
--
-- If this function is used directly, use 'generalBracket' to pair
-- 'unsafeNewActionRegistry' with an 'unsafeFinaliseActionRegistry'.
unsafeNewActionRegistry :: PrimMonad m => m (ActionRegistry m)
unsafeNewActionRegistry = do
    registryRollback <- newMutVar $! []
    registryDelay <- newMutVar $! []
    pure $! ActionRegistry {..}

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
    ExitCaseSuccess{} -> unsafeCommitActionRegistry reg
    _                 -> unsafeAbortActionRegistry reg

-- | Perform delayed actions, but not rollback actions.
unsafeCommitActionRegistry :: (PrimMonad m, MonadCatch m) => ActionRegistry m -> m ()
unsafeCommitActionRegistry reg = do
    as <- readMutVar (registryDelay reg)
    -- Run actions in LIFO order
    r <- runActions as
    case NE.nonEmpty r of
      Nothing         -> pure ()
      Just exceptions -> throwIO (CommitActionRegistryError exceptions)

data CommitActionRegistryError = CommitActionRegistryError (NonEmpty SomeException)
  deriving stock Show
  deriving anyclass Exception

-- | Perform rollback actions, but not delayed actions
unsafeAbortActionRegistry :: (PrimMonad m, MonadCatch m) => ActionRegistry m -> m ()
unsafeAbortActionRegistry reg = do
    as <- readMutVar (registryRollback reg)
    -- Run actions in LIFO order
    r <- runActions as
    case NE.nonEmpty r of
      Nothing         -> pure ()
      Just exceptions -> throwIO (AbortActionRegistryError exceptions)

data AbortActionRegistryError = AbortActionRegistryError (NonEmpty SomeException)
  deriving stock Show
  deriving anyclass Exception

-- Run all actions even if previous actions threw exceptions.
runActions :: MonadCatch m => [Action m] -> m [SomeException]
runActions as = do
    results <- mapM (\a -> try @_ @SomeException (runAction a)) as
    pure (lefts results)


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
    committed. See 'delay' for examples.

  Immediate actions are run with asynchronous exceptions masked to guarantee
  that the rollback action is registered after the immediate action has returned
  successfully. This means that all the usual masking caveats apply for the
  immediate acion.

  Rollback actions and delayed actions are performed /precisely/ when aborting
  or committing an action registry respectively. To achieve this, finalisation
  of the action registry happens in the same masked state as runnning the
  registered actions. This means all the usual masking caveats apply for the
  registered actions.
-}

-- | Perform an immediate action and register a rollback action.
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
  => ActionRegistry m
  -> m a
  -> (a -> m ())
  -> m a
withRollback reg acquire release =
    withRollbackFun reg Just acquire release

-- | Like 'withRollback', but the rollback action does not get access to the
-- result of the immediate action.
--
withRollback_ ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> m a
  -> m ()
  -> m a
withRollback_ reg acquire release =
    withRollbackFun reg Just acquire (\_ -> release)

-- | Like 'withRollback', but the immediate action may fail with a 'Nothing'.
-- The rollback action will only be registered if 'Just'.
--
withRollbackMaybe ::
     (PrimMonad m, MonadMask m)
  => ActionRegistry m
  -> m (Maybe a)
  -> (a -> m ())
  -> m (Maybe a)
withRollbackMaybe reg acquire release =
    withRollbackFun reg id acquire release

-- | Like 'withRollback', but the immediate action may fail with a 'Left'. The
-- rollback action will only be registered if 'Right'.
--
withRollbackEither ::
     (PrimMonad m, MonadMask m)
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
          consAction (Action (release y)) (registryRollback reg)
          pure x

-- | Register a delayed action.
--
-- A typical use case for 'delay' is to delay destructive actions until they are
-- safe to be performed. For example, a destructive action such as removing a
-- file can often not be rolled back without jumping through additional hoops.
--
-- If you can think of a sensible rollback action for the action you want to
-- delay then 'withRollback' might be a more suitable fit than 'delay'. For
-- example, incrementing a thread-safe mutable variable can easily be rolled
-- back by decrementing the same variable again.
--
delay :: PrimMonad m => ActionRegistry m -> m () -> m ()
delay reg action = consAction (Action action) (registryDelay reg)

-- TODO: could we statically disallow using a resource after it is freed using
-- @delay@, for example through data abstraction?
