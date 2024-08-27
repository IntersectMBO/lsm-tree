-- TODO: we are starting to use the TempRegistry for more than just resource
-- allocation/release, we are more generally using it for /actions that can be
-- rolled back/ and /actions that are delayed/. Maybe we should reframe the use
-- cases for the 'TempRegistry', and do some renaming:
-- * Rename @'allocateTemp'*@ to @'withRollback'*@
-- * Rename @'freeTemp'@ to @'delayUntilEnd'@
module Control.TempRegistry (
    TempRegistry
  , withTempRegistry
  , allocateTemp
  , allocateMaybeTemp
  , allocateEitherTemp
  , freeTemp
  , modifyWithTempRegistry
  , modifyWithTempRegistry_
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad.Class.MonadThrow
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Void

-- | A temporary registry for resources that are bound to end up in some final
-- state, after which they /should/ be guaranteed to be released correctly.
--
-- It is the responsibility of the user to guarantee that this final state is
-- released correctly in the presence of async exceptions.
--
-- NOTE: this is based on [the @ResourceRegistry@ module from @ouroboros-consensus@](https://github.com/IntersectMBO/ouroboros-consensus/blob/main/ouroboros-consensus/src/ouroboros-consensus/Ouroboros/Consensus/Util/ResourceRegistry.hs).
--
-- There are some differences between @WithTempRegistry@ from
-- @ouroboros-consensus@ and our 'TempRegistry'. For one, 'TempRegistry' allows
-- for the temporary /freeing/ of resources, which @WithTempRegistry@ does not.
-- However, @WithTempRegistry@ can check whether newly allocated resources
-- actually end up in the final state.
--
-- TODO: make 'TempRegistry' more sophisticated. Ideas:
--
-- * Use a similar approach (like in 'WithTempRegistry@) for checking that
--   temporarily allocated resources end up in the final state, and that
--   temporarily freed resources are removed from the final state.
--
-- * Statically disallow using a resource after @freeTemp@, for example through
--   data abstraction.
--
-- TODO: could https://hackage.haskell.org/package/resourcet be a suitable
-- abstraction instead of 'TempRegistry'?
newtype TempRegistry m = TempRegistry {
    tempRegistryState :: StrictMVar m (TempRegistryState m)
  }

data TempRegistryState m = TempRegistryState {
    tempAllocated :: !(Map ResourceId (Resource m))
  , tempFreed     :: !(Map ResourceId (Resource m))
  , nextId        :: !ResourceId
  }

newtype ResourceId = ResourceId Int
  deriving stock (Eq, Ord)
  deriving newtype (Num)

newtype Resource m = Resource {
    resourceRelease :: (m ())
  }

{-# SPECIALISE withTempRegistry :: (TempRegistry IO -> IO a) -> IO a #-}
withTempRegistry ::
     (MonadMVar m, MonadCatch m)
  => (TempRegistry m -> m a)
  -> m a
withTempRegistry k = fst <$> generalBracket acquire release k
  where
    acquire = unsafeNewTempRegistry
    release reg ec = unsafeReleaseTempRegistry reg ec

{-# SPECIALISE unsafeNewTempRegistry :: IO (TempRegistry IO) #-}
-- | This is considered unsafe, because one should properly 'bracket' this
-- function. Example:
--
-- @
--  generalBracket unsafeNewTempRegistry unsafeReleaseTempRegistry
-- @
unsafeNewTempRegistry :: MonadMVar m => m (TempRegistry m)
unsafeNewTempRegistry = TempRegistry <$> newMVar (TempRegistryState Map.empty Map.empty (ResourceId 0))

{-# SPECIALISE unsafeReleaseTempRegistry :: TempRegistry IO -> ExitCase a -> IO () #-}
-- | See 'unsafeNewTempRegistry'.
unsafeReleaseTempRegistry :: MonadMVar m => TempRegistry m -> ExitCase a -> m ()
unsafeReleaseTempRegistry reg ec = case ec of
    ExitCaseSuccess{} -> mapM_ resourceRelease . tempFreed     =<< takeMVar (tempRegistryState reg)
    _                 -> mapM_ resourceRelease . tempAllocated =<< takeMVar (tempRegistryState reg)


{-# SPECIALISE allocateTemp :: TempRegistry IO -> IO a -> (a -> IO ()) -> IO a #-}
-- | Temporarily allocate a resource.
--
-- This runs the @acquire@ function with async exceptions masked to ensure that
-- acquired resources are always put into the registry. However, note that in
-- general the following two expressions are not equivalent:
--
-- @
--   allocateTemp reg acquire free
--   acquire >>= \x -> allocateTemp reg free (pure x)
-- @
--
-- Assuming that @acquire@ is not already exception safe, it is /not/
-- exception-safe to pass the result of @acquire@ to @allocateTemp@: an async
-- exception could be thrown in between @acquire@ and @allocateTemp@, which
-- leaks resources.
allocateTemp :: (MonadMask m, MonadMVar m) =>
     TempRegistry m
  -> m a
  -> (a -> m ())
  -> m a
allocateTemp reg acquire free = mustBeRight <$> allocateEitherTemp reg (fmap Right acquire) free
  where
    mustBeRight :: Either Void a -> a
    mustBeRight (Left  v) = absurd v
    mustBeRight (Right a) = a

{-# SPECIALISE allocateMaybeTemp :: TempRegistry IO -> IO (Maybe a) -> (a -> IO ()) -> IO (Maybe a) #-}
-- | Like 'allocateTemp', but for resources that might fail to be acquired.
allocateMaybeTemp ::
     (MonadMask m, MonadMVar m)
  => TempRegistry m
  -> m (Maybe a)
  -> (a -> m ())
  -> m (Maybe a)
allocateMaybeTemp reg acquire free = fromEither <$> allocateEitherTemp reg (toEither <$> acquire) free
  where
    toEither :: Maybe a -> Either () a
    toEither Nothing  = Left ()
    toEither (Just x) = Right x

    fromEither :: Either () a -> Maybe a
    fromEither (Left ()) = Nothing
    fromEither (Right x) = Just x

{-# SPECIALISE allocateEitherTemp :: TempRegistry IO -> IO (Either e a) -> (a -> IO ()) -> IO (Either e a) #-}
-- | Like 'allocateTemp', but for resources that might fail to be acquired.
allocateEitherTemp ::
     (MonadMask m, MonadMVar m)
  => TempRegistry m
  -> m (Either e a)
  -> (a -> m ())
  -> m (Either e a)
allocateEitherTemp reg acquire free =
    mask_ $ do
      eith <- acquire
      case eith of
        Left e -> pure $ Left e
        Right x -> do
          modifyMVar_ (tempRegistryState reg) $ \st -> do
            let rid = nextId st
                rid' = rid + 1
            pure TempRegistryState {
                tempAllocated = Map.insert rid (Resource (free x)) (tempAllocated st)
              , tempFreed = tempFreed st
              , nextId = rid'
              }
          pure $ Right x

{-# SPECIALISE freeTemp :: TempRegistry IO -> IO () -> IO () #-}
-- | Temporarily free a resource.
--
-- NOTE: the resource is not actually released until the 'TempRegistry' is
-- released. This makes rolling back simple, but it means that /use after free/
-- within the scope of a 'TempRegistry' will work just as if there had been no
-- free at all. As such, though it is not recommended to rely on this
-- peculiarity, the following is safe:
--
-- @
--  allocateTemp reg free acquire >>= \x ->
--    freeTemp reg (free x) >>= \_ -> {- do something with x -}
-- @
freeTemp :: MonadMVar m => TempRegistry m -> m () -> m ()
freeTemp reg free = modifyMVarMasked_ (tempRegistryState reg) $ \st -> do
    let rid = nextId st
        rid' = rid + 1
    pure TempRegistryState {
        tempAllocated = tempAllocated st
      , tempFreed = Map.insert rid (Resource free) (tempFreed st)
      , nextId = rid'
      }

{-# SPECIALISE modifyWithTempRegistry :: IO st -> (st -> IO ()) -> (TempRegistry IO -> st -> IO (st, a)) -> IO a #-}
-- | Exception-safe modification of state with a temporary registry.
--
-- [Example:] When we modify a table's content (stored in a mutable variable),
-- we might add new runs to the levels, or remove old runs from the level. If an
-- exception is thrown before putting the updated table contents into the
-- variable, then any resources that were acquired or released in the meantime
-- should be rolled back. The 'TempRegistry' can be used to "temporarily"
-- allocate or free resources, the effects of which are rolled back in case of
-- an exception, or put into the final state when no exceptions were raised.
modifyWithTempRegistry ::
     (MonadMVar m, MonadCatch m)
  => m st -- ^ Get the state
  -> (st -> m ()) -- ^ Store a state
  -> (TempRegistry m -> st -> m (st, a)) -- ^ Modify the state
  -> m a
modifyWithTempRegistry getSt putSt action =
    snd . fst <$> generalBracket acquire release (uncurry action)
  where
    acquire = (,) <$> unsafeNewTempRegistry <*> getSt
    release (reg, oldSt) ec = do
        case ec of
          ExitCaseSuccess (newSt, _) -> putSt newSt
          ExitCaseException _        -> putSt oldSt
          ExitCaseAbort              -> putSt oldSt
        unsafeReleaseTempRegistry reg ec

{-# SPECIALISE modifyWithTempRegistry_ :: IO st -> (st -> IO ()) -> (TempRegistry IO -> st -> IO st) -> IO () #-}
-- | Like 'modifyWithTempRegistry', but without a return value.
modifyWithTempRegistry_ ::
     (MonadMVar m, MonadCatch m)
  => m st -- ^ Get the state
  -> (st -> m ()) -- ^ Store a state
  -> (TempRegistry m -> st -> m st)
  -> m ()
modifyWithTempRegistry_ getSt putSt action =
    modifyWithTempRegistry getSt putSt (\reg content -> (,()) <$> action reg content)
