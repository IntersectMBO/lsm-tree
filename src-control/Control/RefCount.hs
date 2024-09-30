{-# LANGUAGE MagicHash #-}

{- HLINT ignore "Evaluate" -}

module Control.RefCount (
    RefCounter (..)
  , RefCount (..)
  , unsafeMkRefCounterN
  , mkRefCounterN
  , mkRefCounter1
  , addReference
  , removeReference
  , upgradeWeakReference
  , readRefCount
  ) where

import           Control.DeepSeq
import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Maybe
import           Data.Primitive.PrimVar
import           GHC.Stack

-- | A reference counter with an optional finaliser action. Once the reference
-- count reaches @0@, the finaliser will be run.
data RefCounter m = RefCounter {
    countVar  :: !(PrimVar (PrimState m) Int)
  , finaliser :: !(Maybe (m ()))
  }

instance Show (RefCounter m) where
  show _ = "<RefCounter>"

-- | NOTE: Only strict in the variable and not the referenced value.
instance NFData (RefCounter m) where
  rnf RefCounter{countVar, finaliser} =
      rwhnf countVar `seq` rwhnf finaliser

newtype RefCount = RefCount Int
  deriving stock (Eq, Ord, Show)

{-# SPECIALISE unsafeMkRefCounterN :: RefCount -> Maybe (IO ()) -> IO (RefCounter IO) #-}
-- | Like 'mkRefCounterN', but throws an error if the initial @n < 1@
unsafeMkRefCounterN :: PrimMonad m => RefCount -> Maybe (m ()) -> m (RefCounter m)
unsafeMkRefCounterN !n finaliser = mkRefCounterN n finaliser >>= \case
    Nothing -> error "unsafeMkRefCounterN: n < 1"
    Just rc -> pure rc

{-# SPECIALISE mkRefCounterN :: RefCount -> Maybe (IO ()) -> IO (Maybe (RefCounter IO)) #-}
-- | Make a reference counter with initial value @n@. An optional finaliser is
-- run when the reference counter reaches @0@.
--
-- Returns 'Nothing' if @n < 1@, and 'Just' otherwise.
mkRefCounterN :: PrimMonad m => RefCount -> Maybe (m ()) -> m (Maybe (RefCounter m))
mkRefCounterN (RefCount !n) finaliser
  | n < 1 = pure Nothing
  | otherwise = do
    countVar <- newPrimVar $! n
    pure $! Just $! RefCounter{countVar, finaliser}

{-# SPECIALISE mkRefCounter1 :: Maybe (IO ()) -> IO (RefCounter IO) #-}
-- | Make a reference counter with initial value @1@. An optional finaliser is
-- run when the reference counter reaches @0@.
mkRefCounter1 :: PrimMonad m => Maybe (m ()) -> m (RefCounter m)
mkRefCounter1 finaliser = fromJust <$> mkRefCounterN (RefCount 1) finaliser

{-# SPECIALISE addReference :: HasCallStack => RefCounter IO -> IO () #-}
-- | Increase the reference counter by one.
--
-- The count must be known (from context) to be non-zero already. Typically
-- this will be because the caller has a reference already and is handing out
-- another reference to some other code.
addReference :: (HasCallStack, PrimMonad m) => RefCounter m -> m ()
addReference RefCounter{countVar} = do
    prevCount <- fetchAddInt countVar 1
    assertWithCallStack (prevCount > 0) $ pure ()

{-# SPECIALISE removeReference :: HasCallStack => RefCounter IO -> IO () #-}
-- | Decrease the reference counter by one.
--
-- The count must be known (from context) to be non-zero. Typically this will
-- be because the caller has a reference already (that they took out themselves
-- or were given).
removeReference :: (HasCallStack, PrimMonad m, MonadMask m) => RefCounter m -> m ()
removeReference RefCounter{countVar, finaliser} = mask_ $ do
    prevCount <- fetchSubInt countVar 1
    assertWithCallStack (prevCount > 0) $ pure ()
    when (prevCount == 1) $ sequence_ finaliser

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
upgradeWeakReference :: PrimMonad m => RefCounter m -> m Bool
upgradeWeakReference RefCounter{countVar} = do
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

{-# SPECIALISE readRefCount :: RefCounter IO -> IO RefCount #-}
-- | Warning: reading the current reference count is inherently racy as there is
-- no way to reliably act on the information. It can be useful for debugging.
readRefCount :: PrimMonad m => RefCounter m -> m RefCount
readRefCount RefCounter{countVar} = RefCount <$> readPrimVar countVar

{-# INLINE assertWithCallStack #-}
-- | Version of 'assert' that does not complain about redundant constraints when
-- compiling with @-O@ or @-fignore-asserts@.
assertWithCallStack :: HasCallStack => Bool -> a -> a
assertWithCallStack b = assert (const b callStack)
