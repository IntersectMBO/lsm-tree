{-# LANGUAGE MagicHash #-}

module Control.RefCount (
    RefCounter
  , RefCount (..)
  , unsafeMkRefCounterN
  , mkRefCounterN
  , mkRefCounter1
  , addReference
  , removeReference
  , readRefCount
  ) where

import           Control.DeepSeq
import           Control.Exception (assert)
import           Control.Monad (when)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Maybe
import           Data.Primitive.PrimVar

-- | A reference counter with an optional finaliser action. Once the reference
-- count reaches @0@, the finaliser will be run.
data RefCounter m = RefCounter {
    countVar  :: !(PrimVar (PrimState m) Int)
  , finaliser :: !(Maybe (m ()))
  }

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
    -- evaluate the finaliser a little bit before we store it
    let !() = case finaliser of
                Nothing       -> ()
                Just !_action -> ()
    countVar <- newPrimVar $! n
    pure $! Just $! RefCounter{countVar, finaliser}

{-# SPECIALISE mkRefCounter1 :: Maybe (IO ()) -> IO (RefCounter IO) #-}
-- | Make a reference counter with initial value @1@. An optional finaliser is
-- run when the reference counter reaches @0@.
mkRefCounter1 :: PrimMonad m => Maybe (m ()) -> m (RefCounter m)
mkRefCounter1 finaliser = fromJust <$> mkRefCounterN (RefCount 1) finaliser

{-# SPECIALISE addReference :: RefCounter IO -> IO () #-}
-- | Increase the reference counter by one.
addReference :: PrimMonad m => RefCounter m -> m ()
addReference RefCounter{countVar} = do
    prevCount <- fetchAddInt countVar 1
    assert (prevCount > 0) $ pure ()

{-# SPECIALISE removeReference :: RefCounter IO -> IO () #-}
-- | Decrease the reference counter by one.
removeReference :: (PrimMonad m, MonadMask m) => RefCounter m -> m ()
removeReference RefCounter{countVar, finaliser} = mask_ $ do
    prevCount <- fetchSubInt countVar 1
    assert (prevCount > 0) $ pure ()
    when (prevCount == 1) $ sequence_ finaliser

{-# SPECIALISE readRefCount :: RefCounter IO -> IO RefCount #-}
-- | Warning: reading the current reference count is inherently racy as there is
-- no way to reliably act on the information. It can be useful for debugging.
readRefCount :: PrimMonad m => RefCounter m -> m RefCount
readRefCount RefCounter{countVar} = RefCount <$> readPrimVar countVar
