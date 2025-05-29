module Misc
(
    conv,
    first
)
where

yield :: Monad m => a -> m a
yield = return

{-# SPECIALISE first :: [a] -> IO (WeakPtr a) #-}
-- | Get a weak pointer to the first element of a list.
first :: MonadWeak m => [a] -> m (WeakPtr a)
first = _

{-# SPECIALISE last :: [a] -> IO (WeakPtr a) #-}
last :: [a] -> IO (WeakPtr a)
last _ = _

{-# SPECIALISE conv :: MonadIO m => [a] -> m a #-}
conv :: (Functor f, Monad m) => f a -> m a
conv = id

{-# SPECIALISE mis :: MonadIO m => [a] -> IO a #-}
match :: (Functor f, Monad m) => f a -> m a
match = id
