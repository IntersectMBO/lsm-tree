-- | A read-write-locked mutable variable with a bias towards write locks.
--
-- This module is intended to be imported qualified:
--
-- @
--   import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
--   import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
-- @
module Control.Concurrent.Class.MonadSTM.RWVar (
    RWVar (..)
  , RWState (..)
  , new
  , unsafeAcquireReadAccess
  , unsafeReleaseReadAccess
  , withReadAccess
  , unsafeAcquireWriteAccess
  , unsafeReleaseWriteAccess
  , withWriteAccess
  , withWriteAccess_
  ) where

import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.DeepSeq
import           Control.Monad.Class.MonadThrow
import           Data.Word

-- | A read-write-locked mutable variable with a bias towards write-locks.
newtype RWVar m a = RWVar (StrictTVar m (RWState a))

-- | __NOTE__: Only strict in the reference and not the referenced value.
instance NFData (RWVar m a) where
  rnf = rwhnf

data RWState a =
    -- | @n@ concurrent readers and no writer.
    Reading !Word64 !a
    -- | @n@ concurrent readers and no writer, but no new readers can get
    -- access.
  | WaitingToWrite !Word64 !a
    -- | A single writer and no concurrent readers.
  | Writing

{-# SPECIALISE new :: a -> IO (RWVar IO a) #-}
new :: MonadSTM m => a -> m (RWVar m a)
new !x = RWVar <$> newTVarIO (Reading 0 x)

{-# SPECIALISE unsafeAcquireReadAccess :: RWVar IO a -> STM IO a #-}
unsafeAcquireReadAccess :: MonadSTM m => RWVar m a -> STM m a
unsafeAcquireReadAccess (RWVar !var) = do
    readTVar var >>= \case
      Reading n x -> do
        writeTVar var (Reading (n+1) x)
        pure x
      WaitingToWrite{} -> retry
      Writing -> retry

{-# SPECIALISE unsafeReleaseReadAccess :: RWVar IO a -> STM IO () #-}
unsafeReleaseReadAccess :: MonadSTM m => RWVar m a -> STM m ()
unsafeReleaseReadAccess (RWVar !var) = do
    readTVar var >>= \case
      Reading n x
        | n == 0 -> error "releasing a reader without read access (Reading)"
        | otherwise -> writeTVar var (Reading (n - 1) x)
      WaitingToWrite n x
        | n == 0 -> error "releasing a reader without read access (WaitingToWrite)"
        | otherwise -> writeTVar var (WaitingToWrite (n - 1) x)
      Writing -> error "releasing a reader without read access (Writing)"

{-# SPECIALISE withReadAccess :: RWVar IO a -> (a -> IO b) -> IO b #-}
withReadAccess :: (MonadSTM m, MonadThrow m) => RWVar m a -> (a -> m b) -> m b
withReadAccess rwvar k =
    bracket
      (atomically $ unsafeAcquireReadAccess rwvar)
      (\_ -> atomically $ unsafeReleaseReadAccess rwvar)
      k

{-# SPECIALISE unsafeAcquireWriteAccess :: RWVar IO a -> IO a #-}
unsafeAcquireWriteAccess :: MonadSTM m => RWVar m a -> m a
unsafeAcquireWriteAccess rw@(RWVar !var) = do
    may <- atomically $ readTVar var >>= \case
      Reading n x
        | n == 0 -> do
            writeTVar var Writing
            pure (Just x)
        | otherwise -> do
            writeTVar var (WaitingToWrite n x)
            pure Nothing
      WaitingToWrite n x
        | n == 0 -> do
            writeTVar var Writing
            pure (Just x)
        | otherwise -> retry
      -- If we see this, someone else has already acquired a write lock, so we retry.
      Writing -> retry
    -- When the previous STM transaction returns Nothing, it signals that we set
    -- the RWState to WaitingToWrite. All that remains is to try acquiring write
    -- access again.
    --
    -- If multiple threads try to acquire write access concurrently, then they
    -- will race for access. Even if a thread has set RWState to WaitingToWrite,
    -- there is no guarantee that the same thread will acquire write access
    -- first when all readers have finished. However, if the writer has
    -- finished, then all other waiting threads will try to get write again
    -- until they succeed.
    --
    -- TODO: unsafeReleaseWriteAccess will set RWState to Reading 0. In case we
    -- have readers *and* writers waiting for a writer to finish, once the
    -- writer is finished there will be a race. In this race, readers and
    -- writers are just as likely to acquire access first. However, if we wanted
    -- to make RWVar even more biased towards writers, then we could ensure that
    -- all waiting writers get access before the readers get a chance. This
    -- would probably require us to change RWState to represent the case where
    -- writers are waiting for a writer to finish.
    case may of
      Nothing -> unsafeAcquireWriteAccess rw
      Just x  -> pure x

{-# SPECIALISE unsafeReleaseWriteAccess :: RWVar IO a -> a -> STM IO () #-}
unsafeReleaseWriteAccess :: MonadSTM m => RWVar m a -> a -> STM m ()
unsafeReleaseWriteAccess (RWVar !var) !x = do
    readTVar var >>= \case
      Reading _ _ -> error "releasing a writer without write access (Reading)"
      WaitingToWrite _ _ -> error "releasing a writer without write access (WaitingToWrite)"
      Writing -> writeTVar var (Reading 0 x)

{-# SPECIALISE withWriteAccess :: RWVar IO a -> (a -> IO (a, b)) -> IO b #-}
withWriteAccess :: (MonadSTM m, MonadCatch m) => RWVar m a -> (a -> m (a, b)) -> m b
withWriteAccess rwvar k = snd . fst <$>
    generalBracket
      (unsafeAcquireWriteAccess rwvar)
      (\x ec -> do
        atomically $ case ec of
            ExitCaseSuccess (x', _) -> unsafeReleaseWriteAccess rwvar x'
            ExitCaseException _     -> unsafeReleaseWriteAccess rwvar x
            ExitCaseAbort           -> unsafeReleaseWriteAccess rwvar x
      )
      k

{-# SPECIALISE withWriteAccess_ :: RWVar IO a -> (a -> IO a) -> IO () #-}
withWriteAccess_ :: (MonadSTM m, MonadCatch m) => RWVar m a -> (a -> m a) -> m ()
withWriteAccess_ rwvar k = withWriteAccess rwvar (fmap (,()) . k)
