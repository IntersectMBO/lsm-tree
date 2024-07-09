-- | A read-write-locked mutable variable with a bias towards write locks.
--
-- This module is intended to be imported qualified:
--
-- @
--   import           Control.Concurrent.Class.MonadSTM.RWVar (RWVar)
--   import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
-- @
module Control.Concurrent.Class.MonadSTM.RWVar (
    RWVar
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
import           Control.Monad.Class.MonadThrow
import           Data.Word

-- | A read-write-locked mutable variable with a bias towards write-locks.
newtype RWVar m a = RWVar (StrictTVar m (RWState a))

data RWState a =
    -- | @n@ concurrent readers and no writer.
    Reading !Word64 !a
    -- | @n@ concurrent readers, and a single writer that is waiting for the
    -- readers to finish.
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

{-# SPECIALISE unsafeAcquireWriteAccess :: RWVar IO a -> STM IO a #-}
unsafeAcquireWriteAccess :: MonadSTM m => RWVar m a -> STM m a
unsafeAcquireWriteAccess (RWVar !var) = do
    readTVar var >>= \case
      Reading n x
        | n == 0 -> do
            writeTVar var Writing
            pure x
        | otherwise -> do
            writeTVar var (WaitingToWrite n x)
            retry
      WaitingToWrite n x
        | n == 0 -> do
            writeTVar var Writing
            pure x
        | otherwise -> retry
      -- One thread might be trying to acquire a write lock when another thread
      -- already has a write lock, so we retry.
      Writing -> retry

{-# SPECIALISE unsafeReleaseWriteAccess :: RWVar IO a -> a -> STM IO () #-}
unsafeReleaseWriteAccess :: MonadSTM m => RWVar m a -> a -> STM m ()
unsafeReleaseWriteAccess (RWVar !var) x = do
    readTVar var >>= \case
      Reading _ _ -> error "releasing a writer without write access (Reading)"
      WaitingToWrite _ _ -> error "releasing a writer without write access (WaitingToWrite)"
      Writing -> writeTVar var (Reading 0 x)

{-# SPECIALISE withWriteAccess :: RWVar IO a -> (a -> IO (a, b)) -> IO b #-}
withWriteAccess :: (MonadSTM m, MonadCatch m) => RWVar m a -> (a -> m (a, b)) -> m b
withWriteAccess rwvar k = snd . fst <$>
    generalBracket
      (atomically $ unsafeAcquireWriteAccess rwvar)
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
