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
-- | Acquire write access. This function assumes that it runs in a masked
-- context, and that is properly paired with an 'unsafeReleaseWriteAccess'!
--
-- If multiple threads try to acquire write access concurrently, then they will
-- race for access. However, if a thread has set RWState to WaitingToWrite, then
-- it is guaranteed that the same thread will acquire write access when all
-- readers have finished. That is, other writes can not "jump the queue". When
-- the writer finishes, then all other waiting threads will race for write
-- access again.
--
-- TODO: unsafeReleaseWriteAccess will set RWState to Reading 0. In case we have
-- readers *and* writers waiting for a writer to finish, once the writer is
-- finished there will be a race. In this race, readers and writers are just as
-- likely to acquire access first. However, if we wanted to make RWVar even more
-- biased towards writers, then we could ensure that all waiting writers get
-- access before the readers get a chance. This would probably require us to
-- change RWState to represent the case where writers are waiting for a writer
-- to finish.
unsafeAcquireWriteAccess :: (MonadSTM m, MonadCatch m) => RWVar m a -> m a
unsafeAcquireWriteAccess (RWVar !var) =
    -- trySetWriting is interruptible, but it is fine if it is interrupted
    -- because the RWState can not be changed before the interruption.
    --
    -- trySetWriting might update the RWState. There are interruptible
    -- operations in the body of the bracketOnError (in waitToWrite), so async
    -- exceptions can be delivered there. If an async exception happens because
    -- of an interrupt, we undo the RWState change using undoWaitingToWrite.
    --
    -- Note that if waitToWrite is interrupted, that it is impossible for the
    -- RWState to have changed from WaitingToWrite to either Reading or Writing.
    -- Therefore, undoWaitingToWrite can assume that it will find WaitingToWrite
    -- in the lock.
    bracketOnError trySetWriting undoWaitingToWrite $
      -- When Nothing is returned, it means that we set the RWState to
      -- WaitingToWrite, and so we wait to acquire the final write access.
      --
      -- When Just is returned, we already have write access.
      maybe waitToWrite pure
  where
    -- Try to acquire a write lock immediately, or otherwise set the internal
    -- state to WaitingToWrite as soon as possible.
    --
    -- Note: this is interruptible
    trySetWriting = atomically $ readTVar var >>= \case
        Reading n x
          | n == 0 -> do
              writeTVar var Writing
              pure (Just x)
          | otherwise -> do
              writeTVar var (WaitingToWrite n x)
              pure Nothing
        -- The following two branches are interruptible
        WaitingToWrite _n _x -> retry
        Writing -> retry

    -- Note: this is uninterruptible
    undoWaitingToWrite Nothing  =  atomically $ readTVar var >>= \case
        Reading _n _x -> error "undoWaitingToWrite: found Reading but expected WaitingToWrite"
        WaitingToWrite n x -> writeTVar var (Reading n x)
        Writing -> error "undoWaitingToWrite: found Writing but expected WaitingToWrite"
    undoWaitingToWrite (Just _) = error "undoWaitingToWrite: found Just but expected Nothing"

    -- Wait for the number of readers to go to 0, and then finally acquire write
    -- access.
    --
    -- Note: this is interruptible
    waitToWrite = atomically $ readTVar var >>= \case
        Reading _n _x -> error "waitToWrite: found Reading but expected WaitingToWrite"
        WaitingToWrite n x
          | n == 0 -> do
              writeTVar var Writing
              pure x
          -- This branch is interruptible
          | otherwise -> retry
        Writing -> error "waitToWrite: found Reading but expected Writing"

{-# SPECIALISE unsafeReleaseWriteAccess :: RWVar IO a -> a -> STM IO () #-}
-- | Release write access. This function assumes that it runs in a masked
-- context, and that is properly paired with an 'unsafeAcquireWriteAccess'!
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
