{-# LANGUAGE DataKinds #-}
{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.Cursor (
    readEntriesWhile
  ) where

import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadThrow
import qualified Data.Vector as V
import           Database.LSMTree.Internal.BlobRef (RawBlobRef,
                     WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Entry (Entry)
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import qualified Database.LSMTree.Internal.Readers as Readers
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     SerialisedValue)
import qualified Database.LSMTree.Internal.Vector as V

{-# INLINE readEntriesWhile #-}
{-# SPECIALISE readEntriesWhile :: forall h res.
     ResolveSerialisedValue
  -> (SerialisedKey -> Bool)
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO h) -> res)
  -> Readers.Readers IO h
  -> Int
  -> IO (V.Vector res, Readers.HasMore) #-}
-- | General notes on the code below:
-- * it is quite similar to the one in Internal.Merge, but different enough
--   that it's probably easier to keep them separate
-- * any function that doesn't take a 'hasMore' argument assumes that the
--   readers have not been drained yet, so we must check before calling them
-- * there is probably opportunity for optimisations
readEntriesWhile :: forall h m res.
     (MonadMask m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> (SerialisedKey -> Bool)
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m h) -> res)
  -> Readers.Readers m h
  -> Int
  -> m (V.Vector res, Readers.HasMore)
readEntriesWhile resolve keyIsWanted fromEntry readers n =
    flip (V.unfoldrNM' n) Readers.HasMore $ \case
      Readers.Drained -> return (Nothing, Readers.Drained)
      Readers.HasMore -> readEntryIfWanted
  where
    -- Produces a result unless the readers have been drained or 'keyIsWanted'
    -- returned False.
    readEntryIfWanted :: m (Maybe res, Readers.HasMore)
    readEntryIfWanted = do
        key <- Readers.peekKey readers
        if keyIsWanted key then readEntry
                           else return (Nothing, Readers.HasMore)

    readEntry :: m (Maybe res, Readers.HasMore)
    readEntry = do
        (key, readerEntry, hasMore) <- Readers.pop readers
        let !entry = Reader.toFullEntry readerEntry
        case hasMore of
          Readers.Drained -> do
            handleResolved key entry Readers.Drained
          Readers.HasMore -> do
            case entry of
              Entry.Mupdate v ->
                handleMupdate key v
              _ -> do
                -- Anything but Mupdate supersedes all previous entries of
                -- the same key, so we can simply drop them and are done.
                hasMore' <- dropRemaining key
                handleResolved key entry hasMore'

    dropRemaining :: SerialisedKey -> m Readers.HasMore
    dropRemaining key = do
        (_, hasMore) <- Readers.dropWhileKey readers key
        return hasMore

    -- Resolve a 'Mupsert' value with the other entries of the same key.
    handleMupdate :: SerialisedKey
                  -> SerialisedValue
                  -> m (Maybe res, Readers.HasMore)
    handleMupdate key v = do
        nextKey <- Readers.peekKey readers
        if nextKey /= key
          then
            -- No more entries for same key, done.
            handleResolved key (Entry.Mupdate v) Readers.HasMore
          else do
            (_, nextEntry, hasMore) <- Readers.pop readers
            let resolved = Entry.combine resolve (Entry.Mupdate v)
                             (Reader.toFullEntry nextEntry)
            case hasMore of
              Readers.HasMore -> case resolved of
                Entry.Mupdate v' ->
                  -- Still a mupsert, keep resolving!
                  handleMupdate key v'
                _ -> do
                  -- Done with this key, remaining entries are obsolete.
                  hasMore' <- dropRemaining key
                  handleResolved key resolved hasMore'
              Readers.Drained -> do
                handleResolved key resolved Readers.Drained

    -- Once we have a resolved entry, we still have to make sure it's not
    -- a 'Delete', since we only want to write values to the result vector.
    handleResolved :: SerialisedKey
                   -> Entry SerialisedValue (RawBlobRef m h)
                   -> Readers.HasMore
                   -> m (Maybe res, Readers.HasMore)
    handleResolved key entry hasMore =
        case toResult key entry of
          Just !res ->
            -- Found one resolved value, done.
            return (Just res, hasMore)
          Nothing ->
            -- Resolved value was a Delete, which we don't want to include.
            -- So look for another one (unless there are no more entries!).
            case hasMore of
              Readers.HasMore -> readEntryIfWanted
              Readers.Drained -> return (Nothing, Readers.Drained)

    toResult :: SerialisedKey
             -> Entry SerialisedValue (RawBlobRef m h)
             -> Maybe res
    toResult key = \case
        Entry.Insert v -> Just $ fromEntry key v Nothing
        Entry.InsertWithBlob v b -> Just $ fromEntry key v (Just (BlobRef.rawToWeakBlobRef b))
        Entry.Mupdate v -> Just $ fromEntry key v Nothing
        Entry.Delete -> Nothing
