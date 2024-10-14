{-# LANGUAGE DataKinds #-}

module Database.LSMTree.Internal.Cursor (
    -- TODO: right place?
    ResolveSerialisedValue
    -- ** Cursor API
  , Cursor (..)
  , CursorState (..)
  , CursorEnv (..)
  , OffsetKey (..)
  , withCursor
  , newCursor
  , closeCursor
  , readCursor
  , readCursorWhile
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.DeepSeq
import           Control.Monad.Class.MonadST (MonadST (..))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Fix (MonadFix)
import           Control.Monad.Primitive
import           Control.TempRegistry
import           Control.Tracer
import           Data.Foldable
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Internal.BlobRef (WeakBlobRef (..))
import qualified Database.LSMTree.Internal.BlobRef as BlobRef
import           Database.LSMTree.Internal.Entry (Entry)
import qualified Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.RunReaders (OffsetKey (..))
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise (SerialisedKey,
                     SerialisedValue)
import           Database.LSMTree.Internal.Session
import           Database.LSMTree.Internal.TableHandle
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.Vector as V
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           System.FS.API (Handle)

{-------------------------------------------------------------------------------
  Cursors
-------------------------------------------------------------------------------}

-- | A read-only view into the table state at the time of cursor creation.
--
-- For more information, see 'Database.LSMTree.Normal.Cursor'.
--
-- The representation of a cursor is similar to that of a 'TableHandle', but
-- simpler, as it is read-only.
data Cursor m h = Cursor {
      -- | Mutual exclusion, only a single thread can read from a cursor at a
      -- given time.
      cursorState  :: !(StrictMVar m (CursorState m h))
    , cursorTracer :: !(Tracer m CursorTrace)
    }

instance NFData (Cursor m h) where
  rnf (Cursor a b) = rwhnf a `seq` rwhnf b

data CursorState m h =
    CursorOpen !(CursorEnv m h)
  | CursorClosed  -- ^ Calls to a closed cursor raise an exception.

data CursorEnv m h = CursorEnv {
    -- === Session-inherited

    -- | The session that this cursor belongs to.
    --
    -- NOTE: Consider using the 'cursorSessionEnv' field instead of acquiring
    -- the session lock.
    cursorSession    :: !(Session m h)
    -- | Use this instead of 'cursorSession' for easy access. An open cursor may
    -- assume that its session is open. A session's global resources, and
    -- therefore resources that are inherited by the cursor, will only be
    -- released once the session is sure that no cursors are open anymore.
  , cursorSessionEnv :: !(SessionEnv m h)

    -- === Cursor-specific

    -- | Session-unique identifier for this cursor.
  , cursorId         :: !Word64
    -- | Readers are immediately discarded once they are 'Readers.Drained',
    -- so if there is a 'Just', we can assume that it has further entries.
    -- However, the reference counts to the runs only get removed when calling
    -- 'closeCursor', as there might still be 'BlobRef's that need the
    -- corresponding run to stay alive.
  , cursorReaders    :: !(Maybe (Readers.Readers m h))
    -- | The runs held open by the cursor. We must remove a reference when the
    -- cursor gets closed.
  , cursorRuns       :: !(V.Vector (Run m h))

    -- | The write buffer blobs, which like the runs, we have to keep open
    -- untile the cursor is closed.
  , cursorWBB        :: WBB.WriteBufferBlobs m h
  }

{-# SPECIALISE withCursor ::
     OffsetKey
  -> TableHandle IO h
  -> (Cursor IO h -> IO a)
  -> IO a #-}
-- | See 'Database.LSMTree.Normal.withCursor'.
withCursor ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => OffsetKey
  -> TableHandle m h
  -> (Cursor m h -> m a)
  -> m a
withCursor offsetKey th = bracket (newCursor offsetKey th) closeCursor

{-# SPECIALISE newCursor ::
     OffsetKey
  -> TableHandle IO h
  -> IO (Cursor IO h) #-}
-- | See 'Database.LSMTree.Normal.newCursor'.
newCursor ::
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => OffsetKey
  -> TableHandle m h
  -> m (Cursor m h)
newCursor !offsetKey th = withOpenTable th $ \thEnv -> do
    let cursorSession = tableSession thEnv
    let cursorSessionEnv = tableSessionEnv thEnv
    cursorId <- uniqueToWord64 <$>
      incrUniqCounter (sessionUniqCounter cursorSessionEnv)
    let cursorTracer = TraceCursor cursorId `contramap` sessionTracer cursorSession
    traceWith cursorTracer $ TraceCreateCursor (tableId thEnv)

    -- We acquire a read-lock on the session open-state to prevent races, see
    -- 'sessionOpenTables'.
    withOpenSession cursorSession $ \_ -> do
      withTempRegistry $ \reg -> do
        (wb, wbblobs, cursorRuns) <-
          allocTableContent reg (tableContent thEnv)
        cursorReaders <-
          allocateMaybeTemp reg
            (Readers.new offsetKey (Just (wb, wbblobs)) cursorRuns)
            Readers.close
        let cursorWBB = wbblobs
        cursorState <- newMVar (CursorOpen CursorEnv {..})
        let !cursor = Cursor {cursorState, cursorTracer}
        -- Track cursor, but careful: If now an exception is raised, all
        -- resources get freed by the registry, so if the session still
        -- tracks 'cursor' (which is 'CursorOpen'), it later double frees.
        -- Therefore, we only track the cursor if 'withTempRegistry' exits
        -- successfully, i.e. using 'freeTemp'.
        freeTemp reg $
          modifyMVar_ (sessionOpenCursors cursorSessionEnv) $
            pure . Map.insert cursorId (Resource (closeCursor cursor))
        pure $! cursor
  where
    -- The table contents escape the read access, but we just added
    -- references to each run, so it is safe.
    allocTableContent reg contentVar = do
        RW.withReadAccess contentVar $ \content -> do
          let wb      = tableWriteBuffer content
              wbblobs = tableWriteBufferBlobs content
          allocateTemp reg
            (WBB.addReference wbblobs)
            (\_ -> WBB.removeReference wbblobs)
          let runs = cachedRuns (tableCache content)
          V.forM_ runs $ \r -> do
            allocateTemp reg
              (Run.addReference r)
              (\_ -> Run.removeReference r)
          pure (wb, wbblobs, runs)

{-# SPECIALISE closeCursor :: Cursor IO h -> IO () #-}
-- | See 'Database.LSMTree.Normal.closeCursor'.
closeCursor ::
     (MonadMask m, MonadMVar m, MonadSTM m, PrimMonad m)
  => Cursor m h
  -> m ()
closeCursor Cursor {..} = do
    traceWith cursorTracer $ TraceCloseCursor
    modifyWithTempRegistry_ (takeMVar cursorState) (putMVar cursorState) $ \reg -> \case
      CursorClosed -> return CursorClosed
      CursorOpen CursorEnv {..} -> do
        -- This should be safe-ish, but it's still not ideal, because it doesn't
        -- rule out sync exceptions in the cleanup operations.
        -- In that case, the cursor ends up closed, but resources might not have
        -- been freed. Probably better than the other way around, though.
        freeTemp reg $
          modifyMVar_ (sessionOpenCursors cursorSessionEnv) $
            pure . Map.delete cursorId

        forM_ cursorReaders $ freeTemp reg . Readers.close
        V.forM_ cursorRuns $ freeTemp reg . Run.removeReference
        freeTemp reg (WBB.removeReference cursorWBB)
        return CursorClosed

{-# SPECIALISE readCursor ::
     ResolveSerialisedValue
  -> Int
  -> Cursor IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO (Handle h)) -> res)
  -> IO (V.Vector res) #-}
-- | See 'Database.LSMTree.Normal.readCursor'.
readCursor ::
     forall m h res.
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> Int  -- ^ Maximum number of entries to read
  -> Cursor m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m (Handle h)) -> res)
     -- ^ How to map to a query result, different for normal/monoidal
  -> m (V.Vector res)
readCursor resolve n cursor fromEntry =
    readCursorWhile resolve (const True) n cursor fromEntry

{-# SPECIALISE readCursorWhile ::
     ResolveSerialisedValue
  -> (SerialisedKey -> Bool)
  -> Int
  -> Cursor IO h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO (Handle h)) -> res)
  -> IO (V.Vector res) #-}
-- | @readCursorWhile _ p n cursor _@ reads elements until either:
--
--    * @n@ elements have been read already
--    * @p@ returns @False@ for the key of an entry to be read
--    * the cursor is drained
--
-- Consequently, once a call returned fewer than @n@ elements, any subsequent
-- calls with the same predicate @p@ will return an empty vector.
readCursorWhile ::
     forall m h res.
     (MonadFix m, MonadMask m, MonadMVar m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> (SerialisedKey -> Bool)  -- ^ Only read as long as this predicate holds
  -> Int  -- ^ Maximum number of entries to read
  -> Cursor m h
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m (Handle h)) -> res)
     -- ^ How to map to a query result, different for normal/monoidal
  -> m (V.Vector res)
readCursorWhile resolve keyIsWanted n Cursor {..} fromEntry = do
    traceWith cursorTracer $ TraceReadCursor n
    modifyMVar cursorState $ \case
      CursorClosed -> throwIO ErrCursorClosed
      state@(CursorOpen cursorEnv) -> do
        case cursorReaders cursorEnv of
          Nothing ->
            -- a drained cursor will just return an empty vector
            return (state, V.empty)
          Just readers -> do
            (vec, hasMore) <- readCursorEntriesWhile resolve keyIsWanted fromEntry readers n
            -- if we drained the readers, remove them from the state
            let !state' = case hasMore of
                  Readers.HasMore -> state
                  Readers.Drained -> CursorOpen (cursorEnv {cursorReaders = Nothing})
            return (state', vec)

{-# INLINE readCursorEntriesWhile #-}
{-# SPECIALISE readCursorEntriesWhile :: forall h res.
     ResolveSerialisedValue
  -> (SerialisedKey -> Bool)
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef IO (Handle h)) -> res)
  -> Readers.Readers IO h
  -> Int
  -> IO (V.Vector res, Readers.HasMore) #-}
-- | General notes on the code below:
-- * it is quite similar to the one in Internal.Merge, but different enough
--   that it's probably easier to keep them separate
-- * any function that doesn't take a 'hasMore' argument assumes that the
--   readers have not been drained yet, so we must check before calling them
-- * there is probably opportunity for optimisations
readCursorEntriesWhile :: forall h m res.
     (MonadFix m, MonadMask m, MonadST m, MonadSTM m)
  => ResolveSerialisedValue
  -> (SerialisedKey -> Bool)
  -> (SerialisedKey -> SerialisedValue -> Maybe (WeakBlobRef m (Handle h)) -> res)
  -> Readers.Readers m h
  -> Int
  -> m (V.Vector res, Readers.HasMore)
readCursorEntriesWhile resolve keyIsWanted fromEntry readers n =
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
                   -> Entry SerialisedValue (BlobRef.BlobRef m (Handle h))
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
             -> Entry SerialisedValue (BlobRef.BlobRef m (Handle h))
             -> Maybe res
    toResult key = \case
        Entry.Insert v -> Just $ fromEntry key v Nothing
        Entry.InsertWithBlob v b -> Just $ fromEntry key v (Just (WeakBlobRef b))
        Entry.Mupdate v -> Just $ fromEntry key v Nothing
        Entry.Delete -> Nothing
