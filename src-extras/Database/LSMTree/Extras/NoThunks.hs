{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE UndecidableInstances  #-}

{-# OPTIONS_GHC -Wno-orphans #-}

-- | 'NoThunks' orphan instances
module Database.LSMTree.Extras.NoThunks (
    assertNoThunks
  , NoThunksIOLike
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM.RWVar
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception
import           Control.Monad.Primitive
import           Control.Monad.ST.Unsafe (unsafeSTToIO)
import           Control.RefCount
import           Control.Tracer
import           Data.Arena
import           Data.BloomFilter
import           Data.Map.Strict
import           Data.Primitive
import           Data.Primitive.PrimVar
import           Data.Proxy
import           Data.Typeable
import qualified Data.Vector.Primitive as VP
import           Data.Word
import           Database.LSMTree.Internal as Internal
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.IndexCompact
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.RawBytes
import           Database.LSMTree.Internal.RawOverflowPage
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.RunReader hiding (Entry)
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.RunReaders
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
import           Database.LSMTree.Internal.Unsliced
import           Database.LSMTree.Internal.WriteBuffer
import           GHC.Generics
import           KMerge.Heap
import           NoThunks.Class
import           System.FS.API
import           System.FS.BlockIO.API
import           Unsafe.Coerce

assertNoThunks :: NoThunks a => a -> b -> b
assertNoThunks x = assert p
  where p = case unsafeNoThunks x of
              Nothing -> True
              Just thunkInfo -> error $ "Assertion failed: found thunk" <> show thunkInfo

{-------------------------------------------------------------------------------
  Public API
-------------------------------------------------------------------------------}

-- | Also checks 'NoThunks' for the 'Normal.TableHandle's that are known to be
-- open in the 'Common.Session'.
instance (NoThunksIOLike m, Typeable m, Typeable (PrimState m))
      => NoThunks (Session' m ) where
  showTypeOf (_ :: Proxy (Session' m)) = "Session'"
  wNoThunks ctx (Session' s) = wNoThunks ctx s

-- | Does not check 'NoThunks' for the 'Common.Session' that this
-- 'Normal.TableHandle' belongs to.
instance (NoThunksIOLike m, Typeable m)
      => NoThunks (NormalTable m k v blob) where
  showTypeOf (_ :: Proxy (NormalTable m k v blob)) = "NormalTable"
  wNoThunks ctx (NormalTable th) = wNoThunks ctx th

{-------------------------------------------------------------------------------
  Internal
-------------------------------------------------------------------------------}

deriving stock instance Generic (Internal.Session m h)
-- | Also checks 'NoThunks' for the 'Internal.TableHandle's that are known to be
-- open in the 'Internal.Session'.
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (Internal.Session m h)

deriving stock instance Generic (SessionState m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (SessionState m h)

deriving stock instance Generic (SessionEnv m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (SessionEnv m h)

deriving stock instance Generic (Internal.TableHandle m h)
-- | Does not check 'NoThunks' for the 'Internal.Session' that this
-- 'Internal.TableHandle' belongs to.
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h)
                        => NoThunks (Internal.TableHandle m h)

deriving stock instance Generic (TableHandleState m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h)
                        => NoThunks (TableHandleState m h)

deriving stock instance Generic (TableHandleEnv m h)
deriving via AllowThunksIn ["tableSession", "tableSessionEnv"] (TableHandleEnv m h)
    instance (NoThunksIOLike m, Typeable m, Typeable h) => NoThunks (TableHandleEnv m h)

-- | Does not check 'NoThunks' for the 'Internal.Session' that this
-- 'Internal.Cursor' belongs to.
deriving stock instance Generic (Internal.Cursor m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (Internal.Cursor m h)

deriving stock instance Generic (CursorState m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (CursorState m h)

deriving stock instance Generic (CursorEnv m h)
deriving via AllowThunksIn ["cursorSession", "cursorSessionEnv"] (CursorEnv m h)
    instance (Typeable m, Typeable h, Typeable (PrimState m))
          => NoThunks (CursorEnv m h)

{-------------------------------------------------------------------------------
  UniqCounter
-------------------------------------------------------------------------------}

deriving stock instance Generic (UniqCounter m)
deriving anyclass instance NoThunks (StrictMVar m Word64)
                        => NoThunks (UniqCounter m)

{-------------------------------------------------------------------------------
  Serialise
-------------------------------------------------------------------------------}

deriving stock instance Generic RawBytes
deriving anyclass instance NoThunks RawBytes

deriving stock instance Generic SerialisedKey
deriving anyclass instance NoThunks SerialisedKey

deriving stock instance Generic SerialisedValue
deriving anyclass instance NoThunks SerialisedValue

deriving stock instance Generic SerialisedBlob
deriving anyclass instance NoThunks SerialisedBlob

instance NoThunks (Unsliced a) where
  showTypeOf (_ :: Proxy (Unsliced a)) = "Unsliced"
  wNoThunks ctx (x :: Unsliced a) = noThunks ctx y
    where
      -- Unsliced is a newtype around a ByteArray, so we can unsafeCoerce
      -- safely. The bang pattern will only evaluate the coercion, because the
      -- byte array is already in WHNF.
      y :: ByteArray
      !y = unsafeCoerce x

{-------------------------------------------------------------------------------
  Run
-------------------------------------------------------------------------------}

deriving stock instance Generic (Run m (Handle h))
deriving anyclass instance NoThunks (Run m (Handle h))

deriving stock instance Generic RunDataCaching
deriving anyclass instance NoThunks RunDataCaching

{-------------------------------------------------------------------------------
  Paths
-------------------------------------------------------------------------------}

deriving stock instance Generic RunNumber
deriving anyclass instance NoThunks RunNumber

deriving stock instance Generic SessionRoot
deriving anyclass instance NoThunks SessionRoot

deriving stock instance Generic RunFsPaths
deriving anyclass instance NoThunks RunFsPaths

{-------------------------------------------------------------------------------
  WriteBuffer
-------------------------------------------------------------------------------}

instance NoThunks WriteBuffer where
  showTypeOf (_ :: Proxy WriteBuffer) = "WriteBuffer"
  wNoThunks ctx (x :: WriteBuffer) = noThunks ctx y
    where
      -- toMap simply unwraps the WriteBuffer newtype wrapper. The bang pattern
      -- will only evaluate the coercion, because the inner Map is already in
      -- WHNF.
      y :: Map SerialisedKey (Entry SerialisedValue SerialisedBlob)
      !y = toMap x

{-------------------------------------------------------------------------------
  IndexCompact
-------------------------------------------------------------------------------}

deriving stock instance Generic IndexCompact
deriving anyclass instance NoThunks IndexCompact

deriving stock instance Generic PageNo
deriving anyclass instance NoThunks PageNo

{-------------------------------------------------------------------------------
  MergeSchedule
-------------------------------------------------------------------------------}

deriving stock instance Generic (TableContent m h)
deriving anyclass instance NoThunks (TableContent m h)

deriving stock instance Generic (LevelsCache m (Handle h))
deriving anyclass instance NoThunks (LevelsCache m (Handle h))

deriving stock instance Generic (Level m (Handle h))
deriving anyclass instance NoThunks (Level m (Handle h))

deriving stock instance Generic (MergingRun m (Handle h))
deriving anyclass instance NoThunks (MergingRun m (Handle h))

deriving stock instance Generic (MergingRunState m (Handle h))
deriving anyclass instance NoThunks (MergingRunState m (Handle h))

{-------------------------------------------------------------------------------
  Entry
-------------------------------------------------------------------------------}

deriving stock instance Generic (Entry v b)
deriving anyclass instance (NoThunks v, NoThunks b)
                        => NoThunks (Entry v b)

deriving stock instance Generic NumEntries
deriving anyclass instance NoThunks NumEntries

{-------------------------------------------------------------------------------
  Readers
-------------------------------------------------------------------------------}

deriving stock instance Generic (Readers s (Handle h))
deriving anyclass instance (Typeable s, Typeable h)
                        => NoThunks (Readers s (Handle h))

deriving stock instance Generic ReaderNumber
deriving anyclass instance NoThunks ReaderNumber

deriving stock instance Generic (ReadCtx (Handle h))
deriving anyclass instance NoThunks (ReadCtx (Handle h))

{-------------------------------------------------------------------------------
  Reader
-------------------------------------------------------------------------------}

deriving stock instance Generic (RunReader m (Handle h))
deriving anyclass instance NoThunks (RunReader m (Handle h))

deriving stock instance Generic (Reader.Entry m (Handle h))
deriving anyclass instance NoThunks (Reader.Entry m (Handle h))

{-------------------------------------------------------------------------------
  RawPage
-------------------------------------------------------------------------------}

deriving stock instance Generic RawPage
deriving anyclass instance NoThunks RawPage

{-------------------------------------------------------------------------------
  RawPage
-------------------------------------------------------------------------------}

deriving stock instance Generic RawOverflowPage
deriving anyclass instance NoThunks RawOverflowPage

{-------------------------------------------------------------------------------
  BlobRef
-------------------------------------------------------------------------------}

deriving stock instance Generic (BlobRef r)
deriving anyclass instance NoThunks r => NoThunks (BlobRef r)

deriving stock instance Generic BlobSpan
deriving anyclass instance NoThunks BlobSpan

{-------------------------------------------------------------------------------
  Arena
-------------------------------------------------------------------------------}

-- TODO: proper instance
deriving via OnlyCheckWhnfNamed "ArenaManager" (ArenaManager m)
    instance NoThunks (ArenaManager m)

{-------------------------------------------------------------------------------
  Config
-------------------------------------------------------------------------------}

deriving stock instance Generic TableConfig
deriving anyclass instance NoThunks TableConfig

deriving stock instance Generic MergePolicy
deriving anyclass instance NoThunks MergePolicy

deriving stock instance Generic SizeRatio
deriving anyclass instance NoThunks SizeRatio

deriving stock instance Generic WriteBufferAlloc
deriving anyclass instance NoThunks WriteBufferAlloc

deriving stock instance Generic BloomFilterAlloc
deriving anyclass instance NoThunks BloomFilterAlloc

deriving stock instance Generic FencePointerIndex
deriving anyclass instance NoThunks FencePointerIndex

deriving stock instance Generic DiskCachePolicy
deriving anyclass instance NoThunks DiskCachePolicy

deriving stock instance Generic MergeSchedule
deriving anyclass instance NoThunks MergeSchedule

{-------------------------------------------------------------------------------
  RWVar
-------------------------------------------------------------------------------}

deriving stock instance Generic (RWVar m a)
deriving anyclass instance NoThunks (StrictTVar m (RWState a)) => NoThunks (RWVar m a)

deriving stock instance Generic (RWState a)
deriving anyclass instance NoThunks a => NoThunks (RWState a)

{-------------------------------------------------------------------------------
  RefCount
-------------------------------------------------------------------------------}

instance NoThunks (RefCounter m) where
  showTypeOf (_ :: Proxy (RefCounter m)) = "RefCounter"
  wNoThunks ctx
    (RefCounter (a :: PrimVar (PrimState m) Int) (b :: Maybe (m ())))
    = allNoThunks [
          noThunks ctx a
          -- it is okay to use @$!@ because @b :: Maybe _@ was already in WHNF
        , noThunks ctx $! (OnlyCheckWhnfNamed <$> b :: Maybe (OnlyCheckWhnfNamed "finaliser" (m ())))
        ]

deriving stock instance Generic RefCount
deriving anyclass instance NoThunks RefCount

{-------------------------------------------------------------------------------
  kmerge
-------------------------------------------------------------------------------}

instance (NoThunks a, Typeable s, Typeable a) => NoThunks (MutableHeap s a) where
  showTypeOf (p :: Proxy (MutableHeap s a)) = show $ typeRep p
  wNoThunks ctx
    (MH (a :: PrimVar s Int) (b :: SmallMutableArray s a))
    = allNoThunks [
          noThunks ctx a
          -- the small array may contain bogus/undefined placeholder values
          -- after the first @n@ elements in the heap
        , noThunks ctx $! do
            n <- unsafeSTToIO (readPrimVar a)
            allNoThunks [
                unsafeSTToIO (readSmallArray b i) >>= \x -> noThunks ctx x
              | i <- [0..n-1]
              ]
        ]

{-------------------------------------------------------------------------------
  IOLike
-------------------------------------------------------------------------------}

-- | 'NoThunks' constraints for IO-like monads
--
-- Some constraints, like @NoThunks (MutVar s a)@ and @NoThunks (StrictTVar m
-- a)@, can not be satisfied for arbitrary @m@\/@s@, and must be instantiated
-- for a concrete @m@\/@s@, like @IO@\/@RealWorld@.
class ( forall a. NoThunks a => NoThunks (StrictTVar m a)
      , forall a. NoThunks a => NoThunks (StrictMVar m a)
      ) => NoThunksIOLike' m s

instance NoThunksIOLike' IO RealWorld

type NoThunksIOLike m = NoThunksIOLike' m (PrimState m)

instance NoThunks a => NoThunks (StrictTVar IO a) where
  showTypeOf (_ :: Proxy (StrictTVar IO a)) = "StrictTVar IO"
  wNoThunks ctx var = do
      x <- readTVarIO var
      noThunks ctx x

instance NoThunks a => NoThunks (StrictMVar IO a) where
  showTypeOf (_ :: Proxy (StrictMVar IO a)) = "StrictMVar IO"
  wNoThunks ctx var = do
      x <- readMVar var
      noThunks ctx x

{-------------------------------------------------------------------------------
  vector
-------------------------------------------------------------------------------}

-- TODO: https://github.com/input-output-hk/nothunks/issues/57
deriving via OnlyCheckWhnfNamed "Primitive.Vector" (VP.Vector a)
    instance NoThunks (VP.Vector a)

{-------------------------------------------------------------------------------
  primitive
-------------------------------------------------------------------------------}

-- TODO: https://github.com/input-output-hk/nothunks/issues/56
instance NoThunks a => NoThunks (MutVar s a) where
  showTypeOf (_ :: Proxy (MutVar s a)) = "MutVar"
  wNoThunks ctx var = do
      x <- unsafeSTToIO $ readMutVar var
      noThunks ctx (AllowThunk x) -- TODO: atomicModifyMutVar' is not strict enough

-- TODO: https://github.com/input-output-hk/nothunks/issues/56
instance (NoThunks a, Prim a) => NoThunks (PrimVar s a) where
  showTypeOf (_ :: Proxy (PrimVar s a)) = "PrimVar"
  wNoThunks ctx var = do
      x <- unsafeSTToIO $ readPrimVar var
      noThunks ctx x

-- TODO: https://github.com/input-output-hk/nothunks/issues/56
instance NoThunks a => NoThunks (SmallMutableArray s a) where
  showTypeOf (_ :: Proxy (SmallMutableArray s a)) = "SmallMutableArray"
  wNoThunks ctx arr = do
      n <- unsafeSTToIO $ getSizeofSmallMutableArray arr
      allNoThunks [
          unsafeSTToIO (readSmallArray arr i) >>= \x -> noThunks ctx x
        | i <- [0..n-1]
        ]

-- TODO: https://github.com/input-output-hk/nothunks/issues/56
deriving via OnlyCheckWhnfNamed "ByteArray" ByteArray
    instance NoThunks ByteArray

{-------------------------------------------------------------------------------
  bloomfilter
-------------------------------------------------------------------------------}

-- TODO: check heap?
deriving via OnlyCheckWhnfNamed "Bloom" (Bloom a)
    instance NoThunks (Bloom a)

{-------------------------------------------------------------------------------
  fs-api and fs-sim
-------------------------------------------------------------------------------}

-- TODO: check heap?
deriving via OnlyCheckWhnfNamed "HasFS" (HasFS m h)
    instance NoThunks (HasFS m h)

-- TODO: check heap?
deriving via OnlyCheckWhnfNamed "Handle" (Handle h)
    instance NoThunks (Handle h)

-- TODO: check heap?
deriving via OnlyCheckWhnfNamed "FsPath" FsPath
    instance NoThunks FsPath

{-------------------------------------------------------------------------------
  blockio-api and blockio-sim
-------------------------------------------------------------------------------}

-- TODO: check heap?
deriving via OnlyCheckWhnfNamed "HasBlockIO" (HasBlockIO m h)
    instance NoThunks (HasBlockIO m h)

-- TODO: check heap?
deriving via OnlyCheckWhnfNamed "LockFileHandle" (LockFileHandle m)
    instance NoThunks (LockFileHandle m)

{-------------------------------------------------------------------------------
  contra-tracer
-------------------------------------------------------------------------------}

-- TODO: check heap?
deriving via OnlyCheckWhnfNamed "Tracer" (Tracer m a)
    instance NoThunks (Tracer m a)
