{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE UndecidableInstances  #-}

{-# OPTIONS_GHC -Wno-orphans #-}

-- | 'NoThunks' orphan instances
module Database.LSMTree.Extras.NoThunks (
    assertNoThunks
  , prop_NoThunks
  , NoThunksIOLike
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM.RWVar
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception
import           Control.Monad.Primitive
import           Control.Monad.ST.Unsafe (unsafeIOToST, unsafeSTToIO)
import           Control.RefCount
import           Control.Tracer
import           Data.Arena
import           Data.Bit
import           Data.BloomFilter
import           Data.Map.Strict
import           Data.Primitive
import           Data.Primitive.PrimVar
import           Data.Proxy
import           Data.STRef
import           Data.Typeable
import qualified Data.Vector.Mutable as VM
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed.Mutable as VUM
import           Data.Word
import           Database.LSMTree.Internal as Internal
import           Database.LSMTree.Internal.BlobFile
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.ChecksumHandle
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.CRC32C
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Index.Compact
import           Database.LSMTree.Internal.Index.CompactAcc
import           Database.LSMTree.Internal.Merge hiding (Level)
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.Page
import           Database.LSMTree.Internal.PageAcc
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.RawBytes
import           Database.LSMTree.Internal.RawOverflowPage
import           Database.LSMTree.Internal.RawPage
import           Database.LSMTree.Internal.Run
import           Database.LSMTree.Internal.RunAcc
import           Database.LSMTree.Internal.RunBuilder
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.RunReader hiding (Entry)
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.RunReaders
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
import           Database.LSMTree.Internal.Unsliced
import           Database.LSMTree.Internal.WriteBuffer
import           Database.LSMTree.Internal.WriteBufferBlobs
import           GHC.Generics
import           KMerge.Heap
import           NoThunks.Class
import           System.FS.API
import           System.FS.BlockIO.API
import           Test.QuickCheck (Property, Testable (..), counterexample)
import           Unsafe.Coerce

assertNoThunks :: NoThunks a => a -> b -> b
assertNoThunks x = assert p
  where p = case unsafeNoThunks x of
              Nothing -> True
              Just thunkInfo -> error $ "Assertion failed: found thunk" <> show thunkInfo

prop_NoThunks :: NoThunks a => a -> Property
prop_NoThunks x =
    case unsafeNoThunks x of
      Nothing        -> property True
      Just thunkInfo -> counterexample ("Found thunk " <> show thunkInfo) False

{-------------------------------------------------------------------------------
  Public API
-------------------------------------------------------------------------------}

-- | Also checks 'NoThunks' for the 'Normal.Table's that are known to be
-- open in the 'Common.Session'.
instance (NoThunksIOLike m, Typeable m, Typeable (PrimState m))
      => NoThunks (Session' m ) where
  showTypeOf (_ :: Proxy (Session' m)) = "Session'"
  wNoThunks ctx (Session' s) = wNoThunks ctx s

-- | Does not check 'NoThunks' for the 'Common.Session' that this
-- 'Normal.Table' belongs to.
instance (NoThunksIOLike m, Typeable m, Typeable (PrimState m))
      => NoThunks (NormalTable m k v blob) where
  showTypeOf (_ :: Proxy (NormalTable m k v blob)) = "NormalTable"
  wNoThunks ctx (NormalTable t) = wNoThunks ctx t

{-------------------------------------------------------------------------------
  Internal
-------------------------------------------------------------------------------}

deriving stock instance Generic (Internal.Session m h)
-- | Also checks 'NoThunks' for the 'Internal.Table's that are known to be
-- open in the 'Internal.Session'.
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (Internal.Session m h)

deriving stock instance Generic (SessionState m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (SessionState m h)

deriving stock instance Generic (SessionEnv m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (SessionEnv m h)

deriving stock instance Generic (Internal.Table m h)
-- | Does not check 'NoThunks' for the 'Internal.Session' that this
-- 'Internal.Table' belongs to.
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (Internal.Table m h)

deriving stock instance Generic (TableState m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (TableState m h)

deriving stock instance Generic (TableEnv m h)
deriving via AllowThunksIn ["tableSession", "tableSessionEnv"] (TableEnv m h)
    instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
          => NoThunks (TableEnv m h)

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

deriving stock instance Generic (Run m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (Run m h)

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

deriving stock instance Generic (ForKOps a)
deriving anyclass instance NoThunks a => NoThunks (ForKOps a)

deriving stock instance Generic (ForBlob a)
deriving anyclass instance NoThunks a => NoThunks (ForBlob a)

deriving stock instance Generic (ForFilter a)
deriving anyclass instance NoThunks a => NoThunks (ForFilter a)

deriving stock instance Generic (ForIndex a)
deriving anyclass instance NoThunks a => NoThunks (ForIndex a)

deriving stock instance Generic (ForRunFiles a)
deriving anyclass instance NoThunks a => NoThunks (ForRunFiles a)

{-------------------------------------------------------------------------------
  CRC32C
-------------------------------------------------------------------------------}

deriving stock instance Generic CRC32C
deriving anyclass instance NoThunks CRC32C

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
      y :: Map SerialisedKey (Entry SerialisedValue BlobSpan)
      !y = toMap x

{-------------------------------------------------------------------------------
  BlobFile
-------------------------------------------------------------------------------}

deriving stock instance Generic (WriteBufferBlobs m h)
deriving anyclass instance (Typeable (PrimState m), Typeable h)
                        => NoThunks (WriteBufferBlobs m h)

deriving stock instance Generic (FilePointer m)
deriving anyclass instance Typeable (PrimState m)
                        => NoThunks (FilePointer m)

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
deriving anyclass instance ( Typeable m, Typeable (PrimState m), Typeable h
                           , NoThunks (StrictMVar m (MergingRunState m h))
                           ) => NoThunks (TableContent m h)

deriving stock instance Generic (LevelsCache m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (LevelsCache m h)

deriving stock instance Generic (Level m h)
deriving anyclass instance ( Typeable m, Typeable (PrimState m), Typeable h
                           , NoThunks (StrictMVar m (MergingRunState m h))
                           ) => NoThunks (Level m h)

deriving stock instance Generic (IncomingRun m h)
deriving anyclass instance ( Typeable m, Typeable (PrimState m), Typeable h
                           , NoThunks (StrictMVar m (MergingRunState m h))
                           ) => NoThunks (IncomingRun m h)

deriving stock instance Generic (MergingRun m h)
deriving anyclass instance ( Typeable m, Typeable (PrimState m), Typeable h
                           , NoThunks (StrictMVar m (MergingRunState m h))
                           ) => NoThunks (MergingRun m h)

deriving stock instance Generic (MergingRunState m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (MergingRunState m h)

deriving stock instance Generic MergePolicyForLevel
deriving anyclass instance NoThunks MergePolicyForLevel

deriving stock instance Generic NumRuns
deriving anyclass instance NoThunks NumRuns

deriving stock instance Generic (UnspentCreditsVar s)
deriving anyclass instance Typeable s => NoThunks (UnspentCreditsVar s)

deriving stock instance Generic (TotalStepsVar s)
deriving anyclass instance Typeable s => NoThunks (TotalStepsVar s)

deriving stock instance Generic (SpentCreditsVar s)
deriving anyclass instance Typeable s => NoThunks (SpentCreditsVar s)

deriving stock instance Generic MergeKnownCompleted
deriving anyclass instance NoThunks MergeKnownCompleted

{-------------------------------------------------------------------------------
  Entry
-------------------------------------------------------------------------------}

deriving stock instance Generic (Entry v b)
deriving anyclass instance (NoThunks v, NoThunks b)
                        => NoThunks (Entry v b)

deriving stock instance Generic NumEntries
deriving anyclass instance NoThunks NumEntries

{-------------------------------------------------------------------------------
  RunBuilder
-------------------------------------------------------------------------------}

deriving stock instance Generic (RunBuilder m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (RunBuilder m h)

deriving stock instance Generic (ChecksumHandle s h)
deriving anyclass instance (Typeable s, Typeable h)
                        => NoThunks (ChecksumHandle s h)

{-------------------------------------------------------------------------------
  RunAcc
-------------------------------------------------------------------------------}

deriving stock instance Generic (RunAcc s)
deriving anyclass instance Typeable s
                        => NoThunks (RunAcc s)

{-------------------------------------------------------------------------------
  IndexCompactAcc
-------------------------------------------------------------------------------}

deriving stock instance Generic (IndexCompactAcc s)
deriving anyclass instance Typeable s
                        => NoThunks (IndexCompactAcc s)

deriving stock instance Generic (SMaybe a)
deriving anyclass instance NoThunks a => NoThunks (SMaybe a)

{-------------------------------------------------------------------------------
  PageAcc
-------------------------------------------------------------------------------}

deriving stock instance Generic (PageAcc s)
deriving anyclass instance Typeable s
                        => NoThunks (PageAcc s)

{-------------------------------------------------------------------------------
  Merge
-------------------------------------------------------------------------------}

deriving stock instance Generic (Merge m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (Merge m h)

deriving stock instance Generic Merge.Level
deriving anyclass instance NoThunks Merge.Level

deriving stock instance Generic Merge.StepResult
deriving anyclass instance NoThunks Merge.StepResult

deriving stock instance Generic Merge.MergeState
deriving anyclass instance NoThunks Merge.MergeState

{-------------------------------------------------------------------------------
  Readers
-------------------------------------------------------------------------------}

deriving stock instance Generic (Readers m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (Readers m h)

deriving stock instance Generic (Reader m h)
instance (Typeable m, Typeable (PrimState m), Typeable h)
      => NoThunks (Reader m h) where
  showTypeOf (_ :: Proxy (Reader m h)) = "Reader"
  wNoThunks ctx = \case
    ReadRun r      -> noThunks ctx r
    ReadBuffer var -> noThunks ctx (OnlyCheckWhnf var) -- contents intentionally lazy

deriving stock instance Generic ReaderNumber
deriving anyclass instance NoThunks ReaderNumber

deriving stock instance Generic (ReadCtx m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (ReadCtx m h)

{-------------------------------------------------------------------------------
  Reader
-------------------------------------------------------------------------------}

deriving stock instance Generic (RunReader m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (RunReader m h)

deriving stock instance Generic (Reader.Entry m h)
deriving anyclass instance (Typeable m, Typeable (PrimState m), Typeable h)
                        => NoThunks (Reader.Entry m h)

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

deriving stock instance Generic BlobSpan
deriving anyclass instance NoThunks BlobSpan

deriving stock instance Generic (BlobFile m h)
deriving anyclass instance (Typeable h, Typeable (PrimState m))
                        => NoThunks (BlobFile m h)

deriving stock instance Generic (RawBlobRef m h)
deriving anyclass instance (Typeable h, Typeable (PrimState m))
                        => NoThunks (RawBlobRef m h)

deriving stock instance Generic (WeakBlobRef m h)
deriving anyclass instance (Typeable h, Typeable (PrimState m))
                        => NoThunks (WeakBlobRef m h)

{-------------------------------------------------------------------------------
  Arena
-------------------------------------------------------------------------------}

-- TODO: proper instance
deriving via OnlyCheckWhnf (ArenaManager m)
    instance Typeable m => NoThunks (ArenaManager m)

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

instance Typeable (PrimState m) => NoThunks (RefCounter m) where
  showTypeOf (_ :: Proxy (RefCounter m)) = "RefCounter"
  wNoThunks ctx
    (RefCounter (a :: PrimVar (PrimState m) Int) (b :: Maybe (m ())))
    = allNoThunks [
          noThunks ctx a
        , noThunks ctx $ (OnlyCheckWhnfNamed b :: OnlyCheckWhnfNamed "finaliser" (Maybe (m ())))
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

-- TODO: on ghc-9.4, a check on StrictTVar IO (RWState (TableContent IO h))
-- fails, but we have not yet found out why so we simply disable NoThunks checks
-- for StrictTVars on ghc-9.4
instance NoThunks a => NoThunks (StrictTVar IO a) where
  showTypeOf (_ :: Proxy (StrictTVar IO a)) = "StrictTVar IO"
  wNoThunks _ctx _var = do
#if defined(MIN_VERSION_GLASGOW_HASKELL)
#if MIN_VERSION_GLASGOW_HASKELL(9,4,0,0) && !MIN_VERSION_GLASGOW_HASKELL(9,6,0,0)
    pure Nothing
#else
    x <- readTVarIO _var
    noThunks _ctx x
#endif
#endif

instance NoThunks a => NoThunks (StrictMVar IO a) where
  showTypeOf (_ :: Proxy (StrictMVar IO a)) = "StrictMVar IO"
  wNoThunks ctx var = do
      x <- readMVar var
      noThunks ctx x

{-------------------------------------------------------------------------------
  vector
-------------------------------------------------------------------------------}

-- TODO: upstream to @nothunks@
instance (NoThunks a, Typeable s, Typeable a) => NoThunks (VM.MVector s a) where
    showTypeOf (p :: Proxy (VM.MVector s a)) = show $ typeRep p
    wNoThunks ctx v =
      allNoThunks [
          unsafeSTToIO (VM.read v i >>= \ x -> unsafeIOToST (noThunks ctx x))
        | i <- [0.. VM.length v-1]
        ]

-- TODO: https://github.com/input-output-hk/nothunks/issues/57
deriving via OnlyCheckWhnf (VP.Vector a)
    instance Typeable a => NoThunks (VP.Vector a)

-- TODO: upstream to @nothunks@
deriving via OnlyCheckWhnf (VUM.MVector s Word64)
    instance Typeable s => NoThunks (VUM.MVector s Word64)

-- TODO: upstream to @nothunks@
deriving via OnlyCheckWhnf (VUM.MVector s Bit)
    instance Typeable s => NoThunks (VUM.MVector s Bit)

{-------------------------------------------------------------------------------
  ST
-------------------------------------------------------------------------------}

-- TODO: upstream to @nothunks@
instance NoThunks a => NoThunks (STRef s a) where
  showTypeOf (_ :: Proxy (STRef s a)) = "STRef"
  wNoThunks ctx ref = do
    x <- unsafeSTToIO $ readSTRef ref
    noThunks ctx x

{-------------------------------------------------------------------------------
  primitive
-------------------------------------------------------------------------------}

-- TODO: https://github.com/input-output-hk/nothunks/issues/56
instance NoThunks a => NoThunks (MutVar s a) where
  showTypeOf (_ :: Proxy (MutVar s a)) = "MutVar"
  wNoThunks ctx var = do
      x <- unsafeSTToIO $ readMutVar var
      noThunks ctx x

-- TODO: https://github.com/input-output-hk/nothunks/issues/56
deriving via OnlyCheckWhnf (PrimVar s a)
    instance (Typeable s, Typeable a) => NoThunks (PrimVar s a)

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
deriving via OnlyCheckWhnf (MutablePrimArray s a)
    instance (Typeable s, Typeable a) => NoThunks (MutablePrimArray s a)

-- TODO: https://github.com/input-output-hk/nothunks/issues/56
deriving via OnlyCheckWhnf ByteArray
    instance NoThunks ByteArray

{-------------------------------------------------------------------------------
  bloomfilter
-------------------------------------------------------------------------------}

-- TODO: check heap?
deriving via OnlyCheckWhnf (Bloom a)
    instance Typeable a => NoThunks (Bloom a)

-- TODO: check heap?
deriving via OnlyCheckWhnf (MBloom s a)
    instance (Typeable s, Typeable a) => NoThunks (MBloom s a)

{-------------------------------------------------------------------------------
  fs-api and fs-sim
-------------------------------------------------------------------------------}

-- TODO: check heap?
deriving via OnlyCheckWhnf (HasFS m h)
    instance (Typeable m, Typeable h) => NoThunks (HasFS m h)

-- TODO: check heap?
deriving via OnlyCheckWhnf (Handle h)
    instance Typeable h => NoThunks (Handle h)

-- TODO: check heap?
deriving via OnlyCheckWhnf FsPath
    instance NoThunks FsPath

{-------------------------------------------------------------------------------
  blockio-api and blockio-sim
-------------------------------------------------------------------------------}

-- TODO: check heap?
deriving via OnlyCheckWhnf (HasBlockIO m h)
    instance (Typeable m, Typeable h) => NoThunks (HasBlockIO m h)

-- TODO: check heap?
deriving via OnlyCheckWhnf (LockFileHandle m)
    instance Typeable m => NoThunks (LockFileHandle m)

{-------------------------------------------------------------------------------
  contra-tracer
-------------------------------------------------------------------------------}

-- TODO: check heap?
deriving via OnlyCheckWhnf (Tracer m a)
    instance (Typeable m, Typeable a) => NoThunks (Tracer m a)
