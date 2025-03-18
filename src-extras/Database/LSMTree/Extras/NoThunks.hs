{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE UndecidableInstances  #-}

{-# OPTIONS_GHC -Wno-orphans #-}

-- | 'NoThunks' orphan instances
module Database.LSMTree.Extras.NoThunks (
    assertNoThunks
  , propUnsafeNoThunks
  , propNoThunks
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
import           Database.LSMTree.Internal.Chunk
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.CRC32C
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.IncomingRun
import           Database.LSMTree.Internal.Index
import           Database.LSMTree.Internal.Index.Compact
import           Database.LSMTree.Internal.Index.CompactAcc
import           Database.LSMTree.Internal.Index.Ordinary
import           Database.LSMTree.Internal.Index.OrdinaryAcc
import           Database.LSMTree.Internal.Merge
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.MergeSchedule
import           Database.LSMTree.Internal.MergingRun
import           Database.LSMTree.Internal.MergingTree
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
import           Database.LSMTree.Internal.Vector.Growing
import           Database.LSMTree.Internal.WriteBuffer
import           Database.LSMTree.Internal.WriteBufferBlobs
import           GHC.Generics
import           KMerge.Heap
import           NoThunks.Class
import           System.FS.API
import           System.FS.BlockIO.API
import           System.FS.IO
import           System.FS.Sim.MockFS
import           Test.QuickCheck (Property, Testable (..), counterexample)
import           Unsafe.Coerce

assertNoThunks :: NoThunks a => a -> b -> b
assertNoThunks x = assert p
  where p = case unsafeNoThunks x of
              Nothing -> True
              Just thunkInfo -> error $ "Assertion failed: found thunk" <> show thunkInfo

propUnsafeNoThunks :: NoThunks a => a -> Property
propUnsafeNoThunks x =
    case unsafeNoThunks x of
      Nothing        -> property True
      Just thunkInfo -> counterexample ("Found thunk " <> show thunkInfo) False

propNoThunks :: NoThunks a => a -> IO Property
propNoThunks x = do
    thunkInfoMay <- noThunks [] x
    pure $ case thunkInfoMay of
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
      => NoThunks (NormalTable m k v b) where
  showTypeOf (_ :: Proxy (NormalTable m k v b)) = "NormalTable"
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
deriving via AllowThunksIn '["tableSession"] (Table m h)
    instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
          => NoThunks (Internal.Table m h)

deriving stock instance Generic (TableState m h)
deriving anyclass instance (NoThunksIOLike m, Typeable m, Typeable h, Typeable (PrimState m))
                        => NoThunks (TableState m h)

deriving stock instance Generic (TableEnv m h)
deriving via AllowThunksIn '["tableSessionEnv"] (TableEnv m h)
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

deriving stock instance Generic TableId
deriving anyclass instance NoThunks TableId

deriving stock instance Generic CursorId
deriving anyclass instance NoThunks CursorId

{-------------------------------------------------------------------------------
  UniqCounter
-------------------------------------------------------------------------------}

deriving stock instance Generic (UniqCounter m)
deriving anyclass instance (NoThunks (PrimVar (PrimState m) Int))
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

deriving stock instance Generic RunParams
deriving anyclass instance NoThunks RunParams

deriving stock instance Generic RunBloomFilterAlloc
deriving anyclass instance NoThunks RunBloomFilterAlloc

deriving stock instance Generic RunDataCaching
deriving anyclass instance NoThunks RunDataCaching

deriving stock instance Generic IndexType
deriving anyclass instance NoThunks IndexType

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
deriving anyclass instance (Typeable (PrimState m), Typeable m, Typeable h)
                        => NoThunks (WriteBufferBlobs m h)

deriving stock instance Generic (FilePointer m)
deriving anyclass instance Typeable (PrimState m)
                        => NoThunks (FilePointer m)

{-------------------------------------------------------------------------------
  Index
-------------------------------------------------------------------------------}

deriving stock instance Generic IndexCompact
deriving anyclass instance NoThunks IndexCompact

deriving stock instance Generic PageNo
deriving anyclass instance NoThunks PageNo

deriving stock instance Generic IndexOrdinary
deriving anyclass instance NoThunks IndexOrdinary

deriving stock instance Generic Index
deriving anyclass instance NoThunks Index

{-------------------------------------------------------------------------------
  MergeSchedule
-------------------------------------------------------------------------------}

deriving stock instance Generic (TableContent m h)
deriving anyclass instance
  ( Typeable m, Typeable (PrimState m), Typeable h
  , NoThunks (StrictMVar m (MergingRunState LevelMergeType m h))
  , NoThunks (StrictMVar m (MergingTreeState m h))
  ) => NoThunks (TableContent m h)

deriving stock instance Generic (LevelsCache m h)
deriving anyclass instance
  (Typeable m, Typeable (PrimState m), Typeable h)
  => NoThunks (LevelsCache m h)

deriving stock instance Generic (Level m h)
deriving anyclass instance
  ( Typeable m, Typeable (PrimState m), Typeable h
  , NoThunks (StrictMVar m (MergingRunState LevelMergeType m h))
  ) => NoThunks (Level m h)

deriving stock instance Generic (IncomingRun m h)
deriving anyclass instance
  ( Typeable m, Typeable (PrimState m), Typeable h
  , NoThunks (StrictMVar m (MergingRunState LevelMergeType m h))
  ) => NoThunks (IncomingRun m h)

deriving stock instance Generic (UnionLevel m h)
deriving anyclass instance
  ( Typeable m, Typeable (PrimState m), Typeable h
  , NoThunks (StrictMVar m (MergingTreeState m h))
  ) => NoThunks (UnionLevel m h)

deriving stock instance Generic MergePolicyForLevel
deriving anyclass instance NoThunks MergePolicyForLevel

deriving stock instance Generic NominalDebt
deriving anyclass instance NoThunks NominalDebt

deriving stock instance Generic NominalCredits
deriving anyclass instance NoThunks NominalCredits

{-------------------------------------------------------------------------------
  MergingRun
-------------------------------------------------------------------------------}

deriving stock instance Generic (MergingRun t m h)
deriving anyclass instance ( Typeable m, Typeable (PrimState m), Typeable h
                           , NoThunks (StrictMVar m (MergingRunState t m h))
                           ) => NoThunks (MergingRun t m h)

deriving stock instance Generic (MergingRunState t m h)
deriving anyclass instance ( Typeable m, Typeable (PrimState m), Typeable h
                           , NoThunks t
                           ) => NoThunks (MergingRunState t m h)

deriving stock instance Generic MergeDebt
deriving anyclass instance NoThunks MergeDebt

deriving stock instance Generic MergeCredits
deriving anyclass instance NoThunks MergeCredits

deriving stock instance Generic (CreditsVar s)
deriving anyclass instance Typeable s => NoThunks (CreditsVar s)

deriving stock instance Generic MergeKnownCompleted
deriving anyclass instance NoThunks MergeKnownCompleted

{-------------------------------------------------------------------------------
  MergingTree
-------------------------------------------------------------------------------}

deriving stock instance Generic (MergingTree m h)
deriving anyclass instance
  ( Typeable m, Typeable (PrimState m), Typeable h
  , NoThunks (StrictMVar m (MergingTreeState m h))
  ) => NoThunks (MergingTree m h)

deriving stock instance Generic (MergingTreeState m h)
deriving anyclass instance
  ( Typeable m, Typeable (PrimState m), Typeable h
  , NoThunks (StrictMVar m (MergingRunState LevelMergeType m h))
  , NoThunks (StrictMVar m (MergingRunState TreeMergeType m h))
  , NoThunks (StrictMVar m (MergingTreeState m h))
  ) => NoThunks (MergingTreeState m h)

deriving stock instance Generic (PendingMerge m h)
deriving anyclass instance
  ( Typeable m, Typeable (PrimState m), Typeable h
  , NoThunks (StrictMVar m (MergingRunState LevelMergeType m h))
  , NoThunks (StrictMVar m (MergingTreeState m h))
  ) => NoThunks (PendingMerge m h)

deriving stock instance Generic (PreExistingRun m h)
deriving anyclass instance
  ( Typeable m, Typeable (PrimState m), Typeable h
  , NoThunks (StrictMVar m (MergingRunState LevelMergeType m h))
  ) => NoThunks (PreExistingRun m h)

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
  IndexAcc
-------------------------------------------------------------------------------}

deriving stock instance Generic (IndexCompactAcc s)
deriving anyclass instance Typeable s
                        => NoThunks (IndexCompactAcc s)

deriving stock instance Generic (SMaybe a)
deriving anyclass instance NoThunks a => NoThunks (SMaybe a)

deriving stock instance Generic (IndexOrdinaryAcc s)
deriving anyclass instance Typeable s
                        => NoThunks (IndexOrdinaryAcc s)

deriving stock instance Generic (IndexAcc s)
deriving anyclass instance Typeable s
                        => NoThunks (IndexAcc s)

{-------------------------------------------------------------------------------
  GrowingVector
-------------------------------------------------------------------------------}

instance (NoThunks a, Typeable s, Typeable a) => NoThunks (GrowingVector s a) where
  showTypeOf (p :: Proxy (GrowingVector s a)) = show $ typeRep p
  wNoThunks ctx
    (GrowingVector (a :: STRef s (VM.MVector s a)) (b :: PrimVar s Int))
    = allNoThunks [
          noThunks ctx b
          -- Check that the STRef is in WHNF
        , noThunks ctx $ OnlyCheckWhnf a
          -- Check that the MVector is in WHNF
        , do
            mvec <- unsafeSTToIO $ readSTRef a
            noThunks ctx' $ OnlyCheckWhnf mvec
          -- Check that the vector elements contain no thunks. The vector
          -- contains undefined elements after the first @n@ elements
        , do
            n <- unsafeSTToIO $ readPrimVar b
            mvec <- unsafeSTToIO $ readSTRef a
            allNoThunks [
                unsafeSTToIO (VM.read mvec i) >>= \x -> noThunks ctx'' x
              | i <- [0..n-1]
              ]
        ]
    where
      ctx' = showTypeOf (Proxy @(STRef s (VM.MVector s a))) : ctx
      ctx'' = showTypeOf (Proxy @(VM.MVector s a)) : ctx'

{-------------------------------------------------------------------------------
  Baler
-------------------------------------------------------------------------------}

deriving stock instance Generic (Baler s)
deriving anyclass instance Typeable s
                        => NoThunks (Baler s)

{-------------------------------------------------------------------------------
  PageAcc
-------------------------------------------------------------------------------}

deriving stock instance Generic (PageAcc s)
deriving anyclass instance Typeable s
                        => NoThunks (PageAcc s)

{-------------------------------------------------------------------------------
  Merge
-------------------------------------------------------------------------------}

deriving stock instance Generic (Merge t m h)
deriving anyclass instance ( Typeable m, Typeable (PrimState m), Typeable h
                           , NoThunks t
                           ) => NoThunks (Merge t m h)

deriving stock instance Generic MergeType
deriving anyclass instance NoThunks MergeType

deriving stock instance Generic LevelMergeType
deriving anyclass instance NoThunks LevelMergeType

deriving stock instance Generic TreeMergeType
deriving anyclass instance NoThunks TreeMergeType

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

-- | Allows thunks in the overflow pages
instance ( Typeable m, Typeable (PrimState m), Typeable h
         ) => NoThunks (Reader.Entry m h) where
  showTypeOf (p :: Proxy (Reader.Entry m h)) = show $ typeRep p
  wNoThunks ctx (Reader.Entry (e :: Entry SerialisedValue (RawBlobRef m h))) = noThunks ctx e
  wNoThunks ctx (EntryOverflow
    (entryPrefix :: Entry SerialisedValue (RawBlobRef m h))
    (page :: RawPage)
    (len :: Word32)
    (overflowPages :: [RawOverflowPage]) ) =
      allNoThunks [
          noThunks ctx entryPrefix
        , noThunks ctx page
        , noThunks ctx len
        , noThunks ctx (OnlyCheckWhnf overflowPages)
        ]

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
deriving anyclass instance (Typeable h, Typeable m, Typeable (PrimState m))
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
  RefCounter
-------------------------------------------------------------------------------}

instance Typeable (PrimState m) => NoThunks (RefCounter m) where
  showTypeOf (_ :: Proxy (RefCounter m)) = "RefCounter"
  wNoThunks ctx
    (RefCounter (a :: PrimVar (PrimState m) Int) (b :: m ()))
    = allNoThunks [
          noThunks ctx a
        , noThunks ctx $ (OnlyCheckWhnfNamed b :: OnlyCheckWhnfNamed "finaliser" (m ()))
        ]

-- Ref constructor not exported, cannot derive Generic, use DeRef instead.
instance (NoThunks obj, Typeable obj) => NoThunks (Ref obj) where
  showTypeOf p@(_ :: Proxy (Ref obj)) = show $ typeRep p
  wNoThunks ctx (DeRef ref) = noThunks ctx ref

deriving stock instance Generic (WeakRef obj)
deriving anyclass instance (NoThunks obj, Typeable obj) => NoThunks (WeakRef obj)

{-------------------------------------------------------------------------------
  kmerge
-------------------------------------------------------------------------------}

instance (NoThunks a, Typeable s, Typeable a) => NoThunks (MutableHeap s a) where
  showTypeOf (p :: Proxy (MutableHeap s a)) = show $ typeRep p
  wNoThunks ctx
    (MH (a :: PrimVar s Int) (b :: SmallMutableArray s a))
    = allNoThunks [
          noThunks ctx a
          -- Check that the array is in WHNF
        , noThunks ctx (OnlyCheckWhnf b)
          -- Check that the array elements contain no thunks. The small array
          -- may contain undefined placeholder values after the first @n@
          -- elements in the array. The very first element of the array can also
          -- be undefined.
        , do
            n <- unsafeSTToIO (readPrimVar a)
            allNoThunks [
                unsafeSTToIO (readSmallArray b i) >>= \x -> noThunks ctx' x
              | i <- [1..n-1]
              ]
        ]
    where
      ctx' = showTypeOf (Proxy @(SmallMutableArray s a)) : ctx

{-------------------------------------------------------------------------------
  IOLike
-------------------------------------------------------------------------------}

-- | 'NoThunks' constraints for IO-like monads
--
-- Some constraints, like @NoThunks (MutVar s a)@ and @NoThunks (StrictTVar m
-- a)@, can not be satisfied for arbitrary @m@\/@s@, and must be instantiated
-- for a concrete @m@\/@s@, like @IO@\/@RealWorld@.
class ( forall a. (NoThunks a, Typeable a) => NoThunks (StrictTVar m a)
      , forall a. (NoThunks a, Typeable a) => NoThunks (StrictMVar m a)
      ) => NoThunksIOLike' m s

instance NoThunksIOLike' IO RealWorld

type NoThunksIOLike m = NoThunksIOLike' m (PrimState m)

instance (NoThunks a, Typeable a) => NoThunks (StrictTVar IO a) where
  showTypeOf (p :: Proxy (StrictTVar IO a)) = show $ typeRep p
  wNoThunks _ctx _var = do
    x <- readTVarIO _var
    noThunks _ctx x

-- TODO: in some cases, strict-mvar functions leave thunks behind, in particular
-- modifyMVarMasked and modifyMVarMasked_. So in some specific cases we evaluate
-- the contents of the MVar to WHNF, and keep checking nothunks from there. See
-- lsm-tree#444.
--
-- TODO: we tried using overlapping instances for @StrictMVar IO a@ and
-- @StrictMVar IO (MergingRunState IO h)@, but the quantified constraint in
-- NoThunksIOLike' will throw a compiler error telling us to mark the instances
-- for StrictMVar as incoherent. Marking them as incoherent makes the tests
-- fail... We are unsure if it can be overcome, but the current casting approach
-- works, so there is no priority to use rewrite this code to use overlapping
-- instances.
instance (NoThunks a, Typeable a) => NoThunks (StrictMVar IO a) where
  showTypeOf (p :: Proxy (StrictMVar IO a)) = show $ typeRep p
  wNoThunks ctx var
    -- TODO: Revisit which of these cases are still needed.
    | Just (Proxy :: Proxy (MergingRunState LevelMergeType IO HandleIO))
        <- gcast (Proxy @a)
    = workAroundCheck
    | Just (Proxy :: Proxy (MergingRunState TreeMergeType IO HandleIO))
        <- gcast (Proxy @a)
    = workAroundCheck
    | Just (Proxy :: Proxy (MergingRunState LevelMergeType IO HandleMock))
        <- gcast (Proxy @a)
    = workAroundCheck
    | Just (Proxy :: Proxy (MergingRunState TreeMergeType IO HandleMock))
        <- gcast (Proxy @a)
    = workAroundCheck
    | otherwise
    = properCheck
    where
      properCheck = do
        x <- readMVar var
        noThunks ctx x

      workAroundCheck = do
        !x <- readMVar var
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

-- TODO: upstream to @nothunks@
deriving via OnlyCheckWhnf (VP.MVector s Word8)
    instance Typeable s => NoThunks (VP.MVector s Word8)

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
