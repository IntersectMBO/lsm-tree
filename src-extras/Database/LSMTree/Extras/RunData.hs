-- | Utilities for generating collections of key\/value pairs, and creating runs
-- from them. Tests and benchmarks should preferably use these utilities instead
-- of (re-)defining their own.
module Database.LSMTree.Extras.RunData (
    -- * Flush runs
    withRun
  , withRuns
  , unsafeFlushAsWriteBuffer
    -- * Serialise write buffers
  , withRunDataAsWriteBuffer
  , withSerialisedWriteBuffer
    -- * RunData
  , RunData (..)
  , SerialisedRunData
  , serialiseRunData
  , simplePath
  , simplePaths
    -- * QuickCheck
  , labelRunData
  , genRunData
  , shrinkRunData
  , liftArbitrary2Map
  , liftShrink2Map
  ) where

import           Control.Exception (bracket, bracket_)
import           Control.Monad
import           Control.RefCount
import           Data.Bifoldable (Bifoldable (bifoldMap))
import           Data.Bifunctor
import           Data.Foldable (for_)
import qualified Data.Map as M
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Database.LSMTree.Extras (showPowersOf10)
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Index (IndexType)
import           Database.LSMTree.Internal.Lookup (ResolveSerialisedValue)
import           Database.LSMTree.Internal.MergeSchedule (addWriteBufferEntries)
import           Database.LSMTree.Internal.Paths
import qualified Database.LSMTree.Internal.Paths as Paths
import           Database.LSMTree.Internal.Run (Run, RunDataCaching (..))
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..),
                     entryWouldFitInPage)
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           Database.LSMTree.Internal.WriteBufferWriter (writeWriteBuffer)
import qualified System.FS.API as FS
import           System.FS.API
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API
import           Test.QuickCheck

{-------------------------------------------------------------------------------
  Flush runs
-------------------------------------------------------------------------------}

-- | Create a temporary 'Run' using 'unsafeFlushAsWriteBuffer'.
withRun ::
     HasFS IO h
  -> HasBlockIO IO h
  -> IndexType
  -> RunFsPaths
  -> SerialisedRunData
  -> (Ref (Run IO h) -> IO a)
  -> IO a
withRun hfs hbio indexType path rd = do
    bracket
      (unsafeFlushAsWriteBuffer hfs hbio indexType path $ serialiseRunData rd)
      releaseRef

{-# INLINABLE withRuns #-}
-- | Create temporary 'Run's using 'unsafeFlushAsWriteBuffer'.
withRuns ::
     Traversable f
  => HasFS IO h
  -> HasBlockIO IO h
  -> IndexType
  -> f (RunFsPaths, SerialisedRunData)
  -> (f (Ref (Run IO h)) -> IO a)
  -> IO a
withRuns hfs hbio indexType xs = do
    bracket
      (forM xs $ \(path, rd) -> unsafeFlushAsWriteBuffer hfs hbio indexType path rd)
      (mapM_ releaseRef)

-- | Flush serialised run data to disk as if it were a write buffer.
--
-- This might leak resources if not run with asynchronous exceptions masked.
-- Use helper functions like 'withRun' or 'withRuns' instead.
--
-- Use of this function should be paired with a 'releaseRef'.
unsafeFlushAsWriteBuffer ::
     HasFS IO h
  -> HasBlockIO IO h
  -> IndexType
  -> RunFsPaths
  -> SerialisedRunData
  -> IO (Ref (Run IO h))
unsafeFlushAsWriteBuffer fs hbio indexType fsPaths (RunData m) = do
    let blobpath = addExtension (runBlobPath fsPaths) ".wb"
    wbblobs <- WBB.new fs blobpath
    wb <- WB.fromMap <$> traverse (traverse (WBB.addBlob fs wbblobs)) m
    run <- Run.fromWriteBuffer fs hbio CacheRunData (RunAllocFixed 10) indexType
                               fsPaths wb wbblobs
    releaseRef wbblobs
    return run

{-------------------------------------------------------------------------------
  Serialise write buffers
-------------------------------------------------------------------------------}

-- | Use 'SerialisedRunData' to 'WriteBuffer' and 'WriteBufferBlobs'.
withRunDataAsWriteBuffer ::
     FS.HasFS IO h
  -> ResolveSerialisedValue
  -> WriteBufferFsPaths
  -> SerialisedRunData
  -> (WB.WriteBuffer -> Ref (WBB.WriteBufferBlobs IO h) -> IO a)
  -> IO a
withRunDataAsWriteBuffer hfs f fsPaths rd action = do
  let es = V.fromList . M.toList $ unRunData rd
  let maxn = NumEntries $ V.length es
  let wbbPath = Paths.writeBufferBlobPath fsPaths
  bracket (WBB.new hfs wbbPath) releaseRef $ \wbb -> do
    (wb, _) <- addWriteBufferEntries hfs f wbb maxn WB.empty es
    action wb wbb

-- | Serialise a 'WriteBuffer' and 'WriteBufferBlobs' to disk and perform an 'IO' action.
withSerialisedWriteBuffer ::
     FS.HasFS IO h
  -> FS.HasBlockIO IO h
  -> WriteBufferFsPaths
  -> WB.WriteBuffer
  -> Ref (WBB.WriteBufferBlobs IO h)
  -> IO a
  -> IO a
withSerialisedWriteBuffer hfs hbio wbPaths wb wbb =
  bracket_ (writeWriteBuffer hfs hbio wbPaths wb wbb) $ do
    for_ [ Paths.writeBufferKOpsPath wbPaths
         , Paths.writeBufferBlobPath wbPaths
         , Paths.writeBufferChecksumsPath wbPaths
         ] $ FS.removeFile hfs

{-------------------------------------------------------------------------------
  RunData
-------------------------------------------------------------------------------}

type SerialisedRunData = RunData SerialisedKey SerialisedValue SerialisedBlob

-- | A collection of arbitrary key\/value pairs that are suitable for creating
-- 'Run's.
--
-- Note: 'b ~ Void' should rule out blobs.
newtype RunData k v b = RunData {
    unRunData :: Map k (Entry v b)
  }
  deriving stock (Show, Eq)

serialiseRunData ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b)
  => RunData k v b -> SerialisedRunData
serialiseRunData rd =
    RunData $
    Map.mapKeys (serialiseKey) $
    Map.map (bimap serialiseValue serialiseBlob) $
    unRunData rd

-- | Create a 'RunFsPaths' using an empty 'FsPath'. The empty path corresponds
-- to the "root" or "mount point" of a 'HasFS' instance.
simplePath :: Int -> RunFsPaths
simplePath n = RunFsPaths (mkFsPath []) (RunNumber n)

-- | Like 'simplePath', but for a list.
simplePaths :: [Int] -> [RunFsPaths]
simplePaths ns = fmap simplePath ns

{-------------------------------------------------------------------------------
  QuickCheck
-------------------------------------------------------------------------------}

labelRunData :: SerialisedRunData -> Property -> Property
labelRunData (RunData m) = tabulate "value size" size . label note
  where
    kops = Map.toList m
    size = map (showPowersOf10 . sizeofValue) vals
    vals = concatMap (bifoldMap pure mempty . snd) kops
    note
      | any (uncurry entryWouldFitInPage) kops = "has large k/op"
      | otherwise = "no large k/op"

instance ( Ord k, Arbitrary k, Arbitrary v, Arbitrary b
         ) => Arbitrary (RunData k v b) where
  arbitrary = genRunData arbitrary arbitrary arbitrary
  shrink = shrinkRunData shrink shrink shrink

genRunData ::
     forall k v b. Ord k
  => Gen k
  -> Gen v
  -> Gen b
  -> Gen (RunData k v b)
genRunData genKey genVal genBlob =
    RunData <$> liftArbitrary2Map genKey (liftArbitrary2 genVal genBlob)

shrinkRunData ::
     Ord k
  => (k -> [k])
  -> (v -> [v])
  -> (b -> [b])
  -> RunData k v b
  -> [RunData k v b]
shrinkRunData shrinkKey shrinkVal shrinkBlob =
      fmap RunData
    . liftShrink2Map shrinkKey (liftShrink2 shrinkVal shrinkBlob)
    . unRunData

-- | We cannot implement 'Arbitrary2' since we have constraints on @k@.
liftArbitrary2Map :: Ord k => Gen k -> Gen v -> Gen (Map k v)
liftArbitrary2Map genk genv = Map.fromList <$> liftArbitrary (liftArbitrary2 genk genv)

-- | We cannot implement 'Arbitrary2' since we have constraints @k@.
liftShrink2Map :: Ord k => (k -> [k]) -> (v -> [v]) -> Map k v -> [Map k v]
liftShrink2Map shrinkk shrinkv m = Map.fromList <$>
    liftShrink (liftShrink2 shrinkk shrinkv) (Map.toList m)
