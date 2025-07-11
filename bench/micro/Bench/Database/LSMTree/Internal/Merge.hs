module Bench.Database.LSMTree.Internal.Merge (benchmarks) where

import           Control.RefCount
import           Criterion.Main (Benchmark, bench, bgroup)
import qualified Criterion.Main as Cr
import           Data.Bifunctor (first)
import qualified Data.BloomFilter.Hash as Hash
import           Data.Foldable (traverse_)
import           Data.IORef
import qualified Data.List as List
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe)
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Extras.Orphans ()
import qualified Database.LSMTree.Extras.Random as R
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Extras.UTxO
import qualified Database.LSMTree.Internal.BloomFilter as Bloom
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Index as Index (IndexType (Compact))
import           Database.LSMTree.Internal.Merge (MergeType (..))
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import qualified Database.LSMTree.Internal.RunAcc as RunAcc
import qualified Database.LSMTree.Internal.RunBuilder as RunBuilder
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
import           Prelude hiding (getContents)
import           System.Directory (removeDirectoryRecursive)
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FS
import qualified System.FS.IO as FS
import           System.IO.Temp
import qualified System.Random as R
import           System.Random (StdGen, mkStdGen, uniform, uniformR)

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Merge" [
      -- various numbers of runs
      benchMerge configWord64
        { name         = "word64-insert-x2"
        , nentries     = totalEntries `splitInto` 2
        , finserts     = 1
        }
    , benchMerge configWord64
        { name         = "word64-insert-x4"
        , nentries     = totalEntries `splitInto` 4
        , finserts     = 1
        }
    , benchMerge configWord64
        { name         = "word64-insert-x7"
        , nentries     = totalEntries `splitInto` 7
        , finserts     = 1
        }
    , benchMerge configWord64
        { name         = "word64-insert-x13"
        , nentries     = totalEntries `splitInto` 13
        , finserts     = 1
        }
      -- different operations
    , benchMerge configWord64
        { name         = "word64-delete-x4"
        , nentries     = totalEntries `splitInto` 4
        , fdeletes     = 1
        }
    , benchMerge configWord64
        { name         = "word64-blob-x4"
        , nentries     = totalEntries `splitInto` 4
        , fblobinserts = 1
        }
    , benchMerge configWord64
        { name         = "word64-mupsert-x4"  -- basically no collisions
        , nentries     = totalEntries `splitInto` 4
        , fmupserts    = 1
        , mergeResolve = Just (onDeserialisedValues ((+) @Word64))
        }
    , benchMerge configWord64
        { name         = "word64-mupsert-collisions-x4"
        , nentries     = totalEntries `splitInto` 4
        , fmupserts    = 1
        , randomKey    = -- each run uses half of the possible keys
                         randomWord64OutOf (totalEntries `div` 2)
        , mergeResolve = Just (onDeserialisedValues ((+) @Word64))
        }
      -- different merge types
    , benchMerge configWord64
        { name         = "word64-mix-collisions-x4-midlevel"
        , nentries     = totalEntries `splitInto` 4
        , finserts     = 1
        , fdeletes     = 1
        , fmupserts    = 1
        , randomKey    = -- each run uses half of the possible keys
                         randomWord64OutOf (totalEntries `div` 2)
        , mergeResolve = Just (onDeserialisedValues ((+) @Word64))
        , mergeType    = MergeTypeMidLevel
        }
    , benchMerge configWord64
        { name         = "word64-mix-collisions-x4-lastlevel"
        , nentries     = totalEntries `splitInto` 4
        , finserts     = 1
        , fdeletes     = 1
        , fmupserts    = 1
        , randomKey    = -- each run uses half of the possible keys
                         randomWord64OutOf (totalEntries `div` 2)
        , mergeResolve = Just (onDeserialisedValues ((+) @Word64))
        , mergeType    = MergeTypeLastLevel
        }
    , benchMerge configWord64
        { name         = "word64-mix-collisions-x4-union"
        , nentries     = totalEntries `splitInto` 4
        , finserts     = 1
        , fdeletes     = 1
        , fmupserts    = 1
        , randomKey    = -- each run uses half of the possible keys
                         randomWord64OutOf (totalEntries `div` 2)
        , mergeResolve = Just (onDeserialisedValues ((+) @Word64))
        , mergeType    = MergeTypeUnion
        }
      -- not writing anything at all
    , benchMerge configWord64
        { name         = "word64-delete-x4-lastlevel"
        , nentries     = totalEntries `splitInto` 4
        , fdeletes     = 1
        , mergeType    = MergeTypeLastLevel
        }
      -- different key and value sizes
    , benchMerge configWord64
        { name         = "insert-large-keys-x4"  -- potentially long keys
        , nentries     = (totalEntries `div` 10) `splitInto` 4
        , finserts     = 1
        , randomKey    = first serialiseKey . R.randomByteStringR (8, 4000)
        }
    , benchMerge configWord64
        { name         = "insert-mixed-vals-x4"  -- potentially long values
        , nentries     = (totalEntries `div` 10) `splitInto` 4
        , finserts     = 1
        , randomValue  = first serialiseValue . R.randomByteStringR (0, 4000)
        }
    , benchMerge configWord64
        { name         = "insert-page-x4"  -- 1 page
        , nentries     = (totalEntries `div` 10) `splitInto` 4
        , finserts     = 1
        , randomValue  = first serialiseValue . R.randomByteStringR (4056, 4056)
        }
    , benchMerge configWord64
        { name         = "insert-page-plus-byte-x4"  -- 1 page + 1 byte
        , nentries     = (totalEntries `div` 10) `splitInto` 4
        , finserts     = 1
        , randomValue  = first serialiseValue . R.randomByteStringR (4057, 4057)
        }
    , benchMerge configWord64
        { name         = "insert-huge-vals-x4"  -- 1-5 pages
        , nentries     = (totalEntries `div` 10) `splitInto` 4
        , finserts     = 1
        , randomValue  = first serialiseValue . R.randomByteStringR (10_000, 20_000)
        }
      -- common UTxO scenarios
    , benchMerge configUTxO
        { name         = "utxo-x4"  -- like tiering merge
        , nentries     = totalEntries `splitInto` 4
        , finserts     = 1
        , fdeletes     = 1
        }
    , benchMerge configUTxO
        { name         = "utxo-x4-uneven"
        , nentries     = totalEntries `distributed` [1, 3, 1.5, 2.5]
        , finserts     = 1
        , fdeletes     = 1
        }
    , benchMerge configUTxO
        { name         = "utxo-x4-lastlevel"
        , nentries     = totalEntries `splitInto` 4
        , finserts     = 1
        , fdeletes     = 1
        , mergeType    = MergeTypeLastLevel
        }
    , benchMerge configUTxO
        { name         = "utxo-x4+1-min-skewed-lastlevel"  -- live levelling merge
        , nentries     = totalEntries `distributed` [1, 1, 1, 1, 4]
        , finserts     = 1
        , fdeletes     = 1
        , mergeType    = MergeTypeLastLevel
        }
    , benchMerge configUTxO
        { name         = "utxo-x4+1-max-skewed-lastlevel"  -- live levelling merge
        , nentries     = totalEntries `distributed` [1, 1, 1, 1, 16]
        , finserts     = 1
        , fdeletes     = 1
        , mergeType    = MergeTypeLastLevel
        }
    , benchMerge configUTxOStaking
        { name         = "utxo-x2-tree-union"  -- binary union merge
        , nentries     = totalEntries `distributed` [4, 1]
        , mergeType    = MergeTypeUnion
        }
    , benchMerge configUTxOStaking
        { name         = "utxo-x10-tree-level"  -- merge a whole table (for union)
        , nentries     = totalEntries `distributed` [ 1, 1, 1
                                                    , 4, 4, 4
                                                    , 16, 16, 16
                                                    , 100  -- last level
                                                    ]
        , mergeType    = MergeTypeLastLevel
        }
    ]
  where
    totalEntries = 50_000

    splitInto :: Int -> Int -> [Int]
    n `splitInto` k = n `distributed` replicate k 1

    distributed :: Int -> [Double] -> [Int]
    n `distributed` weights =
      let total = sum weights
      in [ round (fromIntegral n * w / total)
         | w <- weights
         ]

benchSalt :: Bloom.Salt
benchSalt = 4

runParams :: RunBuilder.RunParams
runParams =
    RunBuilder.RunParams {
      runParamCaching = RunBuilder.CacheRunData,
      runParamAlloc   = RunAcc.RunAllocFixed 10,
      runParamIndex   = Index.Compact
    }

benchMerge :: Config -> Benchmark
benchMerge conf@Config{name} =
    withEnv $ \ ~(_dir, hasFS, hasBlockIO, runs) ->
      bgroup name [
          bench "merge" $
            -- We'd like to do: `whnfAppIO (runs' -> ...) runs`.
            -- However, we also need per-run cleanup to avoid running out of
            -- disk space. We use `perRunEnvWithCleanup`, which has two issues:
            -- 1. Just as `whnfAppIO` etc., it takes an IO action and returns
            --    `Benchmarkable`, which does not compose. As a workaround, we
            --    thread `runs` through the environment, too.
            -- 2. It forces the result to normal form, which would traverse the
            --    whole run, so we force to WHNF ourselves and just return `()`.
            -- 3. It doesn't have access to the run we created in the benchmark,
            --    but the cleanup should not be part of the measurement, as it
            --    includes deleting files. So we smuggle the reference out using
            --    an `IORef`.
            Cr.perRunEnvWithCleanup
              ((runs,) <$> newIORef Nothing)
              (releaseRun . snd) $ \(runs', ref) -> do
                !run <- merge hasFS hasBlockIO conf outputRunPaths runs'
                writeIORef ref $ Just $ releaseRef run
        ]
  where
    withEnv =
        Cr.envWithCleanup
          (mergeEnv conf)
          mergeEnvCleanup

    releaseRun :: IORef (Maybe (IO ())) -> IO ()
    releaseRun ref =
        readIORef ref >>= \case
          Nothing      -> pure ()
          Just release -> release

merge ::
     FS.HasFS IO FS.HandleIO
  -> FS.HasBlockIO IO FS.HandleIO
  -> Config
  -> Run.RunFsPaths
  -> InputRuns
  -> IO (Ref (Run IO FS.HandleIO))
merge fs hbio Config {..} targetPaths runs = do
    let f = fromMaybe const mergeResolve
    m <- fromMaybe (error "empty inputs, no merge created") <$>
      Merge.new fs hbio benchSalt runParams mergeType f targetPaths runs
    Merge.stepsToCompletion m stepSize

fsPath :: FS.FsPath
fsPath = FS.mkFsPath []

outputRunPaths :: Run.RunFsPaths
outputRunPaths = RunFsPaths fsPath (RunNumber 0)

inputRunPathsCounter :: IO (UniqCounter IO)
inputRunPathsCounter = newUniqCounter 1  -- 0 is for output

type InputRuns = V.Vector (Ref (Run IO FS.HandleIO))

onDeserialisedValues ::
     SerialiseValue v => (v -> v -> v) -> ResolveSerialisedValue
onDeserialisedValues f x y =
    serialiseValue (f (deserialiseValue x) (deserialiseValue y))

{-------------------------------------------------------------------------------
  Environments
-------------------------------------------------------------------------------}

-- | Config options describing a benchmarking scenario
data Config = Config {
    -- | Name for the benchmark scenario described by this config.
    name         :: !String
    -- | Number of key\/operation pairs, one for each run.
  , nentries     :: ![Int]
    -- | Frequency of inserts within the key\/op pairs.
  , finserts     :: !Int
    -- | Frequency of inserts with blobs within the key\/op pairs.
  , fblobinserts :: !Int
    -- | Frequency of deletes within the key\/op pairs.
  , fdeletes     :: !Int
    -- | Frequency of mupserts within the key\/op pairs.
  , fmupserts    :: !Int
  , randomKey    :: Rnd SerialisedKey
  , randomValue  :: Rnd SerialisedValue
  , randomBlob   :: Rnd SerialisedBlob
  , mergeType    :: !MergeType
    -- | Needs to be defined when generating mupserts.
  , mergeResolve :: !(Maybe ResolveSerialisedValue)
    -- | Merging is done in chunks of @stepSize@ entries.
  , stepSize     :: !Int
  }

type Rnd a = StdGen -> (a, StdGen)

defaultConfig :: Config
defaultConfig = Config {
    name         = "default"
  , nentries     = []
  , finserts     = 0
  , fblobinserts = 0
  , fdeletes     = 0
  , fmupserts    = 0
  , randomKey    = error "randomKey not implemented"
  , randomValue  = error "randomValue not implemented"
  , randomBlob   = error "randomBlob not implemented"
  , mergeType    = MergeTypeMidLevel
  , mergeResolve = Nothing
  , stepSize     = maxBound  -- by default, just do in one go
  }

configWord64 :: Config
configWord64 = defaultConfig {
    randomKey    = first serialiseKey . uniform @Word64 @_
  , randomValue  = first serialiseValue . uniform @Word64 @_
  , randomBlob   = first serialiseBlob . R.randomByteStringR (0, 0x2000)  -- up to 8 kB
  }

configUTxO :: Config
configUTxO = defaultConfig {
    randomKey    = first serialiseKey . uniform @UTxOKey @_
  , randomValue  = first serialiseValue . uniform @UTxOValue @_
  }

configUTxOStaking :: Config
configUTxOStaking = defaultConfig {
    fmupserts    = 1
  , randomKey    = first serialiseKey . uniform @UTxOKey @_
  , randomValue  = first serialiseValue . uniform @Word64 @_
  , mergeResolve = Just (onDeserialisedValues ((+) @Word64))
  }

mergeEnv ::
     Config
  -> IO ( FilePath -- ^ Temporary directory
        , FS.HasFS IO FS.HandleIO
        , FS.HasBlockIO IO FS.HandleIO
        , InputRuns
        )
mergeEnv config = do
    sysTmpDir <- getCanonicalTemporaryDirectory
    benchTmpDir <- createTempDirectory sysTmpDir "mergeEnv"
    (hasFS, hasBlockIO) <- FS.ioHasBlockIO (FS.MountPoint benchTmpDir) FS.defaultIOCtxParams
    runs <- randomRuns hasFS hasBlockIO config (mkStdGen 17)
    pure (benchTmpDir, hasFS, hasBlockIO, runs)

mergeEnvCleanup ::
     ( FilePath -- ^ Temporary directory
     , FS.HasFS IO FS.HandleIO
     , FS.HasBlockIO IO FS.HandleIO
     , InputRuns
     )
  -> IO ()
mergeEnvCleanup (tmpDir, _hasFS, hasBlockIO, runs) = do
    traverse_ releaseRef runs
    removeDirectoryRecursive tmpDir
    FS.close hasBlockIO

-- | Generate keys and entries to insert into the write buffer.
-- They are already serialised to exclude the cost from the benchmark.
randomRuns ::
     FS.HasFS IO FS.HandleIO
  -> FS.HasBlockIO IO FS.HandleIO
  -> Config
  -> StdGen
  -> IO InputRuns
randomRuns hasFS hasBlockIO config@Config {..} rng0 = do
    counter <- inputRunPathsCounter
    fmap V.fromList $
      mapM (unsafeCreateRun hasFS hasBlockIO benchSalt runParams fsPath counter) $
        zipWith
          (randomRunData config)
          nentries
          (List.unfoldr (Just . R.splitGen) rng0)

-- | Generate keys and entries to insert into the write buffer.
-- They are already serialised to exclude the cost from the benchmark.
randomRunData ::
     Config
  -> Int  -- ^ number of entries
  -> StdGen -- ^ RNG
  -> SerialisedRunData
randomRunData Config {..} runentries g0 =
    RunData . Map.fromList $
    zip
      (R.withoutReplacement g1 runentries randomKey)
      (R.withReplacement g2 runentries randomEntry)
  where
    (g1, g2) = R.splitGen g0

    randomEntry :: Rnd (Entry SerialisedValue SerialisedBlob)
    randomEntry = R.frequency
        [ ( finserts
          , \g -> let (!v, !g') = randomValue g
                  in  (Insert v, g')
          )
        , ( fblobinserts
          , \g -> let (!v, !g') = randomValue g
                      (!b, !g'') = randomBlob g'
                  in  (InsertWithBlob v b, g'')
          )
        , ( fdeletes
          , \g -> (Delete, g)
          )
        , ( fmupserts
          , \g -> let (!v, !g') = randomValue g
                  in  (Upsert v, g')
          )
        ]

-- | @randomWord64OutOf n@ generates one out of @n@ distinct keys, which are
-- uniformly distributed.
-- This can be used to make collisions more likely.
--
-- Make sure to pick an @n@ that is significantly larger than the size of a run!
-- Each run entry needs a distinct key.
randomWord64OutOf :: Int -> Rnd SerialisedKey
randomWord64OutOf possibleKeys =
      first (serialiseKey . Hash.hashSalt64 benchSalt)
    . uniformR (0, fromIntegral possibleKeys :: Word64)
