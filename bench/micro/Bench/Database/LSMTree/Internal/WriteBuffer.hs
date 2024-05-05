{-# LANGUAGE DeriveAnyClass     #-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE RecordWildCards    #-}

module Bench.Database.LSMTree.Internal.WriteBuffer (benchmarks) where

import           Control.DeepSeq (NFData (..))
import           Criterion.Main (Benchmark, bench, bgroup)
import qualified Criterion.Main as Cr
import           Data.Bifunctor (first)
import qualified Data.ByteString as BS
import qualified Data.List as List
import           Data.Maybe (fromMaybe)
import           Data.Word (Word64)
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Extras.UTxO
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunFsPaths (RunFsPaths (..),
                     activeRunsDir)
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Monoidal as Monoidal
import qualified Database.LSMTree.Normal as Normal
import           GHC.Generics
import           Prelude hiding (getContents)
import           System.Directory (removeDirectoryRecursive)
import qualified System.FS.API as FS
import qualified System.FS.IO as FS
import           System.IO.Temp
import qualified System.Random as R
import           System.Random (StdGen, mkStdGen, uniform, uniformR)

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.WriteBuffer" [
      benchWriteBuffer configWord64
        { name         = "word64-insert-10k"
        , ninserts     = 10_000
        }
    , benchWriteBuffer configWord64
        { name         = "word64-delete-10k"
        , ndeletes     = 10_000
        }
    , benchWriteBuffer configWord64
        { name         = "word64-blob-10k"
        , nblobinserts = 10_000
        }
    , benchWriteBuffer configWord64
        { name         = "word64-mupsert-10k"
        , nmupserts    = 10_000
                         -- TODO: too few collisions to really measure resolution
        , mappendVal   = Just (onDeserialisedValues ((+) @Word64))
        }
      -- different key and value sizes
    , benchWriteBuffer configWord64
        { name         = "insert-large-keys-2k"  -- large keys
        , ninserts     = 2_000
        , randomKey    = first serialiseKey . randomByteStringR (0, 4000)
        }
    , benchWriteBuffer configWord64
        { name         = "insert-mix-2k"  -- small and large values
        , ninserts     = 2_000
        , randomValue  = first serialiseValue . randomByteStringR (0, 8000)
        }
    , benchWriteBuffer configWord64
        { name         = "insert-page-2k"  -- 1 page
        , ninserts     = 2_000
        , randomValue  = first serialiseValue . randomByteStringR (4056, 4056)
        }
    , benchWriteBuffer configWord64
        { name         = "insert-page-plus-byte-2k"  -- 1 page + 1 byte
        , ninserts     = 2_000
        , randomValue  = first serialiseValue . randomByteStringR (4057, 4057)
        }
    , benchWriteBuffer configWord64
        { name         = "insert-huge-2k"  -- 3-5 pages
        , ninserts     = 2_000
        , randomValue  = first serialiseValue . randomByteStringR (10_000, 20_000)
        }
      -- UTxO workload
      -- compare different buffer sizes to see superlinear cost of map insertion
    , benchWriteBuffer configUTxO
        { name         = "utxo-2k"
        , ninserts     = 1_000
        , ndeletes     = 1_000
        }
    , benchWriteBuffer configUTxO
        { name         = "utxo-10k"
        , ninserts     = 5_000
        , ndeletes     = 5_000
        }
    , benchWriteBuffer configUTxO
        { name         = "utxo-50k"
        , ninserts     = 25_000
        , ndeletes     = 25_000
        }
    ]

benchWriteBuffer :: Config -> Benchmark
benchWriteBuffer conf@Config{name} =
    withEnv $ \ ~(_dir, hasFS, kops) ->
      bgroup name [
          bench "insert" $
            Cr.whnf (\kops' -> insert kops') kops
        , Cr.env (pure $ insert kops) $ \wb ->
            bench "flush" $
              Cr.perRunEnvWithCleanup getPaths (const (cleanupPaths hasFS)) $ \p -> do
                !run <- flush hasFS p wb
                Run.removeReference hasFS run
        , bench "insert+flush" $
            -- To make sure the WriteBuffer really gets recomputed on every run,
            -- we'd like to do: `whnfAppIO (kops' -> ...) kops`.
            -- However, we also need per-run cleanup to avoid running out of
            -- disk space. We use `perRunEnvWithCleanup`, which has two issues:
            -- 1. Just as `whnfAppIO` etc., it takes an IO action and returns
            --    `Benchmarkable`, which does not compose. As a workaround, we
            --    thread `kops` through the environment, too.
            -- 2. It forces the result to normal form, which would traverse the
            --    whole run, so we force to WHNF ourselves and just return `()`.
            Cr.perRunEnvWithCleanup
              ((,) kops <$> getPaths)
              (const (cleanupPaths hasFS)) $ \(kops', p) -> do
                !run <- flush hasFS p (insert kops')
                -- Make sure to immediately close runs so we don't run out of
                -- file handles. Ideally this would not be measured, but at
                -- least it's pretty cheap.
                Run.removeReference hasFS run
        ]
  where
    withEnv =
        Cr.envWithCleanup
          (writeBufferEnv conf)
          writeBufferEnvCleanup

    -- We'll remove the files on every run, so we can re-use the same run number.
    getPaths :: IO RunFsPaths
    getPaths = pure (RunFsPaths 0)

    -- Simply remove the whole active directory.
    cleanupPaths :: FS.HasFS IO FS.HandleIO -> IO ()
    cleanupPaths hasFS = FS.removeDirectoryRecursive hasFS activeRunsDir

insert :: InputKOps -> WriteBuffer
insert (NormalInputs kops) =
    List.foldl' (\wb (k, e) -> WB.addEntryNormal k e wb) WB.empty kops
insert (MonoidalInputs kops mappendVal) =
    List.foldl' (\wb (k, e) -> WB.addEntryMonoidal mappendVal k e wb) WB.empty kops

flush :: FS.HasFS IO FS.HandleIO -> RunFsPaths -> WriteBuffer -> IO (Run (FS.Handle (FS.HandleIO)))
flush = Run.fromWriteBuffer

data InputKOps
  = NormalInputs
      ![(SerialisedKey, Normal.Update SerialisedValue SerialisedBlob)]
  | MonoidalInputs
      ![(SerialisedKey, Monoidal.Update SerialisedValue)]
      !Mappend
  deriving stock Generic
  deriving anyclass NFData

type Mappend = SerialisedValue -> SerialisedValue -> SerialisedValue

onDeserialisedValues :: SerialiseValue v => (v -> v -> v) -> Mappend
onDeserialisedValues f x y =
    serialiseValue (f (deserialiseValue x) (deserialiseValue y))

type SerialisedKOp = (SerialisedKey, SerialisedEntry)
type SerialisedEntry = Entry SerialisedValue SerialisedBlob

{-------------------------------------------------------------------------------
  Environments
-------------------------------------------------------------------------------}

-- | Config options describing a benchmarking scenario
data Config = Config {
    -- | Name for the benchmark scenario described by this config.
    name         :: !String
    -- | Number of key\/operation pairs in the run
  , ninserts     :: !Int
  , nblobinserts :: !Int
  , ndeletes     :: !Int
  , nmupserts    :: !Int
  , randomKey    :: Rnd SerialisedKey
  , randomValue  :: Rnd SerialisedValue
  , randomBlob   :: Rnd SerialisedBlob
  , mappendVal   :: Maybe Mappend
  }

type Rnd a = StdGen -> (a, StdGen)

defaultConfig :: Config
defaultConfig = Config {
    name         = "default"
  , ninserts     = 0
  , nblobinserts = 0
  , ndeletes     = 0
  , nmupserts    = 0
  , randomKey    = error "randomKey not implemented"
  , randomValue  = error "randomValue not implemented"
  , randomBlob   = error "randomBlob not implemented"
  , mappendVal   = Nothing
  }

configWord64 :: Config
configWord64 = defaultConfig {
    randomKey    = first serialiseKey . uniform @_ @Word64
  , randomValue  = first serialiseValue . uniform @_ @Word64
  , randomBlob   = first serialiseBlob . randomByteStringR (0, 0x2000)  -- up to 8 kB
  }

configUTxO :: Config
configUTxO = defaultConfig {
    randomKey    = first serialiseKey . uniform @_ @UTxOKey
  , randomValue  = first serialiseValue . uniform @_ @UTxOValue
  }

writeBufferEnv ::
     Config
  -> IO ( FilePath -- ^ Temporary directory
        , FS.HasFS IO FS.HandleIO
        , InputKOps
        )
writeBufferEnv config = do
    sysTmpDir <- getCanonicalTemporaryDirectory
    benchTmpDir <- createTempDirectory sysTmpDir "writeBufferEnv"
    let kops = lookupsEnv config (mkStdGen 17)
    let inputKOps = case mappendVal config of
          Nothing -> NormalInputs (fmap (fmap expectNormal) kops)
          Just f  -> MonoidalInputs (fmap (fmap expectMonoidal) kops) f
    let hasFS = FS.ioHasFS (FS.MountPoint benchTmpDir)
    pure (benchTmpDir, hasFS, inputKOps)
  where
    expectNormal e = fromMaybe (error ("invalid normal update: " <> show e))
                       (entryToUpdateNormal e)
    expectMonoidal e = fromMaybe (error ("invalid monoidal update: " <> show e))
                       (entryToUpdateMonoidal e)

writeBufferEnvCleanup ::
     ( FilePath -- ^ Temporary directory
     , FS.HasFS IO FS.HandleIO
     , kops
     )
  -> IO ()
writeBufferEnvCleanup (tmpDir, _, _) = do
    removeDirectoryRecursive tmpDir

-- | Generate keys and entries to insert into the write buffer.
-- They are already serialised to exclude the cost from the benchmark.
lookupsEnv ::
     Config
  -> StdGen -- ^ RNG
  -> [SerialisedKOp]
lookupsEnv Config {..} = take nentries . List.unfoldr (Just . randomKOp)
  where
    nentries = ninserts + nblobinserts + ndeletes + nmupserts

    randomKOp :: Rnd SerialisedKOp
    randomKOp g = let (!k, !g')  = randomKey g
                      (!e, !g'') = randomEntry g'
                  in  ((k, e), g'')

    randomEntry :: Rnd SerialisedEntry
    randomEntry = frequency
        [ ( ninserts
          , \g -> let (!v, !g') = randomValue g
                  in  (Insert v, g')
          )
        , ( nblobinserts
          , \g -> let (!v, !g') = randomValue g
                      (!b, !g'') = randomBlob g'
                  in  (InsertWithBlob v b, g'')
          )
        , ( ndeletes
          , \g -> (Delete, g)
          )
        , ( nmupserts
          , \g -> let (!v, !g') = randomValue g
                  in  (Mupdate v, g')
          )
        ]

frequency :: [(Int, Rnd a)] -> Rnd a
frequency xs0 g
  | any ((< 0) . fst) xs0 = error "frequency: frequencies must be non-negative"
  | tot == 0              = error "frequency: at least one frequency should be non-zero"
  | otherwise = pick i xs0
 where
  (i, g') = uniformR (1, tot) g

  tot = sum (map fst xs0)

  pick n ((k,x):xs)
    | n <= k    = x g'
    | otherwise = pick (n-k) xs
  pick _ _  = error "frequency: pick used with empty list"

randomByteStringR :: (Int, Int) -> Rnd BS.ByteString
randomByteStringR range g =
    let (!l, !g')  = uniformR range g
    in  R.genByteString l g'
