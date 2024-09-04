{-# LANGUAGE OverloadedLists #-}

module Bench.Database.LSMTree.Normal (benchmarks) where

import           Control.DeepSeq
import           Control.Tracer
import           Criterion.Main
import           Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as SBS
{- TODO: enable
import           Data.Foldable
import           Data.Functor.Compose
-}
import qualified Data.Vector as V
import           Data.Void
import           Data.Word
import qualified Database.LSMTree.Common as Common
import           Database.LSMTree.Extras
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise.Class
import qualified Database.LSMTree.Normal as Normal
import           GHC.Generics (Generic)
import           Prelude hiding (getContents)
import           System.Directory (removeDirectoryRecursive)
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FS
import qualified System.FS.IO as FS
import           System.IO.Temp
import           System.Random

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Normal" [
      benchLargeValueVsSmallValueBlob
    ]

{-------------------------------------------------------------------------------
  Types
-------------------------------------------------------------------------------}

newtype K = K Word64
  deriving stock Generic
  deriving newtype (Show, Eq, Ord, Num, NFData, SerialiseKey)
  deriving anyclass Uniform

data V1 = V1 !Word64 !ShortByteString
  deriving stock (Show, Eq, Ord)

instance NFData V1 where
  rnf (V1 x s) = rnf x `seq` rnf s

instance SerialiseValue V1 where
  serialiseValue (V1 x s) = serialiseValue x <> serialiseValue s
  deserialiseValue rb = V1 (deserialiseValue $ RB.take 8 rb) (deserialiseValue $ RB.drop 8 rb)
  deserialiseValueN _ = error "deserialiseValueN: unused"

newtype B1 = B1 Void
  deriving newtype (Show, Eq, Ord, NFData, SerialiseValue)

newtype V2 = V2 Word64
  deriving newtype (Show, Eq, Ord, NFData, SerialiseValue)

newtype B2 = B2 ShortByteString
  deriving newtype (Show, Eq, Ord, NFData, SerialiseValue)

benchConfig :: Common.TableConfig
benchConfig = Common.defaultTableConfig {
      Common.confWriteBufferAlloc = Common.AllocNumEntries (Common.NumEntries 20000)
    }

{-------------------------------------------------------------------------------
  Benchmarks
-------------------------------------------------------------------------------}

benchLargeValueVsSmallValueBlob :: Benchmark
benchLargeValueVsSmallValueBlob =
    env mkEntries $ \es -> bgroup "large-value-vs-small-value-blob" [
        env (mkGrouped (mkV1 es)) $ \ ~(ess, kss) -> bgroup "V1" [
            bench "lookups-large-value" $ withEnv ess $ \(_, _, _, _, t) -> do
                V.mapM_ (flip Normal.lookups t) kss
          ]
      , env (mkGrouped (mkV2 es)) $ \ ~(ess, kss) -> bgroup "V2" [
            bench "lookups-small-value" $ withEnv ess $ \(_, _, _, _, t) -> do
                V.mapM_ (flip Normal.lookups t) kss
            -- TODO: enable
            -- TODO: should lookups return a spare vector containing blob references?
{-           , bench "lookups-small-value-blob" $ withEnv ess $ \(_, _, _, s, t) -> do
                V.forM_ kss $ \ks -> do
                  lrs <- Normal.lookups ks t
                  Normal.retrieveBlobs s (V.fromList $ toList $ Compose lrs) -}
          ]
      ]
    where
      initialSize = 80_000
      batchSize = 250

      mkEntries :: IO (V.Vector (K, Word64, ShortByteString))
      mkEntries = pure $ randomEntries initialSize

      mkGrouped :: V.Vector (k, v, b) -> IO (V.Vector (V.Vector (k, v, b)), V.Vector (V.Vector k))
      mkGrouped es = pure $
          let ess = vgroupsOfN batchSize es
              kss = V.map (V.map fst3) ess
          in  (ess, kss)

      withEnv inss = perRunEnvWithCleanup (initialise inss) cleanup

      initialise inss = do
          (tmpDir, hfs, hbio) <- mkFiles
          s <- Normal.openSession nullTracer hfs hbio (FS.mkFsPath [])
          t <- Normal.new s benchConfig
          V.mapM_ (flip Normal.inserts t) inss
          pure (tmpDir, hfs, hbio, s, t)

      cleanup (tmpDir, hfs, hbio, s, t) = do
          Normal.close t
          Normal.closeSession s
          cleanupFiles (tmpDir, hfs, hbio)

fst3 :: (a, b, c) -> a
fst3 (x, _, _) = x

{-------------------------------------------------------------------------------
  Setup
-------------------------------------------------------------------------------}

randomEntries :: Int -> V.Vector (K, Word64, ShortByteString)
randomEntries n = V.unfoldrExactN n f (mkStdGen 17)
  where f !g = let (!k, !g') = uniform g
               in  ((k, v, b), g')
        -- The exact value does not matter much, so we pick an arbitrary
        -- hardcoded one.
        !v = 138
        -- TODO: tweak size of blob
        !b = SBS.pack [0 | _ <- [1 :: Int .. 1500]]

mkV1 :: V.Vector (K, Word64, ShortByteString) -> V.Vector (K, V1, Maybe B1)
mkV1 = V.map (\(k, v, b) -> (k, V1 v b, Nothing))

mkV2 :: V.Vector (K, Word64, ShortByteString) -> V.Vector (K, V2, Maybe B2)
mkV2 = V.map (\(k, v, b) -> (k, V2 v, Just $ B2 b))

mkFiles ::
     IO ( FilePath -- ^ Temporary directory
        , FS.HasFS IO FS.HandleIO
        , FS.HasBlockIO IO FS.HandleIO
        )
mkFiles = do
    sysTmpDir <- getCanonicalTemporaryDirectory
    benchTmpDir <- createTempDirectory sysTmpDir "monoidal"
    let hfs = FS.ioHasFS (FS.MountPoint benchTmpDir)
    hbio <- FS.ioHasBlockIO hfs FS.defaultIOCtxParams
    pure (benchTmpDir, hfs, hbio)

cleanupFiles ::
     ( FilePath -- ^ Temporary directory
     , FS.HasFS IO FS.HandleIO
     , FS.HasBlockIO IO FS.HandleIO
     )
  -> IO ()
cleanupFiles (tmpDir, _hfs, hbio) = do
    FS.close hbio
    removeDirectoryRecursive tmpDir
