{-# LANGUAGE OverloadedLists #-}

module Bench.Database.LSMTree.Monoidal (benchmarks) where

import           Control.DeepSeq
import           Criterion.Main
import qualified Data.Vector as V
import           Data.Void
import           Data.Word
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Monoidal (resolveDeserialised)
import qualified Database.LSMTree.Monoidal as Monoidal
import qualified Database.LSMTree.Normal as Normal
import           GHC.Generics
import           Prelude hiding (getContents)
import           System.Directory (removeDirectoryRecursive)
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FS
import qualified System.FS.IO as FS
import           System.IO.Temp
import           System.Random

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Monoidal" [
      benchInsertsVsMupserts
    , benchLookupsInsertsVsMupserts
    , benchNormalResolveVsMonoidalResolve
    ]

{-------------------------------------------------------------------------------
  Types
-------------------------------------------------------------------------------}

newtype K = K Word64
  deriving stock Generic
  deriving newtype (Show, Eq, Ord, Num, NFData, SerialiseKey)
  deriving anyclass Uniform

newtype V = V Word64
  deriving newtype (Show, Eq, Ord, Num, NFData, SerialiseValue)

type B = Void

-- Simple addition
resolve :: V -> V -> V
resolve = (+)

instance Monoidal.ResolveValue V where
  resolveValue = resolveDeserialised resolve

benchConfig :: Normal.TableConfig -- or, equivalently, Monoidal.TableConfig
benchConfig = Normal.TableConfig {
      confMergePolicy = Normal.MergePolicyLazyLevelling
    , confSizeRatio = Normal.Four
    , confWriteBufferAlloc = Normal.AllocNumEntries (Normal.NumEntries 20000)
    , confBloomFilterAlloc = Normal.AllocFixed 10
    , confDiskCachePolicy = Normal.DiskCacheAll
    }

{-------------------------------------------------------------------------------
  Benchmarks
-------------------------------------------------------------------------------}

-- | Compare normal inserts, monoidal inserts, and monoidal upserts. The logical
-- contents of the resulting database are the same.
benchInsertsVsMupserts :: Benchmark
benchInsertsVsMupserts =
    env (pure $ snd $ randomEntriesGrouped 800_000 250) $ \ess -> bgroup "inserts-vs-mupserts" [
        env (pure $ V.map mkNormalInserts ess) $ \inss -> bench "normal-inserts" $
          withEmptyNormalTable $ \(_, _, _, _, t) ->
            V.mapM_ (flip Normal.inserts t) inss
      , env (pure $ V.map mkMonoidalInserts ess) $ \inss -> bench "monoidal-inserts" $
          withEmptyMonoidalTable $ \(_, _, _, _, t) ->
            V.mapM_ (flip Monoidal.inserts t) inss
      , env (pure $ V.map mkMonoidalMupserts ess) $ \mupss -> bench "monoidal-mupserts" $
          withEmptyMonoidalTable $ \(_, _, _, _, t) ->
            V.mapM_ (flip Monoidal.mupserts t) mupss
      ]
    where
      withEmptyNormalTable =
          perRunEnvWithCleanup
            (do (tmpDir, hfs, hbio) <- mkFiles
                (s, t) <- mkNormalTable hfs hbio benchConfig
                pure (tmpDir, hfs, hbio, s, t)
            )
            (\(tmpDir, hfs, hbio, s, t) -> do
                cleanupNormalTable (s, t)
                cleanupFiles (tmpDir, hfs, hbio)
            )

      withEmptyMonoidalTable =
          perRunEnvWithCleanup
            (do (tmpDir, hfs, hbio) <- mkFiles
                (s, t) <- mkMonoidalTable hfs hbio benchConfig
                pure (tmpDir, hfs, hbio, s, t)
            )
            (\(tmpDir, hfs, hbio, s, t) -> do
                cleanupMonoidalTable (s, t)
                cleanupFiles (tmpDir, hfs, hbio)
            )

-- | Compare monoidal lookups+inserts to monoidal mupserts. The former costs 2
-- LSMT operations, while Mupserts only cost 1 LSMT operation. The number of
-- operations do not directly translate to the number of I\/O operations, but
-- one can assume that lookup+insert is roughly twice as costly as mupsert.
benchLookupsInsertsVsMupserts :: Benchmark
benchLookupsInsertsVsMupserts =
    env (pure $ snd $ randomEntriesGrouped 800_000 250) $ \ess -> bgroup "lookups-inserts-vs-mupserts" [
        env (pure $ V.map mkMonoidalInserts ess) $ \inss -> bench "lookups-inserts" $
          withMonoidalTable inss $ \(_, _, _, _, t) ->
            -- Insert the same keys again, but we sum the existing values in
            -- the table with the values we are going to insert: first lookup
            -- the existing values, sum those with the insert values, then
            -- insert the updated values.
            V.forM_ inss $ \ins -> do
              lrs <- Monoidal.lookups (V.map fst ins) t
              let ins' = V.zipWith f ins lrs
              Monoidal.inserts ins' t
      , env (pure $ V.map mkMonoidalMupserts ess) $ \mupss -> bench "mupserts" $
          withMonoidalTable mupss $ \(_, _, _, _, t) ->
            -- Insert the same keys again, but we sum the existing values in
            -- the table with the values we are going to insert: submit
            -- mupserts with the insert values.
            V.forM_ mupss $ \mups -> Monoidal.mupserts mups t
      ]
  where
    f (k, v) = \case
      Monoidal.NotFound -> (k, v)
      Monoidal.Found v' -> (k, v `resolve` v')

    withMonoidalTable inss = perRunEnvWithCleanup
          -- Make a monoidal table and fill it up
          (do (tmpDir, hfs, hbio) <- mkFiles
              (s, t) <- mkMonoidalTable hfs hbio benchConfig
              V.mapM_ (flip Monoidal.inserts t) inss
              pure (tmpDir, hfs, hbio, s, t)
          )
          (\(tmpDir, hfs, hbio, s, t) -> do
              cleanupMonoidalTable (s, t)
              cleanupFiles (tmpDir, hfs, hbio)
          )

-- | Compare normal lookups against monoidal lookups in the scenario where there
-- are multiple values to resolve in the lookup.
benchNormalResolveVsMonoidalResolve :: Benchmark
benchNormalResolveVsMonoidalResolve =
    env (pure $ snd $ randomEntriesGrouped 80_000 250) $ \ess -> bgroup "normal-resolve-vs-monoidal-resolve" [
        env (pure $ V.map mkNormalInserts ess) $ \inss -> bench "normal-lookups" $
          withNormalTable inss $ \(_, _, _, _, t) -> do
              V.forM_ inss $ \ins -> Normal.lookups (V.map fst3 ins) t
      ,  env (pure $ V.map mkMonoidalInserts ess) $ \inss -> bench "monoidal-lookups" $
          withMonoidalTable inss $ \(_, _, _, _, t) -> do
              V.forM_ inss $ \ins -> Monoidal.lookups (V.map fst ins) t
      ]
  where
    fst3 (x,_,_) = x

    withNormalTable inss =
        perRunEnvWithCleanup
          -- Insert the same keys 10 times, where each new insert behaves like
          -- a lookup+insert. This results in a logical database containing
          -- the original keys with the original value *10.
          (do (tmpDir, hfs, hbio) <- mkFiles
              (s, t) <- mkNormalTable hfs hbio benchConfig
              V.forM_ [1..10] $ \(i::Int) -> do
                let inss' = (V.map . V.map) (\(k, v, b) -> (k, fromIntegral i * v, b)) inss
                V.mapM_ (flip Normal.inserts t) inss'
              pure (tmpDir, hfs, hbio, s, t)
          )
          (\(tmpDir, hfs, hbio, s, t) -> do
              cleanupNormalTable (s, t)
              cleanupFiles (tmpDir, hfs, hbio)
          )

    withMonoidalTable inss =
        perRunEnvWithCleanup
          -- Mupsert the same key 10 times. The results in a logical database
          -- containing the original keys with the original value *10.
          (do (tmpDir, hfs, hbio) <- mkFiles
              (s, t) <- mkMonoidalTable hfs hbio benchConfig
              V.forM_ [1..10] $ \(_::Int) ->
                V.mapM_ (flip Monoidal.mupserts t) inss
              pure (tmpDir, hfs, hbio, s, t)
          )
          (\(tmpDir, hfs, hbio, s, t) -> do
              cleanupMonoidalTable (s, t)
              cleanupFiles (tmpDir, hfs, hbio)
          )

{-------------------------------------------------------------------------------
  Setup
-------------------------------------------------------------------------------}

-- | Random keys, default values @1@
randomEntries :: Int -> V.Vector (K, V)
randomEntries n = V.unfoldrExactN n f (mkStdGen 17)
  where f !g = let (!k, !g') = uniform g
               in  ((k, 1), g')

-- | Like 'randomEntries', but also returns groups of size 'm'
randomEntriesGrouped :: Int -> Int -> (V.Vector (K, V), V.Vector (V.Vector (K, V)))
randomEntriesGrouped n m =
    let es = randomEntries n
    in  (es, vgroupsOfN m es)

mkNormalInserts :: V.Vector (K, V) -> V.Vector (K, V, Maybe B)
mkNormalInserts = V.map (\(k, v) -> (k, v, Nothing))

mkMonoidalInserts :: V.Vector (K, V) -> V.Vector (K, V)
mkMonoidalInserts = id

mkMonoidalMupserts :: V.Vector (K, V) -> V.Vector (K, V)
mkMonoidalMupserts = id

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

mkNormalTable ::
     FS.HasFS IO FS.HandleIO
  -> FS.HasBlockIO IO FS.HandleIO
  -> Normal.TableConfig
  -> IO ( Normal.Session IO
        , Normal.TableHandle IO K V B
        )
mkNormalTable hfs hbio conf = do
    sesh <- Normal.openSession hfs hbio (FS.mkFsPath [])
    th <- Normal.new sesh conf
    pure (sesh, th)

cleanupNormalTable ::
     ( Normal.Session IO
     , Normal.TableHandle IO K V B
     )
  -> IO ()
cleanupNormalTable (s, t) = do
    Normal.close t
    Normal.closeSession s

mkMonoidalTable ::
     FS.HasFS IO FS.HandleIO
  -> FS.HasBlockIO IO FS.HandleIO
  -> Monoidal.TableConfig
  -> IO ( Monoidal.Session IO
        , Monoidal.TableHandle IO K V
        )
mkMonoidalTable hfs hbio conf = do
    sesh <- Monoidal.openSession hfs hbio (FS.mkFsPath [])
    th <- Monoidal.new sesh conf
    pure (sesh, th)

cleanupMonoidalTable ::
     ( Monoidal.Session IO
     , Monoidal.TableHandle IO K V
     )
  -> IO ()
cleanupMonoidalTable (s, t) = do
    Monoidal.close t
    Monoidal.closeSession s
