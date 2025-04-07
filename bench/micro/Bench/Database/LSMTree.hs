{-# LANGUAGE OverloadedLists #-}

module Bench.Database.LSMTree (benchmarks) where

import           Control.DeepSeq
import           Control.Exception
import           Control.Tracer
import           Criterion.Main
import           Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as SBS
import           Data.Foldable
import           Data.Functor.Compose
import qualified Data.Vector as V
import           Data.Void
import           Data.Word
import           Database.LSMTree hiding (withTable)
import qualified Database.LSMTree.Common as Common
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Internal.Assertions (fromIntegralChecked)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise.Class
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
benchmarks = bgroup "Bench.Database.LSMTree" [
      benchLargeValueVsSmallValueBlob
    , benchCursorScanVsRangeLookupScan
    , benchInsertBatches
    , benchInsertsVsMupserts
    , benchLookupsInsertsVsMupserts
    , benchLookupInsertsVsLookupMupserts
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
  deriving ResolveValue via ResolveAsFirst V1

instance NFData V1 where
  rnf (V1 x s) = rnf x `seq` rnf s

instance SerialiseValue V1 where
  serialiseValue (V1 x s) = serialiseValue x <> serialiseValue s
  deserialiseValue rb = V1 (deserialiseValue $ RB.take 8 rb) (deserialiseValue $ RB.drop 8 rb)

newtype B1 = B1 Void
  deriving newtype (Show, Eq, Ord, NFData, SerialiseValue)

newtype V2 = V2 Word64
  deriving newtype (Show, Eq, Ord, NFData, SerialiseValue)
  deriving ResolveValue via ResolveAsFirst V2

newtype B2 = B2 ShortByteString
  deriving newtype (Show, Eq, Ord, NFData, SerialiseValue)

newtype V3 = V3 Word64
  deriving newtype (Show, Eq, Ord, Num, NFData, SerialiseValue)

type B3 = Void

-- Simple addition
resolve :: V3 -> V3 -> V3
resolve = (+)

instance ResolveValue V3 where
  resolveValue = resolveDeserialised resolve

benchConfig :: Common.TableConfig
benchConfig = Common.defaultTableConfig {
      Common.confWriteBufferAlloc  = Common.AllocNumEntries (Common.NumEntries 20000)
    , Common.confFencePointerIndex = Common.CompactIndex
    }

{-------------------------------------------------------------------------------
  Large Value vs. Small Value Blob
-------------------------------------------------------------------------------}

benchLargeValueVsSmallValueBlob :: Benchmark
benchLargeValueVsSmallValueBlob =
    env mkEntries $ \es -> bgroup "large-value-vs-small-value-blob" [
        env (mkGrouped (mkV1 es)) $ \ ~(ess, kss) -> bgroup "V1" [
            withEnv ess $ \ ~(_, _, _, _, t :: Table IO K V1 B1) -> do
              bench "lookups-large-value" $ whnfIO $
                V.mapM_ (lookups t) kss
          ]
      , env (mkGrouped (mkV2 es)) $ \ ~(ess, kss) -> bgroup "V2" [
            withEnv ess $ \ ~(_, _, _, _, t :: Table IO K V2 B2) -> do
              bench "lookups-small-value" $ whnfIO $
                V.mapM_ (lookups t) kss
           , withEnv ess $ \ ~(_, _, _, s, t :: Table IO K V2 B2) -> do
              bench "lookups-small-value-blob" $ whnfIO $ do
                V.forM_ kss $ \ks -> do
                  lrs <- lookups t ks
                  retrieveBlobs s (V.fromList $ toList $ Compose lrs)
          ]
      ]
    where
      !initialSize = 80_000
      !batchSize = 250

      customRandomEntries :: Int -> V.Vector (K, Word64, ShortByteString)
      customRandomEntries n = V.unfoldrExactN n f (mkStdGen 17)
        where f !g = let (!k, !g') = uniform g
                    in  ((k, v, b), g')
              -- The exact value does not matter much, so we pick an arbitrary
              -- hardcoded one.
              !v = 138
              -- TODO: tweak size of blob
              !b = SBS.pack [0 | _ <- [1 :: Int .. 1500]]

      mkEntries :: IO (V.Vector (K, Word64, ShortByteString))
      mkEntries = pure $ customRandomEntries initialSize

      mkGrouped ::
           V.Vector (k, v, b)
        -> IO ( V.Vector (V.Vector (k, v, b))
              , V.Vector (V.Vector k) )
      mkGrouped es = pure $
          let ess = vgroupsOfN batchSize es
              kss = V.map (V.map fst3) ess
          in  (ess, kss)

      withEnv inss = envWithCleanup (initialise inss) cleanup

      initialise inss = do
          (tmpDir, hfs, hbio) <- mkFiles
          s <- openSession nullTracer hfs hbio (FS.mkFsPath [])
          t <- new s benchConfig
          V.mapM_ (inserts t) inss
          pure (tmpDir, hfs, hbio, s, t)

      cleanup (tmpDir, hfs, hbio, s, t) = do
          close t
          closeSession s
          cleanupFiles (tmpDir, hfs, hbio)

      mkV1 :: V.Vector (K, Word64, ShortByteString) -> V.Vector (K, V1, Maybe B1)
      mkV1 = V.map (\(k, v, b) -> (k, V1 v b, Nothing))

      mkV2 :: V.Vector (K, Word64, ShortByteString) -> V.Vector (K, V2, Maybe B2)
      mkV2 = V.map (\(k, v, b) -> (k, V2 v, Just $ B2 b))

fst3 :: (a, b, c) -> a
fst3 (x, _, _) = x

{-------------------------------------------------------------------------------
  Cursor Scan vs. Range Lookup Scan
-------------------------------------------------------------------------------}

benchCursorScanVsRangeLookupScan :: Benchmark
benchCursorScanVsRangeLookupScan =
    env mkEntries $ \es ->
      env (mkGrouped es) $ \ ess ->
        withEnv ess $ \ ~(_, _, _, _, t :: Table IO K V2 B2) ->
          bgroup "cursor-scan-vs-range-lookup-scan" [
              bench "cursor-scan-full" $ whnfIO $ do
                withCursor t $ \c -> do
                  readCursor initialSize c
            , bench "cursor-scan-chunked" $ whnfIO $ do
                withCursor t $ \c -> do
                  forM_ ([1 .. numChunks] :: [Int]) $ \_ -> do
                    readCursor readSize c
            , bench "range-scan-full" $ whnfIO $ do
                rangeLookup t (FromToIncluding (K minBound) (K maxBound))
            , bench "range-scan-chunked" $ whnfIO $ do
                forM_ ranges $ \r -> do
                  rangeLookup t r
            ]
    where
      initialSize, batchSize, numChunks :: Int
      !initialSize = 80_000
      !batchSize = 250
      !numChunks = 100

      readSize :: Int
      !readSize = check $ initialSize `div` numChunks
        where
          check x = assert (x * numChunks == initialSize) $ x

      ranges :: [Range K]
      !ranges = check $ force $ [
            FromToExcluding (K $ c * i) (K $ c * (i + 1))
          | i <- [0 .. fromIntegralChecked numChunks - 2]
          ] <> [
            FromToIncluding (K $ c * (fromIntegralChecked numChunks - 1)) (K maxBound)
          ]
        where
          c = fromIntegralChecked (maxBound `div` numChunks)
          check rs = assert (length rs == numChunks) rs


      customRandomEntries :: Int -> V.Vector (K, V2, Maybe B2)
      customRandomEntries n = V.unfoldrExactN n f (mkStdGen 17)
        where f !g = let (!k, !g') = uniform g
                    in  ((k, v, Nothing), g')
              -- The exact value does not matter much, so we pick an arbitrary
              -- hardcoded one.
              !v = V2 138

      mkEntries :: IO (V.Vector (K, V2, Maybe B2))
      mkEntries = pure $ customRandomEntries initialSize

      mkGrouped ::
           V.Vector (k, v, b)
        -> IO (V.Vector (V.Vector (k, v, b)))
      mkGrouped es = pure $ vgroupsOfN batchSize es

      withEnv inss = envWithCleanup (initialise inss) cleanup

      initialise inss = do
          (tmpDir, hfs, hbio) <- mkFiles
          s <- openSession nullTracer hfs hbio (FS.mkFsPath [])
          t <- new s benchConfig
          V.mapM_ (inserts t) inss
          pure (tmpDir, hfs, hbio, s, t)

      cleanup (tmpDir, hfs, hbio, s, t) = do
          close t
          closeSession s
          cleanupFiles (tmpDir, hfs, hbio)


{-------------------------------------------------------------------------------
  Insert Batches
-------------------------------------------------------------------------------}

benchInsertBatches :: Benchmark
benchInsertBatches =
    env genInserts $ \iss ->
      withEnv $ \ ~(_, _, _, _, t :: Table IO K V2 Void) -> do
        bench "insert-batches" $ whnfIO $
          V.mapM_ (inserts t) iss
    where
      !initialSize = 100_000
      !batchSize = 256

      _benchConfig :: Common.TableConfig
      _benchConfig = benchConfig {
          Common.confWriteBufferAlloc = Common.AllocNumEntries (Common.NumEntries 1000)
        }

      randomInserts :: Int -> V.Vector (K, V2, Maybe Void)
      randomInserts n = V.unfoldrExactN n f (mkStdGen 17)
        where f !g = let (!k, !g') = uniform g
                    in  ((k, v, Nothing), g')
              -- The exact value does not matter much, so we pick an arbitrary
              -- hardcoded one.
              !v = V2 17

      genInserts :: IO (V.Vector (V.Vector (K, V2, Maybe Void)))
      genInserts = pure $ vgroupsOfN batchSize $ randomInserts initialSize

      withEnv = envWithCleanup initialise cleanup

      initialise = do
          (tmpDir, hfs, hbio) <- mkFiles
          s <- openSession nullTracer hfs hbio (FS.mkFsPath [])
          t <- new s _benchConfig
          pure (tmpDir, hfs, hbio, s, t)

      cleanup (tmpDir, hfs, hbio, s, t) = do
          close t
          closeSession s
          cleanupFiles (tmpDir, hfs, hbio)

{-------------------------------------------------------------------------------
  Inserts vs. Mupserts
-------------------------------------------------------------------------------}

-- | Compare inserts and mupserts. The logical contents of the resulting
-- database are the same.
benchInsertsVsMupserts :: Benchmark
benchInsertsVsMupserts =
    env (pure $ snd $ randomEntriesGrouped 800_000 250) $ \ess ->
      env (pure $ V.map mkInserts ess) $ \inss ->
        bgroup "inserts-vs-mupserts" [
          bench "inserts" $
            withEmptyTable $ \(_, _, _, _, t) ->
              V.mapM_ (inserts t) inss
        , bench "mupserts" $
            withEmptyTable $ \(_, _, _, _, t) ->
              V.mapM_ (mupserts t) ess
        ]
    where
      withEmptyTable =
          perRunEnvWithCleanup
            (do (tmpDir, hfs, hbio) <- mkFiles
                (s, t) <- mkTable hfs hbio benchConfig
                pure (tmpDir, hfs, hbio, s, t)
            )
            (\(tmpDir, hfs, hbio, s, t) -> do
                cleanupTable (s, t)
                cleanupFiles (tmpDir, hfs, hbio)
            )

{-------------------------------------------------------------------------------
  Lookups plus Inserts vs. Mupserts
-------------------------------------------------------------------------------}

-- | Compare lookups+inserts to mupserts. The former costs 2 LSMT operations,
--  while Mupserts only cost 1 LSMT operation. The number of operations do not
--  directly translate to the number of I\/O operations, but one can assume that
--  lookup+insert is roughly twice as costly as mupsert.
benchLookupsInsertsVsMupserts :: Benchmark
benchLookupsInsertsVsMupserts =
    env (pure $ snd $ randomEntriesGrouped 800_000 250) $ \ess ->
      env (pure $ V.map mkInserts ess) $ \inss ->
        bgroup "lookups-inserts-vs-mupserts" [
          bench "lookups-inserts" $
            withTable inss $ \(_, _, _, _, t) ->
              -- Insert the same keys again, but we sum the existing values in
              -- the table with the values we are going to insert: first lookup
              -- the existing values, sum those with the insert values, then
              -- insert the updated values.
              V.forM_ ess $ \es -> do
                lrs <- lookups t (V.map fst es)
                let ins' = V.zipWith f es lrs
                inserts t ins'
        , bench "mupserts" $
            withTable inss $ \(_, _, _, _, t) ->
              -- Insert the same keys again, but we sum the existing values in
              -- the table with the values we are going to insert: submit
              -- mupserts with the insert values.
              V.forM_ ess $ \es -> mupserts t es
        ]
  where
    f (k, v) = \case
      NotFound          -> (k, v, Nothing)
      Found v'          -> (k, v `resolve` v', Nothing)
      FoundWithBlob _ _ -> error "Unexpected blob found"

    withTable inss = perRunEnvWithCleanup
          -- Make a table and fill it up
          (do (tmpDir, hfs, hbio) <- mkFiles
              (s, t) <- mkTable hfs hbio benchConfig
              V.mapM_ (inserts t) inss
              pure (tmpDir, hfs, hbio, s, t)
          )
          (\(tmpDir, hfs, hbio, s, t) -> do
              cleanupTable (s, t)
              cleanupFiles (tmpDir, hfs, hbio)
          )

{-------------------------------------------------------------------------------
  Lookup Inserts vs. Lookup Mupserts
-------------------------------------------------------------------------------}

-- | Compare lookups after inserts against lookups after mupserts.
benchLookupInsertsVsLookupMupserts :: Benchmark
benchLookupInsertsVsLookupMupserts =
    env (pure $ snd $ randomEntriesGrouped 80_000 250) $ \ess ->
      env (pure $ V.map mkInserts ess) $ \inss ->
        bgroup "lookup-inserts-vs-lookup-mupserts" [
          bench "lookup-inserts" $
            withInsertTable inss $ \(_, _, _, _, t) -> do
                V.forM_ ess $ \es -> lookups t (V.map fst es)
        , bench "lookup-mupserts" $
            withMupsertTable ess $ \(_, _, _, _, t) -> do
                V.forM_ ess $ \es -> lookups t (V.map fst es)
        ]
  where
    withInsertTable inss =
        perRunEnvWithCleanup
          -- Insert the same keys 10 times, where each new insert behaves like
          -- a lookup+insert. This results in a logical database containing
          -- the original keys with the original value *10.
          (do (tmpDir, hfs, hbio) <- mkFiles
              (s, t) <- mkTable hfs hbio benchConfig
              V.forM_ [1..10] $ \(i::Int) -> do
                let inss' = (V.map . V.map) (\(k, v, b) -> (k, fromIntegral i * v, b)) inss
                V.mapM_ (inserts t) inss'
              pure (tmpDir, hfs, hbio, s, t)
          )
          (\(tmpDir, hfs, hbio, s, t) -> do
              cleanupTable (s, t)
              cleanupFiles (tmpDir, hfs, hbio)
          )

    withMupsertTable ess =
        perRunEnvWithCleanup
          -- Mupsert the same key 10 times. The results in a logical database
          -- containing the original keys with the original value *10.
          (do (tmpDir, hfs, hbio) <- mkFiles
              (s, t) <- mkTable hfs hbio benchConfig
              V.forM_ [1..10] $ \(_::Int) ->
                V.mapM_ (mupserts t) ess
              pure (tmpDir, hfs, hbio, s, t)
          )
          (\(tmpDir, hfs, hbio, s, t) -> do
              cleanupTable (s, t)
              cleanupFiles (tmpDir, hfs, hbio)
          )

{-------------------------------------------------------------------------------
  Setup
-------------------------------------------------------------------------------}

-- | Random keys, default values @1@
randomEntries :: Int -> V.Vector (K, V3)
randomEntries n = V.unfoldrExactN n f (mkStdGen 17)
  where f !g = let (!k, !g') = uniform g
               in  ((k, 1), g')

-- | Like 'randomEntries', but also returns groups of size 'm'
randomEntriesGrouped :: Int -> Int -> (V.Vector (K, V3), V.Vector (V.Vector (K, V3)))
randomEntriesGrouped n m =
    let es = randomEntries n
    in  (es, vgroupsOfN m es)

mkInserts :: V.Vector (K, V3) -> V.Vector (K, V3, Maybe B3)
mkInserts = V.map (\(k, v) -> (k, v, Nothing))

mkFiles ::
     IO ( FilePath -- ^ Temporary directory
        , FS.HasFS IO FS.HandleIO
        , FS.HasBlockIO IO FS.HandleIO
        )
mkFiles = do
    sysTmpDir <- getCanonicalTemporaryDirectory
    benchTmpDir <- createTempDirectory sysTmpDir "full"
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

mkTable ::
     FS.HasFS IO FS.HandleIO
  -> FS.HasBlockIO IO FS.HandleIO
  -> TableConfig
  -> IO ( Session IO
        , Table IO K V3 B3
        )
mkTable hfs hbio conf = do
    sesh <- openSession nullTracer hfs hbio (FS.mkFsPath [])
    t <- new sesh conf
    pure (sesh, t)

cleanupTable ::
     ( Session IO
     , Table IO K V3 B3
     )
  -> IO ()
cleanupTable (s, t) = do
    close t
    closeSession s
