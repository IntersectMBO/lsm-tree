{-# OPTIONS_GHC -Wno-orphans #-}

module Bench.Database.LSMTree.Internal.Lookup (benchmarks) where

import           Control.Monad
import           Control.Monad.ST.Strict (stToIO)
import           Control.RefCount
import           Criterion.Main (Benchmark, bench, bgroup, env, envWithCleanup,
                     perRunEnv, perRunEnvWithCleanup, whnf, whnfAppIO)
import           Data.Arena (ArenaManager, closeArena, newArena,
                     newArenaManager, withArena)
import           Data.Bifunctor (Bifunctor (..))
import           Data.ByteString (ByteString)
import qualified Data.List as List
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromMaybe)
import qualified Data.Vector as V
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Extras.Random (frequency, randomByteStringR,
                     sampleUniformWithReplacement, uniformWithoutReplacement)
import           Database.LSMTree.Extras.UTxO
import           Database.LSMTree.Internal.Entry (Entry (..), NumEntries (..))
import qualified Database.LSMTree.Internal.Index as Index (IndexType (Compact))
import           Database.LSMTree.Internal.Lookup (bloomQueries, indexSearches,
                     intraPageLookups, lookupsIO, prepLookups)
import           Database.LSMTree.Internal.Page (getNumPages)
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc (RunBloomFilterAlloc (..))
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           GHC.Exts (RealWorld)
import           GHC.Stack (HasCallStack)
import           Prelude hiding (getContents)
import           System.Directory (removeDirectoryRecursive)
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.IO as FS
import qualified System.FS.IO as FS
import           System.IO.Temp
import           System.Random as R
import           Test.QuickCheck (generate, shuffle)

-- | TODO: add a separate micro-benchmark that includes multi-pages.
benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Lookup" [
      benchLookups Config {
          name         = "2_000_000 entries, 256 positive lookups"
        , nentries     = 2_000_000
        , npos         = 256
        , nneg         = 0
        , ioctxps      = Nothing
        , caching      = Run.CacheRunData
        }
    , benchLookups Config {
          name = "2_000_000 entries, 256 negative lookups"
        , nentries = 2_000_000
        , npos = 0
        , nneg = 256
        , ioctxps = Nothing
        , caching      = Run.CacheRunData
        }
    , benchLookups Config {
          name         = "2_000_000 entries, 256 positive lookups, NoCache"
        , nentries     = 2_000_000
        , npos         = 256
        , nneg         = 0
        , ioctxps      = Nothing
        , caching      = Run.NoCacheRunData
        }
    , benchLookups Config {
          name         = "2_000_000 entries, 256 negative lookups, NoCache"
        , nentries     = 2_000_000
        , npos         = 0
        , nneg         = 256
        , ioctxps      = Nothing
        , caching      = Run.NoCacheRunData
        }
    ]

benchLookups :: Config -> Benchmark
benchLookups conf@Config{name} =
    withEnv $ \ ~(_dir, arenaManager, _hasFS, hasBlockIO, wbblobs, rs, ks) ->
      env ( pure ( V.map (\(DeRef r) -> Run.runFilter   r) rs
                 , V.map (\(DeRef r) -> Run.runIndex    r) rs
                 , V.map (\(DeRef r) -> Run.runKOpsFile r) rs
                 )
          ) $ \ ~(blooms, indexes, kopsFiles) ->
        bgroup name [
            -- The bloomfilter is queried for all lookup keys. The result is an
            -- unboxed vector, so only use @whnf@.
            bench "Bloomfilter query" $
              whnf (\ks' -> bloomQueries blooms ks') ks
            -- The compact index is only searched for (true and false) positive
            -- lookup keys. We use whnf here because the result is
          , env (pure $ bloomQueries blooms ks) $ \rkixs ->
              bench "Compact index search" $
                whnfAppIO (\ks' -> withArena arenaManager $ \arena -> stToIO $ indexSearches arena indexes kopsFiles ks' rkixs) ks
            -- prepLookups combines bloom filter querying and index searching.
            -- The implementation forces the results to WHNF, so we use
            -- whnfAppIO here instead of nfAppIO.
          , bench "Lookup preparation in memory" $
              whnfAppIO (\ks' -> withArena arenaManager $ \arena -> stToIO $ prepLookups arena blooms indexes kopsFiles ks') ks
            -- Submit the IOOps we get from prepLookups to HasBlockIO. We use
            -- perRunEnv because IOOps contain mutable buffers, so we want fresh
            -- ones for each run of the benchmark. We manually evaluate the
            -- result to WHNF since it is unboxed vector.
          , bench "Submit IOOps" $
              -- TODO: here arena is destroyed too soon
              -- but it should be fine for non-debug code
              perRunEnv (withArena arenaManager $ \arena -> stToIO $ prepLookups arena blooms indexes kopsFiles ks) $ \ ~(_rkixs, ioops) -> do
                !_ioress <- FS.submitIO hasBlockIO ioops
                pure ()
            -- When IO result have been collected, intra-page lookups searches
            -- through the raw bytes (representing a disk page) for the lookup
            -- key. Again, we use perRunEnv here because IOOps contain mutable
            -- buffers, so we want fresh ones for each run of the benchmark. The
            -- result is a boxed vector of Maybe Entry, but since the
            -- implementation takes care to evaluate each of the elements, we
            -- only compute WHNF.
          , bench "Perform intra-page lookups" $
              perRunEnvWithCleanup
                ( do arena <- newArena arenaManager
                     (rkixs, ioops) <- stToIO (prepLookups arena blooms indexes kopsFiles ks)
                     ioress <- FS.submitIO hasBlockIO ioops
                     pure (rkixs, ioops, ioress, arena)
                )
                (\(_, _, _, arena) -> closeArena arenaManager arena)
                (\ ~(rkixs, ioops, ioress, _) -> do
                  !_ <- intraPageLookups resolveV WB.empty wbblobs
                                         rs ks rkixs ioops ioress
                  pure ())
            -- The whole shebang: lookup preparation, doing the IO, and then
            -- performing intra-page-lookups. Again, we evaluate the result to
            -- WHNF because it is the same result that intraPageLookups produces
            -- (see above).
          , bench "Lookups in IO" $
              whnfAppIO (\ks' -> lookupsIO hasBlockIO arenaManager resolveV
                                           WB.empty wbblobs
                                           rs blooms indexes kopsFiles ks') ks
          ]
          -- TODO: consider adding benchmarks that also use the write buffer
          -- (then we can't just use 'WB.empty', but must take it from the env)
  where
    withEnv = envWithCleanup
                (lookupsInBatchesEnv conf)
                lookupsInBatchesCleanup
    -- TODO: pick a better value resolve function
    resolveV = \v1 _v2 -> v1

{-------------------------------------------------------------------------------
  Environments
-------------------------------------------------------------------------------}

-- | Config options describing a benchmarking scenario
data Config = Config {
    -- | Name for the benchmark scenario described by this config.
    name         :: !String
    -- | Number of key\/operation pairs in the run
  , nentries
    -- | Number of positive lookups
  , npos         :: !Int
    -- | Number of negative lookups
  , nneg         :: !Int
    -- | Optional parameters for the io-uring context
  , ioctxps      :: !(Maybe FS.IOCtxParams)
    -- | Whether to use or bypass the OS page cache
  , caching      :: !Run.RunDataCaching
  }

lookupsInBatchesEnv ::
     Config
  -> IO ( FilePath -- ^ Temporary directory
        , ArenaManager RealWorld
        , FS.HasFS IO FS.HandleIO
        , FS.HasBlockIO IO FS.HandleIO
        , Ref (WBB.WriteBufferBlobs IO FS.HandleIO)
        , V.Vector (Ref (Run IO FS.HandleIO))
        , V.Vector SerialisedKey
        )
lookupsInBatchesEnv Config {..} = do
    arenaManager <- newArenaManager
    sysTmpDir <- getCanonicalTemporaryDirectory
    benchTmpDir <- createTempDirectory sysTmpDir "lookupsInBatchesEnv"
    (storedKeys, lookupKeys) <- lookupsEnv (mkStdGen 17) nentries npos nneg
    let hasFS = FS.ioHasFS (FS.MountPoint benchTmpDir)
    hasBlockIO <- FS.ioHasBlockIO hasFS (fromMaybe FS.defaultIOCtxParams ioctxps)
    wbblobs <- WBB.new hasFS (FS.mkFsPath ["0.wbblobs"])
    wb <- WB.fromMap <$> traverse (traverse (WBB.addBlob hasFS wbblobs)) storedKeys
    let fsps = RunFsPaths (FS.mkFsPath []) (RunNumber 0)
    r <- Run.fromWriteBuffer hasFS hasBlockIO caching (RunAllocFixed 10) Index.Compact fsps wb wbblobs
    let NumEntries nentriesReal = Run.size r
    assertEqual nentriesReal nentries $ pure ()
    -- 42 to 43 entries per page
    assertEqual (nentriesReal `div` getNumPages (Run.sizeInPages r)) 42 $ pure ()
    pure ( benchTmpDir
         , arenaManager
         , hasFS
         , hasBlockIO
         , wbblobs
         , V.singleton r
         , lookupKeys
         )

lookupsInBatchesCleanup ::
     ( FilePath -- ^ Temporary directory
     , ArenaManager RealWorld
     , FS.HasFS IO FS.HandleIO
     , FS.HasBlockIO IO FS.HandleIO
     , Ref (WBB.WriteBufferBlobs IO FS.HandleIO)
     , V.Vector (Ref (Run IO FS.HandleIO))
     , V.Vector SerialisedKey
     )
  -> IO ()
lookupsInBatchesCleanup (tmpDir, _arenaManager, _hasFS, hasBlockIO, wbblobs, rs, _) = do
    FS.close hasBlockIO
    forM_ rs releaseRef
    releaseRef wbblobs
    removeDirectoryRecursive tmpDir

-- | Generate keys to store and keys to lookup
lookupsEnv ::
     StdGen -- ^ RNG
  -> Int -- ^ Number of stored key\/operation pairs
  -> Int -- ^ Number of positive lookups
  -> Int -- ^ Number of negative lookups
  -> IO ( Map SerialisedKey (Entry SerialisedValue SerialisedBlob)
        , V.Vector SerialisedKey
        )
lookupsEnv g nentries npos nneg = do
    let  (g1, g') = R.split g
         (g2, g3) = R.split g'
    let (keys, negLookups) = splitAt nentries
                           $ uniformWithoutReplacement @UTxOKey g1 (nentries + nneg)
        posLookups         = sampleUniformWithReplacement g2 npos keys
    let values = take nentries $ List.unfoldr (Just . randomEntry) g3
        entries = Map.fromList $ zip keys values
    lookups <- generate $ shuffle (negLookups ++ posLookups)

    let entries' = Map.mapKeys serialiseKey
              $ Map.map (bimap serialiseValue serialiseBlob) entries
        lookups' = V.fromList $ fmap serialiseKey lookups
    assertEqual (Map.size entries') (nentries) $ pure ()
    assertEqual (length lookups') (npos + nneg) $ pure ()
    pure (entries', lookups')

-- TODO: tweak distribution
randomEntry :: StdGen -> (Entry UTxOValue ByteString, StdGen)
randomEntry g = frequency [
      (20, \g' -> let (!v, !g'') = uniform g' in (Insert v, g''))
    , (1,  \g' -> let (!v, !g'') = uniform g'
                      -- The size of the blobs doesn't matter for the benchmark,
                      -- as it only deals with the blob references. So we make
                      -- them tiny to not slow down the setup.
                      (!b, !g''') = randomByteStringR (0, 100) g''
                  in  (InsertWithBlob v b, g'''))
    , (2,  \g' -> let (!v, !g'') = uniform g' in (Mupdate v, g''))
    , (2,  \g' -> (Delete, g'))
    ] g

-- | Assertions on the generated environment should also be checked for release
-- builds, so don't use 'Control.Exception.assert'.
assertEqual :: (HasCallStack, Eq a, Show a) => a -> a -> b -> b
assertEqual x y
  | x == y    = id
  | otherwise = error $ show x ++ " /= " ++ show y
