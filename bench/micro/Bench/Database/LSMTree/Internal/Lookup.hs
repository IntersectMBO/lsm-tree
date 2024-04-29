{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE NumericUnderscores         #-}
{-# LANGUAGE RecordWildCards            #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeApplications           #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Bench.Database.LSMTree.Internal.Lookup (benchmarks) where

import           Control.DeepSeq (NFData (..))
import           Control.Exception (assert)
import           Control.Monad
import           Criterion.Main (Benchmark, bench, bgroup, env, envWithCleanup,
                     nfAppIO, whnf, whnfAppIO)
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.ByteString as BS
import qualified Data.List as List
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Primitive as P
import qualified Data.Vector as V
import           Data.WideWord.Word256
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Extras.Random (sampleUniformWithReplacement,
                     uniformWithoutReplacement)
import           Database.LSMTree.Internal.Entry (Entry (..), NumEntries (..))
import           Database.LSMTree.Internal.Lookup (BatchSize (..),
                     bloomQueriesDefault, indexSearches, lookupsInBatches,
                     prepLookups)
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Run (Run)
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.Serialise.Class as Class
import           Database.LSMTree.Internal.Vector (mkPrimVector)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           GHC.Generics (Generic)
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
      benchLookups defaultConfig
    ]

benchLookups :: Config -> Benchmark
benchLookups conf@Config{name} =
    withEnv $ \ ~(_dir, _hasFS, hasBlockIO, bsize, rs, ks) ->
      env ( pure ( V.map Run.runFilter rs
                 , V.map Run.runIndex rs
                 , V.map Run.runKOpsFile rs
                 )
          ) $ \ ~(blooms, indexes, kopsFiles) ->
        bgroup name [
            -- The bloomfilter is queried for all lookup keys. The result is an
            -- unboxed vector, so only use whnf.
            bench "Bloomfilter query" $
              whnf (\ks' -> bloomQueriesDefault blooms ks') ks
            -- The compact index is only searched for (true and false) positive
            -- lookup keys. We use whnf here because the result is an unboxed
            -- vector.
          , env (pure $ bloomQueriesDefault blooms ks) $ \rkixs ->
              bench "Compact index search" $
                whnfAppIO (\ks' -> indexSearches indexes kopsFiles ks' rkixs) ks
            -- All prepped lookups are going to be used eventually so we use
            -- @nf@ on the vector of 'IOOp's. We only evaluate the vector of
            -- indexes to WHNF, because it is an unboxed vector.
          , bench "Lookup preparation in memory" $
              whnfAppIO (\ks' -> prepLookups blooms indexes kopsFiles ks') ks
          , bench "Batched lookups in IO" $
              nfAppIO (\ks' -> lookupsInBatches hasBlockIO bsize resolveV rs blooms indexes kopsFiles ks') ks
          ]
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
  }

defaultConfig :: Config
defaultConfig = Config {
    name         = "2_000_000 entries, 256 positive lookups"
  , nentries     = 2_000_000
  , npos         = 256
  , nneg         = 0
  , ioctxps      = Nothing
  }

lookupsInBatchesEnv ::
     Config
  -> IO ( FilePath -- ^ Temporary directory
        , FS.HasFS IO FS.HandleIO
        , FS.HasBlockIO IO FS.HandleIO
        , BatchSize
        , V.Vector (Run (FS.Handle FS.HandleIO))
        , V.Vector SerialisedKey
        )
lookupsInBatchesEnv Config {..} = do
    sysTmpDir <- getCanonicalTemporaryDirectory
    benchTmpDir <- createTempDirectory sysTmpDir "lookupsInBatchesEnv"
    (storedKeys, lookupKeys) <- lookupsEnv (mkStdGen 17) nentries npos nneg
    let hasFS = FS.ioHasFS (FS.MountPoint benchTmpDir)
        hasBufFS = FS.ioHasBufFS (FS.MountPoint benchTmpDir)
    hasBlockIO <- FS.ioHasBlockIO hasFS hasBufFS ioctxps
    let wb = WB.WB storedKeys
        fsps = Run.RunFsPaths 0
    r <- Run.fromWriteBuffer hasFS fsps wb
    let nentriesReal = unNumEntries $ Run.runNumEntries r
    assert (nentriesReal == nentries) $ pure ()
    let npagesReal = Run.sizeInPages r
    assert (npagesReal * 42 <= nentriesReal) $ pure ()
    assert (npagesReal * 43 >= nentriesReal) $ pure ()
    pure ( benchTmpDir
         , hasFS
         , hasBlockIO
         , BatchSize 64
         , V.singleton r
         , lookupKeys
         )

lookupsInBatchesCleanup ::
     ( FilePath -- ^ Temporary directory
     , FS.HasFS IO FS.HandleIO
     , FS.HasBlockIO IO FS.HandleIO
     , BatchSize
     , V.Vector (Run (FS.Handle FS.HandleIO))
     , V.Vector SerialisedKey
     )
  -> IO ()
lookupsInBatchesCleanup (tmpDir, hasFS, hasBlockIO, _, rs, _) = do
    FS.close hasBlockIO
    forM_ rs $ Run.removeReference hasFS
    removeDirectoryRecursive tmpDir

-- | Generate keys to store and keys to lookup
lookupsEnv ::
     StdGen -- ^ RNG
  -> Int -- ^ Number of stored key\/operation pairs
  -> Int -- ^ Number of positive lookups
  -> Int -- ^ Number of negative lookups
  -> IO ( Map SerialisedKey (Entry SerialisedValue SerialisedBlob)
        , V.Vector (SerialisedKey)
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
    assert (Map.size entries' == nentries) $ pure ()
    assert (length lookups' == npos + nneg) $ pure ()
    pure (entries', lookups')

frequency :: [(Int, StdGen -> (a, StdGen))] -> StdGen -> (a, StdGen)
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
  pick _ _  = error "QuickCheck.pick used with empty list"

-- TODO: tweak distribution
randomEntry :: StdGen -> (Entry UTxOValue UTxOBlob, StdGen)
randomEntry g = frequency [
      (20, \g' -> let (!v, !g'') = uniform g' in (Insert v, g''))
    , (1,  \g' -> let (!v, !g'') = uniform g'
                      (!b, !g''') = genBlob g''
                  in  (InsertWithBlob v b, g'''))
    , (2,  \g' -> let (!v, !g'') = uniform g' in (Mupdate v, g''))
    , (2,  \g' -> (Delete, g'))
    ] g

{-------------------------------------------------------------------------------
  UTxO keys, values and blobs
-------------------------------------------------------------------------------}

-- | A model of a UTxO key (256-bit hash)
newtype UTxOKey = UTxOKey Word256
  deriving stock (Show, Generic)
  deriving newtype ( Eq, Ord, NFData
                   , SerialiseKey
                   , Num, Enum, Real, Integral
                   )
  deriving anyclass Uniform


-- | A model of a UTxO value (512-bit)
data UTxOValue = UTxOValue {
    utxoValueHigh :: !Word256
  , utxoValueLow  :: !Word256
  }
  deriving stock (Show, Eq, Ord, Generic)
  deriving anyclass (Uniform, NFData)

instance SerialiseValue UTxOValue where
  serialiseValue (UTxOValue hi lo) = Class.serialiseValue lo <> Class.serialiseValue hi
  deserialiseValue = error "deserialiseValue: unused"
  deserialiseValueN = error "deserialiseValueN: unused"

instance SerialiseValue Word256 where
  serialiseValue (Word256{word256hi, word256m1, word256m0, word256lo}) =
    RB.RawBytes $ mkPrimVector 0 32 $ P.runByteArray $ do
      ba <- P.newByteArray 32
      P.writeByteArray ba 0 word256lo
      P.writeByteArray ba 1 word256m0
      P.writeByteArray ba 2 word256m1
      P.writeByteArray ba 3 word256hi
      return ba
  deserialiseValue = error "deserialiseValue: unused"
  deserialiseValueN = error "deserialiseValueN: unused"

-- | A blob of arbitrary size
newtype UTxOBlob = UTxOBlob BS.ByteString
  deriving stock (Show, Eq, Ord, Generic)
  deriving anyclass NFData

instance SerialiseValue UTxOBlob where
  serialiseValue (UTxOBlob bs) = Class.serialiseValue bs
  deserialiseValue = error "deserialiseValue: unused"
  deserialiseValueN = error "deserialiseValueN: unused"

genBlob :: RandomGen g => g -> (UTxOBlob, g)
genBlob !g =
  let (!len, !g') = uniformR (0, 0x2000) g
      (!bs, !g'') = genByteString len g'
  in (UTxOBlob bs, g'')
