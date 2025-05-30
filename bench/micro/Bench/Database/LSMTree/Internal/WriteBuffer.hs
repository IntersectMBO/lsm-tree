module Bench.Database.LSMTree.Internal.WriteBuffer (benchmarks) where

import           Control.DeepSeq (NFData (..), rwhnf)
import           Control.Exception (assert)
import           Criterion.Main (Benchmark, bench, bgroup)
import qualified Criterion.Main as Cr
import           Data.Bifunctor (first)
import qualified Data.Foldable as Fold
import qualified Data.List as List
import           Data.Maybe (fromMaybe, isJust, isNothing)
import           Data.Word (Word64)
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Extras.Random (frequency, randomByteStringR)
import           Database.LSMTree.Extras.UTxO
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           System.Random (StdGen, mkStdGen, uniform)

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.WriteBuffer" [
      benchWriteBuffer configWord64
        { name         = "word64-insert-10k"
        , nentries     = 10_000
        , finserts     = 1
        }
    , benchWriteBuffer configWord64
        { name         = "word64-delete-10k"
        , nentries     = 10_000
        , fdeletes     = 1
        }
    , benchWriteBuffer configWord64
        { name         = "word64-blob-10k"
        , nentries     = 10_000
        , fblobinserts = 1
        }
    , benchWriteBuffer configWord64
        { name         = "word64-mupsert-10k"
        , nentries     = 10_000
        , fmupserts    = 1
                         -- TODO: too few collisions to really measure resolution
        , resolveVal   = Just (onDeserialisedValues ((+) @Word64))
        }
      -- different key and value sizes
    , benchWriteBuffer configWord64
        { name         = "insert-large-keys-1k"  -- large keys
        , nentries     = 1_000
        , finserts     = 1
        , randomKey    = first serialiseKey . randomByteStringR (6, 4000)
        }
    , benchWriteBuffer configWord64
        { name         = "insert-mixed-vals-1k"  -- small and large values
        , nentries     = 1_000
        , finserts     = 1
        , randomValue  = first serialiseValue . randomByteStringR (0, 8000)
        }
    , benchWriteBuffer configWord64
        { name         = "insert-page-1k"  -- 1 page
        , nentries     = 1_000
        , finserts     = 1
        , randomValue  = first serialiseValue . randomByteStringR (4056, 4056)
        }
    , benchWriteBuffer configWord64
        { name         = "insert-page-plus-byte-1k"  -- 1 page + 1 byte
        , nentries     = 1_000
        , finserts     = 1
        , randomValue  = first serialiseValue . randomByteStringR (4057, 4057)
        }
    , benchWriteBuffer configWord64
        { name         = "insert-huge-vals-1k"  -- 3-5 pages
        , nentries     = 1_000
        , finserts     = 1
        , randomValue  = first serialiseValue . randomByteStringR (10_000, 20_000)
        }
      -- UTxO workload
      -- compare different buffer sizes to see superlinear cost of map insertion
    , benchWriteBuffer configUTxO
        { name         = "utxo-2k"
        , nentries     = 2_000
        , finserts     = 1
        , fdeletes     = 1
        }
    , benchWriteBuffer configUTxO
        { name         = "utxo-10k"
        , nentries     = 10_000
        , finserts     = 1
        , fdeletes     = 1
        }
    , benchWriteBuffer configUTxO
        { name         = "utxo-50k"
        , nentries     = 50_000
        , finserts     = 1
        , fdeletes     = 1
        }
    ]

benchWriteBuffer :: Config -> Benchmark
benchWriteBuffer conf@Config{name} =
    Cr.env (pure (envInputKOps conf)) $ \ kops ->
      bgroup name [
          bench "insert" $
            Cr.whnf (\kops' -> insert kops') kops
          --TODO: re-add I/O tests here:
          -- * writing out blobs during insert
          -- * flushing write buffer to run
        ]

insert :: InputKOps -> WriteBuffer
insert (InputKOps kops resolveVal) =
    Fold.foldl' (\wb (k, e) -> WB.addEntry resolveVal k e wb) WB.empty kops

data InputKOps =
  InputKOps
    [(SerialisedKey, Entry SerialisedValue BlobSpan)]
    ResolveSerialisedValue

instance NFData InputKOps where
  rnf (InputKOps kops resolveVal) = rnf kops `seq` rwhnf resolveVal

onDeserialisedValues ::
     SerialiseValue v => (v -> v -> v) -> ResolveSerialisedValue
onDeserialisedValues f x y =
    serialiseValue (f (deserialiseValue x) (deserialiseValue y))

type SerialisedKOp = (SerialisedKey, SerialisedEntry)
type SerialisedEntry = Entry SerialisedValue BlobSpan

{-------------------------------------------------------------------------------
  Environments
-------------------------------------------------------------------------------}

-- | Config options describing a benchmarking scenario
data Config = Config {
    -- | Name for the benchmark scenario described by this config.
    name         :: !String
    -- | Number of key\/operation pairs in the run
  , nentries     :: !Int
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
    -- | Needs to be defined when generating mupserts.
  , resolveVal   :: !(Maybe ResolveSerialisedValue)
  }

type Rnd a = StdGen -> (a, StdGen)

defaultConfig :: Config
defaultConfig = Config {
    name         = "default"
  , nentries     = 0
  , finserts     = 0
  , fblobinserts = 0
  , fdeletes     = 0
  , fmupserts    = 0
  , randomKey    = error "randomKey not implemented"
  , randomValue  = error "randomValue not implemented"
  , resolveVal   = Nothing
  }

configWord64 :: Config
configWord64 = defaultConfig {
    randomKey    = first serialiseKey . uniform @Word64 @_
  , randomValue  = first serialiseValue . uniform @Word64 @_
  }

configUTxO :: Config
configUTxO = defaultConfig {
    randomKey    = first serialiseKey . uniform @UTxOKey @_
  , randomValue  = first serialiseValue . uniform @UTxOValue @_
  }

envInputKOps :: Config -> InputKOps
envInputKOps config = do
    let kops = randomKOps config (mkStdGen 17)
     in InputKOps kops (fromMaybe const (resolveVal config))

-- | Generate keys and entries to insert into the write buffer.
-- They are already serialised to exclude the cost from the benchmark.
randomKOps ::
     Config
  -> StdGen -- ^ RNG
  -> [SerialisedKOp]
randomKOps Config {..} = take nentries . List.unfoldr (Just . randomKOp) .
    assert (if fmupserts > 0 then isJust resolveVal else isNothing resolveVal)
  where
    randomKOp :: Rnd SerialisedKOp
    randomKOp g = let (!k, !g')  = randomKey g
                      (!e, !g'') = randomEntry g'
                  in  ((k, e), g'')

    randomEntry :: Rnd SerialisedEntry
    randomEntry = frequency
        [ ( finserts
          , \g -> let (!v, !g') = randomValue g
                  in  (Insert v, g')
          )
        , ( fblobinserts
          , \g -> let (!v, !g') = randomValue g
                      (!b, !g'') = randomBlobSpan g'
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

randomBlobSpan :: Rnd BlobSpan
randomBlobSpan !g =
  let (off, !g')  = uniform g
      (len, !g'') = uniform g'
  in (BlobSpan off len, g'')
