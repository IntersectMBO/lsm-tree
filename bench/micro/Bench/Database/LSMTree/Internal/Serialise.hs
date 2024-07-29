module Bench.Database.LSMTree.Internal.Serialise (
    benchmarks
  ) where

import           Criterion.Main
import           Database.LSMTree.Extras.UTxO
import           Database.LSMTree.Internal.Serialise.Class
import           System.Random

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Serialise" [
      env (pure $ fst $ uniform (mkStdGen 12)) $ \(k :: UTxOKey) ->
        bgroup "UTxOKey" [
            bench "serialiseKey" $ whnf serialiseKey k
          , bench "serialiseKeyRoundtrip" $ whnf serialiseKeyRoundtrip k
          ]
    ]

serialiseKeyRoundtrip :: SerialiseKey k => k -> k
serialiseKeyRoundtrip k = deserialiseKey (serialiseKey k)
