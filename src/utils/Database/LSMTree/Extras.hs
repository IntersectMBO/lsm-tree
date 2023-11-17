{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}
{- HLINT ignore "Use camelCase" -}

module Database.LSMTree.Extras (
    -- * Bloom filter construction
    BloomMaker
  , mkBloomST
  , mkBloomST_Monkey
  , mkBloomEasy
  ) where

import           Control.Monad.ST (runST)
import qualified Data.BloomFilter.Easy as Bloom.Easy (easyList)
import           Database.LSMTree.Internal.Run.BloomFilter (Bloom, Hashable)
import qualified Database.LSMTree.Internal.Run.BloomFilter as Bloom

{-------------------------------------------------------------------------------
  Bloom filter construction
-------------------------------------------------------------------------------}

type BloomMaker a = [a] -> Bloom a

-- | Create a bloom filter through the 'MBloom' interface. Tunes the bloom
-- filter using 'suggestSizing'.
mkBloomST :: Hashable a => Double -> BloomMaker a
mkBloomST requestedFPR xs = runST $ do
    b <- Bloom.new (Bloom.cheapHashes numHashFuncs) numBits
    mapM_ (Bloom.insert b) xs
    Bloom.freeze b
  where
    numEntries              = length xs
    (numBits, numHashFuncs) = Bloom.suggestSizing numEntries requestedFPR

-- | Create a bloom filter through the 'MBloom' interface. Tunes the bloom
-- filter a la Monkey.
--
-- === TODO
--
-- The measured FPR exceeds the requested FPR by a number of percentages.
-- Example: @withNewStdGen $ measureApproximateFPR (Proxy @Word64) (mkBloomST'
-- 0.37) 1000000@. I'm unsure why, but I have a number of ideas
--
-- * The FPR (and bits/hash functions) calculations are approximations.
-- * Rounding errors in the Haskell implementation of FPR calculations
-- * The Monkey tuning is incompatible with @bloomfilter@'s /next power of 2/
--   rounding of th ebits.
mkBloomST_Monkey :: Hashable a => Double -> BloomMaker a
mkBloomST_Monkey requestedFPR xs = runST $ do
    b <- Bloom.new (Bloom.cheapHashes numHashFuncs) numBits
    mapM_ (Bloom.insert b) xs
    Bloom.freeze b
  where
    numEntries   = length xs
    numBits      = Bloom.monkeyBits numEntries requestedFPR
    numHashFuncs = Bloom.monkeyHashFuncs numBits numEntries

-- | Create a bloom filter through the "Data.BloomFilter.Easy" interface. Tunes
-- the bloom filter using 'suggestSizing'.
mkBloomEasy :: Hashable a => Double -> BloomMaker a
mkBloomEasy = Bloom.Easy.easyList

