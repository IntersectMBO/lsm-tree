{-# LANGUAGE NumericUnderscores #-}
{-# LANGUAGE TypeApplications   #-}
{- HLINT ignore "Use camelCase" -}

module Database.LSMTree.Extras (
    -- * Bloom filter construction
    BloomMaker
  , mkBloomST
  , mkBloomST_Monkey
  , mkBloomEasy
    -- * Page residency
  , Bytes
  , oneGB
  , pageSize
  , overhead
  , numEntriesInPage
  , numPages
    -- ** UTxO
  , utxoNumEntries
  , utxoKeySize
  , utxoValueSize
  , utxoNumPages
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

{-------------------------------------------------------------------------------
  Page residency
-------------------------------------------------------------------------------}

type Bytes = Int

oneGB :: Bytes
oneGB = 1024 * 1024 * 1024

-- | The size of a disk page.
pageSize :: Bytes
pageSize = 4096

-- | Per entry in a page: 16 bits for the key index, 16 bits for the value
-- index.
overhead :: Bytes
overhead = 2 + 2

-- | How many key-value entries fit in a single page.
numEntriesInPage :: Bytes -> Bytes -> Int
numEntriesInPage keySize valueSize = floor @Double $
    fromIntegral pageSize / fromIntegral (overhead + keySize + valueSize)

-- | How many pages are needed to store a given number of key-value pairs of a
-- given size.
numPages :: Int -> Bytes -> Bytes -> (Int, Int)
numPages numEntries keySize valueSize =
    ( m
    , ceiling @Double (fromIntegral n / fromIntegral m)
    )
  where
    n = numEntries
    m = numEntriesInPage keySize valueSize

--
-- UTxO
--

utxoNumEntries :: Int
utxoNumEntries = 100_000_000

-- These are not worst-case sizes. For now, I'm ignoring multi-asset values and
-- blobs.
utxoKeySize, utxoValueSize :: Bytes
utxoKeySize   = 32
utxoValueSize = 64

-- | How many pages does it take to fit the stretch target of 100 million UTxOs?
utxoNumPages :: (Int, Int)
utxoNumPages = numPages utxoNumEntries utxoKeySize utxoValueSize
