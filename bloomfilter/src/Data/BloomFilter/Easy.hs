{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE ViewPatterns        #-}
-- | An easy-to-use Bloom filter interface.
module Data.BloomFilter.Easy (
    -- * Easy creation and querying
    Bloom,
    easyList,
    B.elem,
    B.notElem,
    B.length,

    -- * Mutable bloom filter
    MBloom,
    easyNew,
    MB.new,
    MB.insert,
    B.freeze,

    -- ** Example: a spell checker
    -- $example

    -- * Useful defaults for creation
    safeSuggestSizing,
    suggestSizing,
) where

import           Control.Monad.ST (ST)
import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as B
import           Data.BloomFilter.Calc
import           Data.BloomFilter.Hash (Hashable)
import           Data.BloomFilter.Mutable (MBloom)
import qualified Data.BloomFilter.Mutable as MB
import qualified Data.ByteString as SB
import           Data.Word (Word64)

-------------------------------------------------------------------------------
-- Easy interface
-------------------------------------------------------------------------------

-- | Create a Bloom filter with the desired false positive rate and
-- members.  The hash functions used are computed by the @cheapHashes@
-- function from the 'Data.BloomFilter.Hash' module.
easyList :: Hashable a
         => Double              -- ^ desired false positive rate (0 < /ε/ < 1)
         -> [a]                 -- ^ values to populate with
         -> Bloom a
{-# SPECIALIZE easyList :: Double -> [SB.ByteString] -> Bloom SB.ByteString #-}
easyList errRate xs = B.fromList numHashes numBits xs
  where
    capacity = length xs
    (numBits, numHashes)
        | capacity > 0 = suggestSizing capacity errRate
        | otherwise    = (1, 1)

-- | Create a Bloom filter with the desired false positive rate, /ε/
-- and expected maximum size, /n/.
easyNew :: Double    -- ^ desired false positive rate (0 < /ε/ < 1)
        -> Int       -- ^ expected maximum size, /n/
        -> ST s (MBloom s a)
easyNew errRate capacity = MB.new numHashes numBits
  where
    (numBits, numHashes) = suggestSizing capacity errRate

-------------------------------------------------------------------------------
-- Size suggestions
-------------------------------------------------------------------------------

-- | Suggest a good combination of filter size and number of hash
-- functions for a Bloom filter, based on its expected maximum
-- capacity and a desired false positive rate.
--
-- The false positive rate is the rate at which queries against the
-- filter should return 'True' when an element is not actually
-- present.  It should be a fraction between 0 and 1, so a 1% false
-- positive rate is represented by 0.01.
--
-- This function will suggest to use a bloom filter of prime size.
-- These theoretically behave the best.
-- Also it won't suggest to use over 63 hash functions,
-- because CheapHashes work only up to 63 functions.
--
-- Note that while creating bloom filters with extremely small (or
-- even negative) capacity is allowed for convenience, it is often
-- not very useful.
-- This function will always suggest to use at least 61 bits.
--
-- >>> safeSuggestSizing 10000 0.01
-- Right (99317,7)
--
safeSuggestSizing
    :: Int              -- ^ expected maximum capacity
    -> Double           -- ^ desired false positive rate (0 < /e/ < 1)
    -> Either String (Word64, Int)
safeSuggestSizing (fromIntegral -> capacity) errRate
    | capacity <= 0                = Right (61, 1)
    | errRate <= 0 || errRate >= 1 = Left "invalid error rate"
    | otherwise                    = pickSize primes
  where
    bits   :: Double
    hashes :: Int
    (bits, hashes) = minimum
        [ (filterSize capacity errRate k, k')
        | k' <- [1 .. 63]
        , let k = fromIntegral k'
        ]

    pickSize [] = Left "capacity too large"
    pickSize (w:ws)
        | fromIntegral w >= bits = Right (w, hashes)
        | otherwise              = pickSize ws

-- primes from around 2^6 to 2^40, with five primes per "octave",
--
-- * 61, 73, 83, 97, 109
-- * 127, 139, ...
-- * 257, 293, ...
-- * ...
--
-- The third next element is around 1.5 times larger:
-- 97/63 = 1.59; 109/73 = 1.49; 127/83 = 1.52
--
-- The approximate growth rate is 1.14.
--
primes :: [Word64]
primes =
    [61,73,83,97,109,127,139,167,193,223,257,293,337,389,443,509,587,673,773
    ,887,1021,1171,1327,1553,1783,2039,2351,2699,3089,3559,4093,4703,5399,6203
    ,7129,8191,9403,10799,12413,14251,16381,18803,21617,24821,28517,32749
    ,37633,43237,49667,57047,65537,75277,86467,99317,114089,131071,150559
    ,172933,198659,228203,262139,301123,345889,397337,456409,524287,602233
    ,691799,794669,912839,1048573,1204493,1383593,1589333,1825673,2097143
    ,2408993,2767201,3178667,3651341,4194301,4817977,5534413,6357353,7302683
    ,8388593,9635981,11068817,12714749,14605411,16777213,19271957,22137667
    ,25429499,29210821,33554393,38543917,44275331,50858999,58421653,67108859
    ,77087833,88550677,101718013,116843297,134217689,154175663,177101321
    ,203436029,233686637,268435399,308351357,354202703,406872031,467373223
    ,536870909,616702721,708405407,813744131,934746541,1073741789,1233405449
    ,1416810797,1627488229,1869493097,2147483647,2466810893,2833621657
    ,3254976541,3738986131,4294967291,4933621843,5667243317,6509953069
    ,7477972391,8589934583,9867243719,11334486629,13019906153,14955944737
    ,17179869143,19734487471,22668973277,26039812297,29911889569,34359738337
    ,39468974939,45337946581,52079624657,59823779149,68719476731,78937949837
    ,90675893137,104159249321,119647558343,137438953447,157875899707
    ,181351786333,208318498651,239295116717,274877906899,315751799521
    ,362703572681,416636997289,478590233419,549755813881,631503599063
    ,725407145383,833273994643,957180466901,1099511627689
    ]

-- | Behaves as 'safeSuggestSizing', but calls 'error' if given
-- invalid or out-of-range inputs.
suggestSizing :: Int            -- ^ expected maximum capacity
              -> Double         -- ^ desired false positive rate (0 < /e/ < 1)
              -> (Word64, Int)
suggestSizing cap errs = either fatal id (safeSuggestSizing cap errs)
  where fatal = error . ("Data.BloomFilter.Util.suggestSizing: " ++)

-- $example
--
-- This example reads a dictionary file containing one word per line,
-- constructs a Bloom filter with a 1% false positive rate, and
-- spellchecks its standard input.  Like the Unix @spell@ command, it
-- prints each word that it does not recognize.
--
-- @
-- import Data.Maybe (mapMaybe)
-- import qualified Data.BloomFilter.Easy as B
--
-- main = do
--   filt \<- B.'easyList' 0.01 . words \<$> readFile "\/usr\/share\/dict\/words"
--   let check word | B.'B.elem' word filt  = Nothing
--                  | otherwise         = Just word
--   interact (unlines . mapMaybe check . lines)
-- @
