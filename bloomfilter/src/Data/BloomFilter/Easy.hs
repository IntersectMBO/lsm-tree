-- | An easy-to-use Bloom filter interface.
module Data.BloomFilter.Easy (
    -- * Easy creation and querying
    Bloom,
    easyList,
    B.elem,
    B.notElem,
    B.size,

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
{-# SPECIALISE easyList :: Double -> [SB.ByteString] -> Bloom SB.ByteString #-}
easyList errRate xs =
    B.fromList (suggestSizing capacity errRate) xs
  where
    capacity = length xs

-- | Create a Bloom filter with the desired false positive rate, /ε/
-- and expected maximum size, /n/.
easyNew :: Double    -- ^ desired false positive rate (0 < /ε/ < 1)
        -> Int       -- ^ expected maximum size, /n/
        -> ST s (MBloom s a)
easyNew errRate capacity =
    MB.new (suggestSizing capacity errRate)

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
safeSuggestSizing ::
       Int              -- ^ expected maximum capacity
    -> Double           -- ^ desired false positive rate (0 < /e/ < 1)
    -> Either String BloomSize
safeSuggestSizing capacity errRate
    | capacity <= 0 = Right BloomSize {
                         bloomNumBits   = 1,
                         bloomNumHashes = 1
                      }
    | errRate <= 0 ||
      errRate >= 1  = Left "invalid error rate"
    | otherwise     = Right $ bloomSizeForPolicy (bloomPolicyForFPR errRate)
                                                 capacity

-- | Behaves as 'safeSuggestSizing', but calls 'error' if given
-- invalid or out-of-range inputs.
suggestSizing :: Int            -- ^ expected maximum capacity
              -> Double         -- ^ desired false positive rate (0 < /e/ < 1)
              -> BloomSize
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
