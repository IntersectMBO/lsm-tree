module MCG (
    MCG,
    make,
    period,
    next,
    reject,
) where

import           Data.Bits (countLeadingZeros, unsafeShiftR)
import           Data.List (nub)
import           Data.Numbers.Primes (isPrime, primeFactors)
import           Data.Word (Word64)

-- $setup
-- >>> import Data.List (unfoldr, nub)

-- | https://en.wikipedia.org/wiki/Lehmer_random_number_generator
data MCG = MCG { m :: !Word64, a :: !Word64, x :: !Word64 }
  deriving stock Show

-- invariants: m is a prime
--             a is a primitive element of Z_m
--             x is in [1..m-1]

-- | Create a MCG
--
-- >>> make 20 04
-- MCG {m = 23, a = 11, x = 5}
--
-- >>> make 101_000_000 20240429
-- MCG {m = 101000023, a = 197265, x = 20240430}
--
make ::
       Word64  -- ^ a lower bound for the period
    -> Word64  -- ^ initial seed.
    -> MCG
make (max 4 -> period_) seed = MCG m a (mod (seed + 1) m)
  where
    -- start prime search from an odd number larger than asked period.
    m  = findM (if odd period_ then period_ + 2 else period_ + 1)
    m' = m - 1
    qs = nub $ primeFactors m'

    a = findA (guessA m)

    findM p = if isPrime p then p else findM (p + 2)

    -- we find `a` using "brute-force" approach.
    -- luckily, many elements a prime factors, so we don't need to try too hard.
    -- and we only need to check prime factors of m - 1.
    findA x
        | all (\q -> mod (x ^ div m' q) m /= 1) qs
        = x

        | otherwise
        = findA (x + 1)

-- | Period of the MCG.
--
-- Period is usually a bit larger than asked for, we look for the next prime:
--
-- >>> let g = make 9 04
-- >>> period g
-- 10
--
-- >>> take 22 (unfoldr (Just . next) g)
-- [4,7,3,1,0,5,2,6,8,9,4,7,3,1,0,5,2,6,8,9,4,7]
--
period :: MCG -> Word64
period (MCG m _ _) = m - 1

-- | Generate next number.
next :: MCG -> (Word64, MCG)
next (MCG m a x) = (x - 1, MCG m a (mod (x * a) m))

-- | Generate next numbers until one less than given bound is generated.
--
-- Replacing 'next' with @'reject' n@ effectively cuts the period to @n@:
--
-- >>> let g = make 9 04
-- >>> period g
-- 10
--
-- >>> take 22 (unfoldr (Just . reject 9) g)
-- [4,7,3,1,0,5,2,6,8,4,7,3,1,0,5,2,6,8,4,7,3,1]
--
-- if @n@ is close enough to actual period of 'MCG', the rejection ratio
-- is very small.
--
reject :: Word64 -> MCG -> (Word64, MCG)
reject ub g = case next g of
    (x, g') -> if x < ub then (x, g') else reject ub g'

-------------------------------------------------------------------------------
-- guessing some initial a
-------------------------------------------------------------------------------

-- | calculate x -> log2 (x + 1) i.e. approximate how large the number is in bits.
word64Log2m1 :: Word64 -> Int
word64Log2m1 x = 64 - countLeadingZeros x

-- | we guess a such that a*a is larger than m:
-- we shift a number a little.
guessA :: Word64 -> Word64
guessA x = unsafeShiftR x (div (word64Log2m1 x) 3)
