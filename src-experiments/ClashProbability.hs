{-# LANGUAGE PatternSynonyms #-}

{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module ClashProbability where

import           Control.Exception
import           Data.Bits
import           Data.IntSet (IntSet)
import qualified Data.IntSet as IntSet
import           Data.Word
import           Database.LSMTree.Extras.Random
import           System.Environment (getArgs)
import           System.Exit (exitFailure, exitSuccess)
import           System.Random
import           Text.Printf

-- $setup
-- >>> import Numeric

newtype NumEntries = NumEntries Word64
  deriving stock Show

newtype IndexBitsPerEntry = IndexBitsPerEntry Int
  deriving stock Show

-- | A distribution where each transaction has 1 output.
pattern DistribUniform :: Distrib
pattern DistribUniform = Distrib [1]

-- | Each element in the list is a frequency, describing the distribution of
-- the number of outputs per transaction.
--
-- * The 1st element in the list describes the frequency of transactions with 1 output
-- * The 2nd element in the list describes the frequency of transactions with 2 outputs
-- * And so on ...
newtype Distrib = Distrib [Int]
  deriving stock (Show, Read)

data SerialiseMode =
    Standard
  | XOR
  | Reorder
  deriving stock (Show, Read)

main :: IO ()
main = do
    args <- getArgs
    case args of
      [a1, a2, a3, a4] -> do
        let !n = NumEntries (read a1)
            !b = IndexBitsPerEntry (read a2)
            !d = read a3
            !sm = read a4
        putStrLn $ printf "Running with args: (%s) (%s) (%s) (%s)"
                      (show n) (show b) (show d) (show sm)
        printf "%.10f" $ clashProbability d sm n b
        exitSuccess
      _ -> do
        putStrLn "Wrong usage, pass arguments of the type: Word64 Int Distrib SerialiseMode"
        exitFailure

-- | Compute the probability of clashing values
--
-- >>> let ns = NumEntries <$> repeat 100
-- >>> let bs = IndexBitsPerEntry <$> [0..16]
-- >>> zipWith (clashProbability DistribUniform Standard) ns bs
-- [0.99,0.98,0.96,0.92,0.84,0.69,0.5,0.26,0.15,9.0e-2,3.0e-2,1.0e-2,1.0e-2,0.0,0.0,0.0,0.0]
--
-- >>> zipWith (clashProbability DistribUniform XOR) ns bs
-- [0.99,0.98,0.96,0.92,0.84,0.69,0.5,0.26,0.15,9.0e-2,3.0e-2,1.0e-2,1.0e-2,0.0,0.0,0.0,0.0]
--
-- >>> zipWith (clashProbability (Distrib [0, 1]) Standard) ns bs
-- [0.99,0.98,0.96,0.92,0.84,0.74,0.68,0.59,0.54,0.52,0.5,0.5,0.5,0.5,0.5,0.5,0.5]
--
-- >>> zipWith (clashProbability (Distrib [0, 1]) XOR) ns bs
-- [0.99,0.98,0.96,0.92,0.84,0.74,0.68,0.59,0.54,0.52,0.5,0.5,0.5,0.5,0.5,0.5,0.0]
--
-- >>> zipWith (clashProbability (Distrib [0, 0, 1]) Standard) ns bs
-- [0.99,0.98,0.96,0.92,0.85,0.79,0.75,0.7,0.67,0.66,0.66,0.66,0.66,0.66,0.66,0.66,0.66]
--
-- >>> zipWith (clashProbability (Distrib [0, 0, 1]) XOR) ns bs
-- [0.99,0.98,0.96,0.92,0.85,0.79,0.75,0.7,0.67,0.66,0.66,0.66,0.66,0.66,0.66,0.32,-2.0e-2]
--
-- >>> zipWith (clashProbability (Distrib [2, 10, 5, 3, 2, 1]) Standard) ns bs
-- [0.99,0.98,0.96,0.92,0.86,0.76,0.71,0.67,0.66,0.65,0.65,0.65,0.65,0.65,0.65,0.65,0.65]
--
-- >>> zipWith (clashProbability (Distrib [2, 10, 5, 3, 2, 1]) XOR) ns bs
-- [0.99,0.98,0.96,0.92,0.86,0.76,0.71,0.67,0.66,0.65,0.65,0.65,0.65,0.65,0.6,0.43,0.0]
--
-- >>> let bs = IndexBitsPerEntry <$> [0..16]
-- >>> zipWith (clashProbability (Distrib [2, 10, 5, 3, 2, 1]) Reorder) ns bs
-- [0.99,0.99,0.99,0.99,0.99,0.99,0.99,0.99,0.99,0.99,0.99,0.99,0.99,0.99,0.98,0.97,0.95]
--
-- >>> let bs = IndexBitsPerEntry <$> [17..32]
-- >>> zipWith (clashProbability (Distrib [2, 10, 5, 3, 2, 1]) Reorder) ns bs
-- [0.91,0.82,0.68,0.48,0.3,0.19,6.0e-2,2.0e-2,0.0,0.0,0.0,0.0,0.0,0.0,0.0,0.0]
clashProbability ::
     Distrib
  -> SerialiseMode
  -> NumEntries
  -> IndexBitsPerEntry
  -> Double
clashProbability d sm (NumEntries !n0) (IndexBitsPerEntry !b) =
    go IntSet.empty (checkedFromIntegral n0) (mkStdGen 17)
  where
    go :: IntSet -> Int -> StdGen -> Double
    go !xs !n !g
      | n <= 0
      = fromIntegral (checkedFromIntegral n0 - IntSet.size xs) / fromIntegral n0
      | otherwise
      = let (!x, !g') = uniform g
            (!i, !g'') = frequency freqs g'
            !ys = serialiseToIntSet x i
        in  go (xs `IntSet.union` ys) (n - checkedFromIntegral i - 1) g''


    freqs :: [(Int, StdGen -> (Word8, StdGen))]
    freqs = case d of
        Distrib fs -> [(f, \g -> (i, g)) | (f, i) <- zip fs [0..] ]

    serialiseToIntSet :: Word64 -> Word8 -> IntSet
    !serialiseToIntSet = case sm of
        Standard -> \x _ -> IntSet.singleton (checkedFromIntegral $ topBits b x)
        XOR      -> \x i -> IntSet.fromList [
            checkedFromIntegral $
            topBits b $
            xorTop2Bytes x (checkedFromIntegral i')
          | i' <- [0..i]
          ]
        Reorder -> \x i -> IntSet.fromList [
            checkedFromIntegral $
            topBits b $
            replaceTop2Bytes x (checkedFromIntegral i')
          | i' <- [0..i]
          ]

checkedFromIntegral :: (Integral a, Integral b) => a -> b
checkedFromIntegral x = assert p $ x'
  where
    x' = fromIntegral x
    p  = fromIntegral x' == x

topBits :: Int -> Word64 -> Word64
topBits !n !x
  | n < 0     = error "topBits: n < 0"
  | n > 64    = error "topBits: n > 64"
  | otherwise = x .>>. (64 - n)

-- |
--
-- >>> showHex (replaceTop2Bytes 0x0 0x1111) ""
-- "1111000000000000"
--
-- >>> showHex (replaceTop2Bytes 0x0123_4567_89ab_cdef 0x1111) ""
-- "1111456789abcdef"
replaceTop2Bytes :: Word64 -> Word16 -> Word64
replaceTop2Bytes !x !y = x .&. (oneBits .>>. 16) .|. (fromIntegral y .<<. 48)

-- |
--
-- >>> showHex (xorTop2Bytes 0x0 0x1111) ""
-- "1111000000000000"
--
-- >>> showHex (xorTop2Bytes 0x0101_0000_0000_0000 0x1111) ""
-- "1010000000000000"
xorTop2Bytes :: Word64 -> Word16 -> Word64
xorTop2Bytes !x !y = x `xor` (fromIntegral y .<<. 48)
