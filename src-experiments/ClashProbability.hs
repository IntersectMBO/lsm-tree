{-# OPTIONS_GHC -Wno-missing-export-lists #-}

module ClashProbability where

import System.Random
import Data.Bits
import Data.Word
import Data.IntSet (IntSet)
import qualified Data.IntSet as IntSet
import Database.LSMTree.Extras.Random
import Control.Exception
import Text.Printf
import System.Environment (getArgs)
import System.Exit (exitFailure, exitSuccess)

newtype NumEntries = NumEntries Word64
  deriving stock Show

newtype IndexBitsPerEntry = IndexBitsPerEntry Int
  deriving stock Show

data GeneratorMode =
    Uniform
    -- | Each element in the list is a frequency, describing the distribution of
    -- the number of outputs per transaction.
    --
    -- * The 1st element in the list describes the frequency of UTxOs with 1 output
    -- * The 2nd element in the list describes the frequency of UTxOs with 2 outputs
    -- * And so on ...
  | Custom [Int]
  deriving stock (Show, Read)

main :: IO ()
main = do
    args <- getArgs
    case args of
      [a1, a2, a3] -> do
        let !n = NumEntries (read a1)
            !b = IndexBitsPerEntry (read a2)
            !m = read a3
        putStrLn $ printf "Running with args: (%s) (%s) (%s)" (show n) (show b) (show m)
        printf "%.10f" $ clashProbability m (NumEntries $ read a1) (IndexBitsPerEntry $ read a2)
        exitSuccess
      _ -> do
        putStrLn "Wrong usage, pass arguments of the type: Word64 Int GeneratorMode"
        exitFailure

-- | Compute the probability of clashing values
--
-- >>> let ns = NumEntries <$> repeat 100
-- >>> let bs = IndexBitsPerEntry <$> [0..10]
-- >>> zipWith (clashProbability Uniform) ns bs
-- [0.99,0.98,0.96,0.92,0.84,0.69,0.5,0.26,0.15,9.0e-2,3.0e-2]
--
-- >>> let ns = NumEntries <$> repeat 100
-- >>> let bs = IndexBitsPerEntry <$> [0..15]
-- >>> zipWith (clashProbability (Custom [0, 1])) ns bs
-- [0.99,0.98,0.96,0.92,0.84,0.74,0.68,0.59,0.54,0.52,0.5,0.5,0.5,0.5,0.5,0.5]
--
-- >>> let ns = NumEntries <$> repeat 100
-- >>> let bs = IndexBitsPerEntry <$> [0..15]
-- >>> zipWith (clashProbability (Custom [0, 0, 1])) ns bs
-- [0.99,0.98,0.96,0.92,0.85,0.79,0.75,0.7,0.67,0.66,0.66,0.66,0.66,0.66,0.66,0.66]
--
-- >>> let ns = NumEntries <$> repeat 100
-- >>> let bs = IndexBitsPerEntry <$> [0..15]
-- >>> zipWith (clashProbability (Custom [0, 10, 5, 3, 2, 1])) ns bs
-- [0.99,0.98,0.96,0.92,0.86,0.79,0.75,0.71,0.69,0.68,0.68,0.68,0.68,0.68,0.68,0.68]
clashProbability ::
      GeneratorMode
   -> NumEntries
   -> IndexBitsPerEntry
   -> Double
clashProbability mode (NumEntries !n0) (IndexBitsPerEntry !b) =
    let go = case mode of
                Uniform -> go1
                Custom _ -> go2
    in  go IntSet.empty (checkedFromIntegral n0) (mkStdGen 17)
  where
    -- Uniform distribution
    go1 :: IntSet -> Int -> StdGen -> Double
    go1 !xs  0  _ = fromIntegral (checkedFromIntegral n0 - IntSet.size xs) / fromIntegral n0
    go1 !xs !n !g =
        let (!x, !g') = uniform g
            !y = topBits b x
        in  go1 (IntSet.insert (checkedFromIntegral y) xs) (n-1) g'

    -- Custom distribution
    go2 :: IntSet -> Int -> StdGen -> Double
    go2 !xs !n !g
      | n <= 0
      = fromIntegral (checkedFromIntegral n0 - IntSet.size xs) / fromIntegral n0
      | otherwise
      = let (!x, !g') = uniform g
            (!i, !g'') = frequency freqs g'
            !y = topBits b x
        in  go2 (IntSet.insert (checkedFromIntegral y) xs) (n - checkedFromIntegral i) g''

    -- Custom distribution, XOR transaction index
    go3 :: IntSet -> Int -> StdGen -> Double
    go3 !xs !n !g
      | n <= 0
      = fromIntegral (checkedFromIntegral n0 - IntSet.size xs) / fromIntegral n0
      | otherwise
      = let (!x, !g') = uniform g
            (!i, !g'') = frequency freqs g'
            !y = topBits b x
        in  go2 (IntSet.insert (checkedFromIntegral y) xs) (n - checkedFromIntegral i) g''

    freqs :: [(Int, StdGen -> (Word8, StdGen))]
    freqs = case mode of
      Uniform   -> [(1, \g -> (1, g))]
      Custom fs -> [(f, \g -> (i, g)) | (f, i) <- zip fs [1..] ]

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

-- | Replace the last byte in a 'Word64' by a given 'Word8'.
--
-- >>> import Numeric
-- >>> showHex (replaceLastByte 0x93ae_f8a4 0x07) ""
-- "93aef807"
replaceLastByte :: Word64 -> Word8 -> Word64
replaceLastByte !x !y = x .&. (oneBits .<<. 8) .|. fromIntegral y
