{-# LANGUAGE ParallelListComp #-}
module Main (main) where

import qualified Data.BloomFilter as B (BitsPerEntry, BloomPolicy, BloomSize,
                     FPR, Hashable, Salt)
import qualified Data.BloomFilter.Blocked as B.Blocked
import qualified Data.BloomFilter.Classic as B.Classic

import           Control.Parallel.Strategies
import           Data.IntSet (IntSet)
import qualified Data.IntSet as IntSet
import           Data.List (unfoldr)
import           Math.Regression.Simple
import           System.Environment (getArgs)
import           System.Exit (exitFailure)
import           System.IO
import           System.Random

import           Prelude hiding (elem)

-- | Write out data files used by gnuplot fpr.plot
main :: IO ()
main = do
    hSetBuffering stdout NoBuffering --for progress reporting

    args <- getArgs
    case args of
      ["Generate"] -> main_generateData
      ["Regression"] -> main_regression
      _   -> do
        putStrLn "Usage: bloomfilter-fpr-calc [Generate|Regression]"
        exitFailure

main_regression :: IO ()
main_regression = do
    s <- readFile "bloomfilter/fpr.blocked.gnuplot.data"
    let parseLine l = case words l of
          [w_xs_blocked, _, w_ys_blocked_actual] ->
            ( read w_xs_blocked, read w_ys_blocked_actual )
          _ -> error "failed parse"
        xs_blocked, ys_blocked_actual :: [Double]
        (xs_blocked, ys_blocked_actual) = unzip $ fmap parseLine $ lines s

    let regressionData     :: [(Double, Double)]
        regressionData      = zip xs_blocked
                                  (map (negate . log) ys_blocked_actual)
        regressionBitsToFPR = quadraticFit (\(x,y)->(x,y)) regressionData
        regressionFPRToBits = quadraticFit (\(x,y)->(y,x)) regressionData
    putStrLn ""
    putStrLn "Blocked bloom filter quadratic regressions:"
    putStrLn "bits independent, FPR dependent:"
    print regressionBitsToFPR
    putStrLn ""
    putStrLn "FPR independent, bits dependent:"
    print regressionFPRToBits

main_generateData :: IO ()
main_generateData = do
    withFile "bloomfilter/fpr.classic.gnuplot.data" WriteMode $ \h -> do
      hSetBuffering h LineBuffering --for incremental output
      mapM_ (\l -> hPutStrLn h l >> putChar '.') $
        [ unwords [show bitsperkey, show y1, show y2]
        | (bitsperkey, _) <- xs_classic
        | y1              <- ys_classic_calc
        | y2              <- ys_classic_actual
        ]
    putStrLn "Wrote bloomfilter/fpr.classic.gnuplot.data"

    withFile "bloomfilter/fpr.blocked.gnuplot.data" WriteMode $ \h -> do
      hSetBuffering h LineBuffering --for incremental output
      mapM_ (\l -> hPutStrLn h l >> putChar '.') $
        [ unwords [show bitsperkey, show y1, show y2]
        | (bitsperkey, _) <- xs_blocked
        | y1              <- ys_blocked_calc
        | y2              <- ys_blocked_actual
        ]
    putStrLn "Wrote bloomfilter/fpr.blocked.gnuplot.data"
  where
    -- x axis values
    xs_classic =
      [ (bitsperkey, g)
      | bitsperkey <- [2,2.3..20]
      , g          <- mkStdGen <$> [1..3]
      ]
      -- We use fewer points for classic, as it's slower and there's less need.

    xs_blocked =
      [ (bitsperkey, g)
      | bitsperkey <- [2,2.2..24]
      , g          <- mkStdGen <$> [1..9]
      ]

    ys_classic_calc, ys_classic_actual,
      ys_blocked_calc, ys_blocked_actual :: [Double]

    ys_classic_calc = ys_calc classicBloomImpl xs_classic
    ys_blocked_calc = ys_calc blockedBloomImpl xs_blocked

    ys_classic_actual = ys_actual classicBloomImpl xs_classic
    ys_blocked_actual = ys_actual blockedBloomImpl xs_blocked

    ys_calc :: BloomImpl b -> [(Double, StdGen)] -> [Double]
    ys_calc BloomImpl{..} xs =
      [ fpr
      | (bitsperkey, _) <- xs
      , let policy = policyForBits bitsperkey
            fpr    = policyFPR policy
      ]

    ys_actual :: BloomImpl b -> [(Double, StdGen)] -> [Double]
    ys_actual impl@BloomImpl{..} xs =
      withStrategy (parList rseq) -- eval in parallel
      [ fpr
      | (bitsperkey, g) <- xs
      , let policy   = policyForBits bitsperkey
            fpr_est  = policyFPR policy
            nentries = round (1000 * recip fpr_est)
            fpr      = actualFalsePositiveRate impl policy nentries g
      ]
{-
    -- fpr values in the range 1e-1 .. 1e-6
    ys = [ exp (-log_fpr)
         | log_fpr <- [2.3,2.4 .. 13.8] ]

    xs_classic_calc = xs_calc classicBloomImpl
    xs_blocked_calc = xs_calc blockedBloomImpl

    xs_calc BloomImpl{..} =
      [ bits
      | fpr <- ys
      , let policy = policyForFPR fpr
            bits   = policyBits policy
      ]
-}

actualFalsePositiveRate :: BloomImpl bloom
                        -> B.BloomPolicy -> Int -> StdGen -> Double
actualFalsePositiveRate bloomimpl policy n g0 =
    fromIntegral (countFalsePositives bloomimpl policy n g0)
  / fromIntegral n

countFalsePositives :: forall bloom. BloomImpl bloom
                    -> B.BloomPolicy -> Int -> StdGen -> Int
countFalsePositives BloomImpl{..} policy n g0 =
    let (!g01, !g02) = splitGen g0

        -- create a random salt
        (!salt, !g03) = uniform g02

        -- create a bloom filter from n elements from g0
        size  = sizeForPolicy policy n

        xs_b :: bloom Int
        !xs_b = unfold size salt nextElement (g01, 0)

        -- and a set, so we can make sure we don't count true positives
        xs_s :: IntSet
        !xs_s = IntSet.fromList (unfoldr nextElement (g01, 0))

        -- now for a different random sequence (that will mostly not overlap)
        -- count the number of false positives
     in length
          [ ()
          | y <- unfoldr nextElement (g03, 0)
          , y `elem` xs_b                -- Bloom filter reports positive
          , not (y `IntSet.member` xs_s) -- but it is not a true positive
          ]
  where
    nextElement :: (StdGen, Int) -> Maybe (Int, (StdGen, Int))
    nextElement (!g, !i)
      | i >= n    = Nothing
      | otherwise = Just (x, (g', i+1))
        where
          (!x, !g') = uniform g

data BloomImpl bloom = BloomImpl {
       policyForBits :: B.BitsPerEntry -> B.BloomPolicy,
       policyForFPR  :: B.FPR          -> B.BloomPolicy,
       policyBits    :: B.BloomPolicy -> B.BitsPerEntry,
       policyFPR     :: B.BloomPolicy -> B.FPR,
       sizeForPolicy :: B.BloomPolicy -> Int -> B.BloomSize,
       unfold        :: forall a b. B.Hashable a
                     => B.BloomSize -> B.Salt -> (b -> Maybe (a, b)) -> b -> bloom a,
       elem          :: forall a. B.Hashable a => a -> bloom a -> Bool
     }

classicBloomImpl :: BloomImpl B.Classic.Bloom
classicBloomImpl =
    BloomImpl {
       policyForBits = B.Classic.policyForBits,
       policyForFPR  = B.Classic.policyForFPR,
       policyBits    = B.Classic.policyBits,
       policyFPR     = B.Classic.policyFPR,
       sizeForPolicy = B.Classic.sizeForPolicy,
       unfold        = B.Classic.unfold,
       elem          = B.Classic.elem
    }

blockedBloomImpl :: BloomImpl B.Blocked.Bloom
blockedBloomImpl =
    BloomImpl {
       policyForBits = B.Blocked.policyForBits,
       policyForFPR  = B.Blocked.policyForFPR,
       policyBits    = B.Blocked.policyBits,
       policyFPR     = B.Blocked.policyFPR,
       sizeForPolicy = B.Blocked.sizeForPolicy,
       unfold        = B.Blocked.unfold,
       elem          = B.Blocked.elem
    }
