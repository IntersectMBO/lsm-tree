{-# LANGUAGE CPP, GeneralizedNewtypeDeriving #-}
{-# OPTIONS_GHC -fno-warn-orphans #-}

module QCSupport
    (
      P(..)
    ) where

import Data.Int (Int8, Int16, Int32, Int64)
import Data.Word (Word8, Word16, Word32, Word64)
import qualified Data.ByteString.Char8 as SB
import qualified Data.ByteString.Lazy.Char8 as LB
import System.Random (Random(..), RandomGen)
import Test.QuickCheck

newtype P = P { unP :: Double }
    deriving (Eq, Ord, Show, Fractional, Num, Random)

instance Arbitrary P where
    arbitrary = choose (epsilon, 1 - epsilon)
        where epsilon = 1e-6 :: P

instance Arbitrary LB.ByteString where
    arbitrary = sized $ \n -> resize (round (sqrt (toEnum n :: Double)))
                ((LB.fromChunks . filter (not . SB.null)) `fmap` arbitrary)

instance Arbitrary SB.ByteString where
    arbitrary = SB.pack `fmap` arbitrary
