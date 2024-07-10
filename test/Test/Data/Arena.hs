{-# LANGUAGE CPP #-}
module Test.Data.Arena (
    tests,
) where

import           Control.Monad.ST (runST)
import           Data.Arena
import           Data.Primitive.ByteArray
import           Data.Word (Word8)
import           GHC.Exts (toList)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCaseSteps, (@?=))

tests :: TestTree
tests = testGroup "Test.Data.Arena"
    [ testCaseSteps "safe" $ \_info -> do
        let !ba = runST $ withUnmanagedArena $ \arena -> do
                (off', mba) <- allocateFromArena arena 32 8
                setByteArray mba off' 32 (1 :: Word8)
                freezeByteArray mba off' 32

        toList ba @?= replicate 32 (1 :: Word8)

    , testCaseSteps "unsafe" $ \_info -> do
        let !(off, ba) = runST $ withUnmanagedArena $ \arena -> do
                (off', mba) <- allocateFromArena arena 32 8
                setByteArray mba off' 32 (1 :: Word8)
                ba' <- unsafeFreezeByteArray mba
                return (off', ba')

#if NO_IGNORE_ASSERTS
        take 32 (drop off (toList ba)) @?= replicate 32 (0x77 :: Word8)
#else
        take 32 (drop off (toList ba)) @?= replicate 32 (1 :: Word8)
#endif
    ]
