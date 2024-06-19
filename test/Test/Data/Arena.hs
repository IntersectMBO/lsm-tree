{-# LANGUAGE CPP #-}
module Test.Data.Arena (
    tests,
) where

import           Control.Monad.ST (runST)
import           Data.Arena
import           Data.Primitive.ByteArray
import           Data.Word (Word8)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCaseSteps, (@?=))

tests :: TestTree
tests = testGroup "Test.Data.Arena"
    [ testCaseSteps "safe" $ \_info -> do
        let !ba = runST $ withUnmanagedArena $ \arena -> do
                (off, mba) <- allocateFromArena arena 32 0
                setByteArray mba off 32 (1 :: Word8)
                freezeByteArray mba off 32

        ba @?= byteArrayFromList (replicate 32 (1 :: Word8))

    , testCaseSteps "safe" $ \_info -> do
        let !ba = runST $ withUnmanagedArena $ \arena -> do
                (off, mba) <- allocateFromArena arena 32 0
                setByteArray mba off 32 (1 :: Word8)
                unsafeFreezeByteArray mba

#if NO_IGNORE_ASSERTS
        ba @?= byteArrayFromList (replicate 32 (0x77 :: Word8))
#else
        ba @?= byteArrayFromList (replicate 32 (1 :: Word8))
#endif
    ]
