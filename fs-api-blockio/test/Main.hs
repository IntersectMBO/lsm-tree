{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}

{-# OPTIONS_GHC -Wno-orphans #-}

{- HLINT ignore "Use camelCase" -}

module Main (main) where

import           Control.Concurrent (modifyMVar_, newMVar, threadDelay,
                     withMVar)
import           Control.Concurrent.Async
import           Control.Exception (SomeException, try)
import           Control.Monad
import           Control.Monad.Primitive
import           Data.ByteString
import qualified Data.ByteString as BS
import qualified Data.ByteString.Short.Internal as SBS
import           Data.Maybe (catMaybes)
import           Data.Primitive.ByteArray
import qualified System.FS.API as FS
import           System.FS.API.Strict (hPutAllStrict)
import           System.FS.BlockIO.API
import qualified System.FS.BlockIO.IO as IO
import qualified System.FS.IO as IO
import           System.IO.Temp
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "fs-api-blockio" [
      testCase "example_initClose" example_initClose
    , testCase "example_closeIsIdempotent" example_closeIsIdempotent
    , testProperty "prop_readWrite" prop_readWrite
    , testProperty "prop_submitToClosedCtx" prop_submitToClosedCtx
    ]

instance Arbitrary ByteString where
  arbitrary = BS.pack <$> arbitrary
  shrink = fmap BS.pack . shrink .  BS.unpack

fromByteString :: PrimMonad m => ByteString -> m (MutableByteArray (PrimState m))
fromByteString bs = thawByteArray (ByteArray ba) 0 (SBS.length sbs)
  where !sbs@(SBS.SBS ba) = SBS.toShort bs

toByteString :: PrimMonad m => Int -> MutableByteArray (PrimState m) -> m ByteString
toByteString n mba = freezeByteArray mba 0 n >>= \(ByteArray ba) -> pure (SBS.fromShort $ SBS.SBS ba)

example_initClose :: Assertion
example_initClose = withSystemTempDirectory "example_initClose" $ \dirPath -> do
    let mount = FS.MountPoint dirPath
        hfs = IO.ioHasFS mount
        hbfs = IO.ioHasBufFS mount
    hbio <- IO.ioHasBlockIO hfs hbfs Nothing
    close hbio

example_closeIsIdempotent :: Assertion
example_closeIsIdempotent = withSystemTempDirectory "example_closeIsIdempotent" $ \dirPath -> do
    let mount = FS.MountPoint dirPath
        hfs = IO.ioHasFS mount
        hbfs = IO.ioHasBufFS mount
    hbio <- IO.ioHasBlockIO hfs hbfs Nothing
    close hbio
    eith <- try @SomeException (close hbio)
    case eith of
      Left (e :: SomeException) ->
        assertFailure ("Close on a closed context threw an error : " <> show e)
      Right () ->
        pure ()

prop_readWrite :: ByteString -> Property
prop_readWrite bs = ioProperty $ withSystemTempDirectory "prop_readWrite" $ \dirPath -> do
    let mount = FS.MountPoint dirPath
        hfs = IO.ioHasFS mount
        hbfs = IO.ioHasBufFS mount
    hbio <- IO.ioHasBlockIO hfs hbfs Nothing
    prop <- FS.withFile hfs (FS.mkFsPath ["temp"]) (FS.WriteMode FS.MustBeNew) $ \h -> do
      let n = BS.length bs
      writeBuf <- fromByteString bs
      [IOResult m] <- submitIO hbio [IOOpWrite h 0 writeBuf 0 (fromIntegral n)]
      let writeTest = n === fromIntegral m
      readBuf <- newPinnedByteArray n
      [IOResult o] <- submitIO hbio [IOOpRead h 0 readBuf 0 (fromIntegral n)]
      let readTest = o === m
      bs' <- toByteString n readBuf
      let cmpTest = bs === bs'
      pure $ writeTest .&&. readTest .&&. cmpTest
    close hbio
    pure prop

prop_submitToClosedCtx :: ByteString -> Property
prop_submitToClosedCtx bs = ioProperty $ withSystemTempDirectory "prop_a" $ \dir -> do
    let mount = FS.MountPoint dir
        hfs = IO.ioHasFS mount
        hbfs = IO.ioHasBufFS mount
    hbio <- IO.ioHasBlockIO hfs hbfs Nothing

    props <- FS.withFile hfs (FS.mkFsPath ["temp"]) (FS.WriteMode FS.MustBeNew) $ \h -> do
      void $ hPutAllStrict hfs h bs
      syncVar <- newMVar False
      forConcurrently [0 .. BS.length bs - 1] $ \i ->
        if i == 0 then do
          threadDelay 15
          modifyMVar_ syncVar $ \_ -> do
            close hbio
            pure True
          pure Nothing
        else do
          readBuf <- newPinnedByteArray (BS.length bs)
          withMVar syncVar $ \b -> do
            eith <- try @SomeException $ submitIO hbio [IOOpRead h 0 readBuf (fromIntegral i) 1]
            pure $ case eith of
              Left _  -> Just $ tabulate "submitIO successful" [show False] $ counterexample "expected failure, but got success" (b === True)
              Right _ -> Just $ tabulate "submitIO successful" [show True]  $ counterexample "expected success, but got failure" (b === False)
    pure $ conjoin (catMaybes props)
