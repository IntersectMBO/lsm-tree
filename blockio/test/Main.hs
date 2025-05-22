{-# OPTIONS_GHC -Wno-orphans #-}

module Main (main) where

import           Control.Concurrent (modifyMVar_, newMVar, threadDelay,
                     withMVar)
import           Control.Concurrent.Async
import           Control.Exception (SomeException (SomeException), bracket, try)
import           Control.Monad
import           Control.Monad.Primitive
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import           Data.Foldable (traverse_)
import           Data.Functor.Compose (Compose (Compose))
import           Data.Maybe (catMaybes)
import           Data.Primitive.ByteArray
import           Data.Typeable
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import           System.FS.API
import qualified System.FS.API.Strict as FS
import           System.FS.API.Strict (hPutAllStrict)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API
import qualified System.FS.BlockIO.IO as IO
import           System.FS.BlockIO.IO
import qualified System.FS.IO as IO
import           System.FS.IO
import           System.IO.Temp
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.HUnit
import           Test.Tasty.QuickCheck (testProperty)

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "blockio:test" [
      testCase "example_initClose" example_initClose
    , testCase "example_closeIsIdempotent" example_closeIsIdempotent
    , testProperty "prop_readWrite" prop_readWrite
    , testProperty "prop_submitToClosedCtx" prop_submitToClosedCtx
    , testProperty "prop_tryLockFileExclusiveTwice" prop_tryLockFileExclusiveTwice
    , testProperty "prop_synchronise" prop_synchronise
    , testProperty "prop_synchroniseFile_fileDoesNotExist"
        prop_synchroniseFile_fileDoesNotExist
    , testProperty "prop_synchroniseDirectory_directoryDoesNotExist"
        prop_synchroniseDirectory_directoryDoesNotExist
    ]

instance Arbitrary ByteString where
  arbitrary = BS.pack <$> arbitrary
  shrink = fmap BS.pack . shrink .  BS.unpack

fromByteStringPinned :: PrimMonad m => ByteString -> m (MutableByteArray (PrimState m))
fromByteStringPinned bs = do
  mba <- newPinnedByteArray (BS.length bs)
  forM_ (zip [0..] (BS.unpack bs)) $ \(i, x) -> writeByteArray mba i x
  pure mba

toByteString :: PrimMonad m => Int -> MutableByteArray (PrimState m) -> m ByteString
toByteString n mba = do
  w8s <- forM [0..n-1] $ \i -> readByteArray mba i
  pure (BS.pack w8s)

example_initClose :: Assertion
example_initClose = withSystemTempDirectory "example_initClose" $ \dirPath -> do
    let mount = FS.MountPoint dirPath
        hfs = IO.ioHasFS mount
    hbio <- IO.ioHasBlockIO hfs FS.defaultIOCtxParams
    close hbio

example_closeIsIdempotent :: Assertion
example_closeIsIdempotent = withSystemTempDirectory "example_closeIsIdempotent" $ \dirPath -> do
    let mount = FS.MountPoint dirPath
        hfs = IO.ioHasFS mount
    hbio <- IO.ioHasBlockIO hfs FS.defaultIOCtxParams
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
    hbio <- IO.ioHasBlockIO hfs FS.defaultIOCtxParams
    prop <- FS.withFile hfs (FS.mkFsPath ["temp"]) (FS.WriteMode FS.MustBeNew) $ \h -> do
      let n = BS.length bs
      writeBuf <- fromByteStringPinned bs
      [IOResult m] <- VU.toList <$> submitIO hbio (V.singleton (IOOpWrite h 0 writeBuf 0 (fromIntegral n)))
      let writeTest = n === fromIntegral m
      readBuf <- newPinnedByteArray n
      [IOResult o] <- VU.toList <$> submitIO hbio (V.singleton (IOOpRead h 0 readBuf 0 (fromIntegral n)))
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
    hbio <- IO.ioHasBlockIO hfs FS.defaultIOCtxParams

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
            eith <- try @SomeException $ submitIO hbio (V.singleton (IOOpRead h 0 readBuf (fromIntegral i) 1))
            pure $ case eith of
              Left _  -> Just $ tabulate "submitIO successful" [show False] $ counterexample "expected failure, but got success" (b === True)
              Right _ -> Just $ tabulate "submitIO successful" [show True]  $ counterexample "expected success, but got failure" (b === False)
    pure $ conjoin (catMaybes props)


{-------------------------------------------------------------------------------
  File locks
-------------------------------------------------------------------------------}

withTempIOHasFS :: FilePath -> (HasFS IO HandleIO -> IO a) -> IO a
withTempIOHasFS path action = withSystemTempDirectory path $ \dir -> do
    let hfs = ioHasFS (MountPoint dir)
    action hfs

withTempIOHasBlockIO :: FilePath -> (HasFS IO HandleIO -> HasBlockIO IO HandleIO -> IO a) -> IO a
withTempIOHasBlockIO path action =
    withTempIOHasFS path $ \hfs -> do
      withIOHasBlockIO hfs defaultIOCtxParams (action hfs)

showLeft :: Show a => String -> Either a b -> String
showLeft x = \case
    Left e -> show e
    Right _ -> x

prop_tryLockFileExclusiveTwice :: Property
prop_tryLockFileExclusiveTwice = ioProperty $
    withTempIOHasBlockIO "prop_tryLockFileExclusiveTwice" $ \_hfs hbio -> do
      bracket (tryLockFile hbio fsp ExclusiveLock)
              (traverse_ hUnlock) $ \_ ->
        bracket (try @SomeException (tryLockFile hbio fsp ExclusiveLock))
                (traverse_ hUnlock . Compose) $ \case
          Left (SomeException e)
            | Just (e' :: FsError) <- cast e -> pure $ label (show $ fsErrorType e') $ True
          x -> pure $ counterexample
                ( "Opening a session twice in the same directory \
                  \should fail with an FsError, but it returned \
                  \the following instead: " <> showLeft "LockFileHandle" x )
                False
  where
    fsp = FS.mkFsPath ["lockfile"]

{-------------------------------------------------------------------------------
  Storage synchronisation
-------------------------------------------------------------------------------}

prop_synchronise :: Property
prop_synchronise =
    ioProperty $
    withTempIOHasBlockIO "temp" $ \hfs hbio -> do
      FS.createDirectory hfs dir
      FS.withFile hfs file (FS.ReadWriteMode FS.MustBeNew) $ \h ->
        void $ FS.hPutAllStrict hfs h (BSC.pack "file-contents")
      FS.synchroniseFile hfs hbio file
      FS.synchroniseDirectory hbio dir
  where
    dir = FS.mkFsPath ["dir"]
    file = dir FS.</> FS.mkFsPath ["file"]

prop_synchroniseFile_fileDoesNotExist :: Property
prop_synchroniseFile_fileDoesNotExist =
    expectFailure $
    ioProperty $
    withTempIOHasBlockIO "temp" $ \hfs hbio -> do
      FS.synchroniseFile hfs hbio file
  where
    file = FS.mkFsPath ["file"]

prop_synchroniseDirectory_directoryDoesNotExist :: Property
prop_synchroniseDirectory_directoryDoesNotExist =
    expectFailure $
    ioProperty $
    withTempIOHasBlockIO "temp" $ \_hfs hbio -> do
      FS.synchroniseDirectory hbio dir
  where
    dir = FS.mkFsPath ["dir"]
