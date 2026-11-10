{-# OPTIONS_GHC -Wno-orphans #-}

module Main (main) where

import           Control.Concurrent (modifyMVar_, newMVar, threadDelay,
                     withMVar)
import           Control.Concurrent.Async
import           Control.Exception (Exception (..),
                     SomeException (SomeException), bracket, try)
import           Control.Monad
import           Control.Monad.Primitive
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import qualified Data.ByteString.Char8 as BSC
import qualified Data.ByteString.Lazy as LBS
import           Data.Foldable (traverse_)
import           Data.Functor.Compose (Compose (Compose))
import qualified Data.List as List
import           Data.Maybe (catMaybes, isJust)
import           Data.Primitive.ByteArray
import           Data.Typeable
import qualified Data.Vector as V
import qualified Data.Vector.Unboxed as VU
import           System.FS.API
import qualified System.FS.API.Lazy as FS
import qualified System.FS.API.Strict as FS
import           System.FS.API.Strict (hPutAllStrict)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API
import qualified System.FS.BlockIO.IO as IO
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

      -- Context
    , testProperty "prop_submitIO_contextClosed" prop_submitIO_contextClosed

      -- Pinned vs. unpinned buffers
    , testProperty "prop_submitIO_buffersPinned" prop_submitIO_buffersPinned
    , testProperty "prop_submitIO_buffersUnpinned" prop_submitIO_buffersUnpinned

      -- File locks
    , testProperty "prop_tryLockFileExclusiveTwice" prop_tryLockFileExclusiveTwice

      -- Storage synchronisation
    , testProperty "prop_synchronise" prop_synchronise
    , testProperty "prop_synchroniseFile_fileDoesNotExist"
        prop_synchroniseFile_fileDoesNotExist
    , testProperty "prop_synchroniseDirectory_directoryDoesNotExist"
        prop_synchroniseDirectory_directoryDoesNotExist
    , testProperty "prop_hSynchronise" prop_hSynchronise
    , testProperty "prop_synchroniseDirectoryRecursive"
        prop_synchroniseDirectoryRecursive

      -- submitIO batches
    , testProperty "prop_submitIO_batch" prop_submitIO_batch
    , testProperty "prop_submitIO_multipleHandles" prop_submitIO_multipleHandles
    , testProperty "prop_submitIO_emptyBatch" prop_submitIO_emptyBatch

      -- File data caching, advice, allocation
    , testProperty "prop_hSetNoCache" prop_hSetNoCache
    , testProperty "prop_hAdvise" prop_hAdvise
    , testProperty "prop_hDropCacheAll" prop_hDropCacheAll
    , testProperty "prop_hAllocate" prop_hAllocate

      -- File locks
    , testProperty "prop_tryLockFile_unlockRelock" prop_tryLockFile_unlockRelock

      -- Hard links
    , testProperty "prop_createHardLink" prop_createHardLink
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
    IO.withIOHasBlockIO mount IO.defaultIOCtxParams $ \_ _ -> pure ()

example_closeIsIdempotent :: Assertion
example_closeIsIdempotent = withSystemTempDirectory "example_closeIsIdempotent" $ \dirPath -> do
    let mount = FS.MountPoint dirPath
    hbio <- IO.withIOHasBlockIO mount IO.defaultIOCtxParams $ \_ hbio -> pure hbio
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
    IO.withIOHasBlockIO mount IO.defaultIOCtxParams $ \hfs hbio -> do
      FS.withFile hfs (FS.mkFsPath ["temp"]) (FS.WriteMode FS.MustBeNew) $ \h -> do
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

prop_submitToClosedCtx :: ByteString -> Property
prop_submitToClosedCtx bs = ioProperty $ withSystemTempDirectory "prop_a" $ \dir -> do
    let mount = FS.MountPoint dir
    IO.withIOHasBlockIO mount IO.defaultIOCtxParams $ \hfs hbio -> do
      FS.withFile hfs (FS.mkFsPath ["temp"]) (FS.WriteMode FS.MustBeNew) $ \h -> do
        void $ hPutAllStrict hfs h bs
        syncVar <- newMVar False
        fmap (conjoin . catMaybes) $ forConcurrently [0 .. BS.length bs - 1] $ \i ->
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

{-------------------------------------------------------------------------------
  Closed context
-------------------------------------------------------------------------------}

-- | Test that 'submitIO' on a closed context returns a "context closed" error
prop_submitIO_contextClosed :: Property
prop_submitIO_contextClosed =
    ioProperty $
    withTempIOHasBlockIO "prop_submitIO_unpinnedBuffers" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      void $ FS.hPutAll hfs h $ LBS.pack [1..100]
      buf <- newByteArray 17
      let ioops = V.fromList [
              IOOpWrite h 0 buf 0 17
            , IOOpRead h 0 buf 0 17
            ]
      close hbio
      eith <- try @FsError $ submitIO hbio ioops
      pure $ case eith of
        Left e
          | isClosedError e
          -> property True
          | otherwise
          -> counterexample ("Unexpected error: " <> displayException e) False
        Right _
          -> counterexample ("Unexpected success") False
  where
    path = FS.mkFsPath ["temp-file"]

-- TODO: add a property that checks @isClosedError . mkClosedError = True@
isClosedError :: FsError -> Bool
isClosedError e
    -- TODO: add an FsResourceVanished constructor to FsErrorType?
  | fsErrorType e == FsOther
  , "HasBlockIO closed: " `List.isPrefixOf` (fsErrorString e)
  = True
  | otherwise
  = False

{-------------------------------------------------------------------------------
  Pinned vs. unpinned buffers
-------------------------------------------------------------------------------}

-- | Test that 'submitIO' using pinned buffers returns /no/ "unpinned buffers"
-- error
prop_submitIO_buffersPinned :: Property
prop_submitIO_buffersPinned =
    ioProperty $
    withTempIOHasBlockIO "prop_submitIO_pinnedBuffers" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      void $ FS.hPutAll hfs h $ LBS.pack [1..100]
      buf <- newPinnedByteArray 17
      let ioops = V.fromList [
              IOOpWrite h 0 buf 0 17
            , IOOpRead h 0 buf 0 17
            ]
      eith <- try @FsError $ submitIO hbio ioops
      pure $ case eith of
        Left e
          -> counterexample ("Unexpected error: " <> displayException e) False
        Right _
          -> property True
  where
    path = FS.mkFsPath ["temp-file"]

-- | Test that 'submitIO' using unpinned buffers returns an "unpinned buffers" error
prop_submitIO_buffersUnpinned :: Property
prop_submitIO_buffersUnpinned =
    ioProperty $
    withTempIOHasBlockIO "prop_submitIO_unpinnedBuffers" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      void $ FS.hPutAll hfs h $ LBS.pack [1..100]
      buf <- newByteArray 17
      let ioops = V.fromList [
              IOOpWrite h 0 buf 0 17
            , IOOpRead h 0 buf 0 17
            ]
      eith <- try @FsError $ submitIO hbio ioops
      pure $ case eith of
        Left e
          | isNotPinnedError e
          -> property True
          | otherwise
          -> counterexample ("Unexpected error: " <> displayException e) False
        Right _
          -> counterexample ("Unexpected success") False
  where
    path = FS.mkFsPath ["temp-file"]

-- TODO: add a property that checks @isNotPinnedError . mkNotPinnedError = True@
isNotPinnedError :: FsError -> Bool
isNotPinnedError e
  | fsErrorType e == FsInvalidArgument
  , "MutableByteArray is unpinned: " `List.isPrefixOf` (fsErrorString e)
  = True
  | otherwise
  = False

  {-------------------------------------------------------------------------------
    File locks
  -------------------------------------------------------------------------------}

  {-------------------------------------------------------------------------------
    submitIO batches
  -------------------------------------------------------------------------------}

-- | Write a batch of chunks back to back into a file using a single 'submitIO', read them back
-- back using a single 'submitIO', and check that results correspond pair-wise to the input 'IOOp's.
prop_submitIO_batch :: [ByteString] -> Property
prop_submitIO_batch bss =
    ioProperty $
    withTempIOHasBlockIO "prop_submitIO_batch" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      let lens = map BS.length bss
          offs = scanl (+) 0 lens
      writeBufs <- traverse fromByteStringPinned bss
      writeRes <- submitIO hbio $ V.fromList
         [ IOOpWrite h (fromIntegral off) buf 0 (fromIntegral n)
         | (off, buf, n) <- zip3 offs writeBufs lens ]
      let writeTest = VU.toList writeRes == [ IOResult (fromIntegral n) | n <- lens ]
      readBufs <- traverse newPinnedByteArray lens
      readRes <- submitIO hbio $ V.fromList
         [ IOOpRead h (fromIntegral off) buf 0 (fromIntegral n)
         | (off, buf, n) <- zip3 offs readBufs lens ]
      let readTest = VU.toList readRes == VU.toList writeRes
      bss' <- sequence [ toByteString n buf | (n, buf) <- zip lens readBufs ]
      pure $ writeTest .&&. readTest .&&. (bss == bss')
  where
    path = FS.mkFsPath ["temp-file"]

-- | A single 'submitIO' batch may mix operations on different file handles.
prop_submitIO_multipleHandles :: ByteString -> ByteString -> Property
prop_submitIO_multipleHandles bs1 bs2 =
    ioProperty $
    withTempIOHasBlockIO "prop_submitIO_multipleHandles" $ \hfs hbio ->
    FS.withFile hfs path1 (FS.ReadWriteMode FS.MustBeNew) $ \h1 ->
    FS.withFile hfs path2 (FS.ReadWriteMode FS.MustBeNew) $ \h2 -> do
      writeBuf1 <- fromByteStringPinned bs1
      writeBuf2 <- fromByteStringPinned bs2
      _ <- submitIO hbio $ V.fromList
        [ IOOpWrite h1 0 writeBuf1 0 (fromIntegral (BS.length bs1))
        , IOOpWrite h2 0 writeBuf2 0 (fromIntegral (BS.length bs2)) ]
      readBuf1 <- newPinnedByteArray (BS.length bs1)
      readBuf2 <- newPinnedByteArray (BS.length bs2)
      _ <- submitIO hbio $ V.fromList
        [ IOOpRead h2 0 readBuf2 0 (fromIntegral (BS.length bs2))
        , IOOpRead h1 0 readBuf1 0 (fromIntegral (BS.length bs1)) ]
      bs1' <- toByteString (BS.length bs1) readBuf1
      bs2' <- toByteString (BS.length bs2) readBuf2
      pure $ bs1 === bs1' .&&. bs2 === bs2'
  where
    path1 = FS.mkFsPath ["temp-file-1"]
    path2 = FS.mkFsPath ["temp-file-2"]


-- | Submitting an empty batch succeeds and returns no results.
prop_submitIO_emptyBatch :: Property
prop_submitIO_emptyBatch =
    ioProperty $
    withTempIOHasBlockIO "prop_submitIO_emptyBatch" $ \_hfs hbio -> do
      res <- submitIO hbio V.empty
      pure $ res === VU.empty

  {-------------------------------------------------------------------------------
    File data caching, advice, allocation
  -------------------------------------------------------------------------------}

  -- | Toggling 'hSetNoCache' does not throw, and reads/writes still work
  -- correctly after the file data caching mode is restored.
  --
  -- Note: this is a no-op on Windows, and the no-cache mode has buffer alignment
  -- requirements on Linux, so we only perform I/O while caching is (re)enabled.
prop_hSetNoCache :: ByteString -> Property
prop_hSetNoCache bs =
    ioProperty $
    withTempIOHasBlockIO "prop_hSetNoCache" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      hSetNoCache hbio h True
      hSetNoCache hbio h False
      void $ hPutAllStrict hfs h bs
      hSetNoCache hbio h True
      hSetNoCache hbio h False
      readBuf <- newPinnedByteArray (BS.length bs)
      _ <- submitIO hbio $ V.singleton $
              IOOpRead h 0 readBuf 0 (fromIntegral (BS.length bs))
      bs' <- toByteString (BS.length bs) readBuf
      pure (bs === bs')
  where
      path = FS.mkFsPath ["temp-file"]

-- | 'hAdvise' accepts every 'Advice' value and arbitrary offsets/lengths.
--
-- Note: advice is only a hint (and a no-op on MacOS and Windows), so we can
-- only check that it does not throw.
prop_hAdvise :: ByteString -> Property
prop_hAdvise bs =
    forAll (elements [ AdviceNormal, AdviceRandom, AdviceSequential
                     , AdviceWillNeed, AdviceDontNeed, AdviceNoReuse ]) $ \advice ->
    ioProperty $
    withTempIOHasBlockIO "prop_hAdvise" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      void $ hPutAllStrict hfs h bs
      hAdvise hbio h 0 (fromIntegral (BS.length bs)) advice
      hAdviseAll hbio h advice
  where
    path = FS.mkFsPath ["temp-file"]

-- | Dropping a file from the page cache does not throw, and the file contents
-- are unaffected.
prop_hDropCacheAll :: ByteString -> Property
prop_hDropCacheAll bs =
    ioProperty $
    withTempIOHasBlockIO "prop_hDropCacheAll" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      void $ hPutAllStrict hfs h bs
      FS.synchroniseFile hfs hbio path
      hDropCacheAll hbio h
      readBuf <- newPinnedByteArray (BS.length bs)
      _ <- submitIO hbio $ V.singleton $
             IOOpRead h 0 readBuf 0 (fromIntegral (BS.length bs))
      bs' <- toByteString (BS.length bs) readBuf
      pure (bs === bs')
  where
    path = FS.mkFsPath ["temp-file"]

-- | 'hAllocate' does not throw, and reads/writes still work afterwards.
--
-- Note: allocation is a no-op on MacOS and Windows, so we can not check the
-- file size portably.
prop_hAllocate :: ByteString -> Property
prop_hAllocate bs =
    ioProperty $
    withTempIOHasBlockIO "prop_hAllocate" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      hAllocate hbio h 0 4096
      void $ hPutAllStrict hfs h bs
      readBuf <- newPinnedByteArray (BS.length bs)
      _ <- submitIO hbio $ V.singleton $
             IOOpRead h 0 readBuf 0 (fromIntegral (BS.length bs))
      bs' <- toByteString (BS.length bs) readBuf
      pure (bs === bs')
  where
    path = FS.mkFsPath ["temp-file"]

withTempIOHasBlockIO :: FilePath -> (HasFS IO HandleIO -> HasBlockIO IO HandleIO -> IO a) -> IO a
withTempIOHasBlockIO path action = withSystemTempDirectory path $ \dir -> do
    IO.withIOHasBlockIO (MountPoint dir) IO.defaultIOCtxParams action

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

-- | A released exclusive lock can be re-acquired.
--
-- Note: we only test 'ExclusiveLock'. Shared locks do not work portably in
-- real IO: on Linux, GHC's file locks are OFD @fcntl@ locks, and requesting a
-- shared lock on the write-only file descriptor that 'tryLockFile' opens fails
-- with @EBADF@.
prop_tryLockFile_unlockRelock :: Property
prop_tryLockFile_unlockRelock =
    ioProperty $
    withTempIOHasBlockIO "prop_tryLockFile_unlockRelock" $ \_hfs hbio -> do
      mlock1 <- tryLockFile hbio fsp ExclusiveLock
      traverse_ hUnlock mlock1
      bracket (tryLockFile hbio fsp ExclusiveLock) (traverse_ hUnlock) $ \mlock2 ->
        pure $ isJust mlock1 .&&. isJust mlock2
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

-- | 'hSynchronise' on a file handle does not throw.
prop_hSynchronise :: ByteString -> Property
prop_hSynchronise bs =
    ioProperty $
    withTempIOHasBlockIO "prop_hSynchronise" $ \hfs hbio ->
    FS.withFile hfs path (FS.ReadWriteMode FS.MustBeNew) $ \h -> do
      void $ hPutAllStrict hfs h bs
      hSynchronise hbio h
  where
    path = FS.mkFsPath ["temp-file"]

-- | 'synchroniseDirectoryRecursive' traverses nested directories and files
-- without throwing.
prop_synchroniseDirectoryRecursive :: ByteString -> Property
prop_synchroniseDirectoryRecursive bs =
    ioProperty $
    withTempIOHasBlockIO "prop_synchroniseDirectoryRecursive" $ \hfs hbio -> do
      FS.createDirectory hfs dir
      FS.createDirectory hfs subdir
      FS.withFile hfs file1 (FS.ReadWriteMode FS.MustBeNew) $ \h ->
        void $ hPutAllStrict hfs h bs
      FS.withFile hfs file2 (FS.ReadWriteMode FS.MustBeNew) $ \h ->
        void $ hPutAllStrict hfs h bs
      synchroniseDirectoryRecursive hfs hbio dir
    where
      dir    = FS.mkFsPath ["dir"]
      subdir = dir FS.</> FS.mkFsPath ["subdir"]
      file1  = dir FS.</> FS.mkFsPath ["file1"]
      file2  = subdir FS.</> FS.mkFsPath ["file2"]

      {-------------------------------------------------------------------------------
        Hard links
      -------------------------------------------------------------------------------}

      -- | A hard link has the same contents as the source file, and survives removal
      -- of the source path.
prop_createHardLink :: ByteString -> Property
prop_createHardLink bs =
    ioProperty $
    withTempIOHasBlockIO "prop_createHardLink" $ \hfs hbio -> do
      FS.withFile hfs source (FS.ReadWriteMode FS.MustBeNew) $ \h ->
        void $ hPutAllStrict hfs h bs
      createHardLink hbio source target
      bs' <- FS.withFile hfs target FS.ReadMode $ \h ->
        BS.concat . LBS.toChunks <$> FS.hGetAll hfs h
      FS.removeFile hfs source
      bs'' <- FS.withFile hfs target FS.ReadMode $ \h ->
        BS.concat . LBS.toChunks <$> FS.hGetAll hfs h
      pure $ bs === bs' .&&. bs === bs''
  where
    source = FS.mkFsPath ["source"]
    target = FS.mkFsPath ["target"]
