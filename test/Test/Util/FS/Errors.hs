module Test.Util.FS.Errors (
    ErrorsLog (..)
  , emptyErrorsLog
  , sumLengths
  , simErrorHasBlockIOLogged
  , simErrorHasFSLogged
  ) where

import           Control.Concurrent.Class.MonadMVar (MonadMVar)
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Void
import           System.FS.API
import           System.FS.BlockIO.API (HasBlockIO)
import           System.FS.BlockIO.Sim (fromHasFS)
import           System.FS.Sim.Error
import           System.FS.Sim.MockFS (HandleMock, MockFS)
import           System.FS.Sim.Stream

data ErrorsLog = ErrorsLog {
    dumpStateLog               :: [FsErrorType]
  , hOpenLog                   :: [FsErrorType]
  , hCloseLog                  :: [FsErrorType]
  , hIsOpenLog                 :: [Void]
  , hSeekLog                   :: [FsErrorType]
  , hGetSomeLog                :: [Either FsErrorType Partial]
  , hGetSomeAtLog              :: [Either FsErrorType Partial]
  , hPutSomeLog                :: [Either (FsErrorType, Maybe PutCorruption) Partial]
  , hTruncateLog               :: [FsErrorType]
  , hGetSizeLog                :: [FsErrorType]
  , createDirectoryLog         :: [FsErrorType]
  , createDirectoryIfMissingLog:: [FsErrorType]
  , listDirectoryLog           :: [FsErrorType]
  , doesDirectoryExistLog      :: [FsErrorType]
  , doesFileExistLog           :: [FsErrorType]
  , removeDirectoryRecursiveLog:: [FsErrorType]
  , removeFileLog              :: [FsErrorType]
  , renameFileLog              :: [FsErrorType]
  -- , mkFsErrorPathLog           :: [FsErrorType]
  -- , unsafeToFilePathLog        :: [FsErrorType]
  , hGetBufSomeLog             :: [Either FsErrorType Partial]
  , hGetBufSomeAtLog           :: [Either FsErrorType Partial]
  , hPutBufSomeLog             :: [Either (FsErrorType, Maybe PutCorruption) Partial]
  , hPutBufSomeAtLog           :: [Either (FsErrorType, Maybe PutCorruption) Partial]
  }

sumLengths :: ErrorsLog -> Int
sumLengths (ErrorsLog a b c d e f g h i j k l m n o p q r s t u v) =
    length a + length b + length c + length d +
    length e + length f + length g + length h +
    length i + length j + length k + length l +
    length m + length n + length o + length p +
    length q + length r + length s + length t +
    length u + length v

emptyErrorsLog :: ErrorsLog
emptyErrorsLog = ErrorsLog [] [] [] [] [] [] [] [] [] [] [] [] [] [] [] [] [] [] [] [] [] []

hIsOpenE :: Errors -> Stream Void
hIsOpenE _ = empty

peek :: Stream a -> Maybe a
peek (UnsafeStream _ xs) = case xs of
  []    -> Nothing
  (x:_) -> x

simErrorHasBlockIOLogged ::
     forall m. (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => StrictTMVar m MockFS
  -> StrictTVar m Errors
  -> StrictTVar m ErrorsLog
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simErrorHasBlockIOLogged fsVar errorsVar logVar = do
    let hfs = simErrorHasFSLogged fsVar errorsVar logVar
    hbio <- fromHasFS hfs
    pure (hfs, hbio)

-- | Introduce possibility of errors
simErrorHasFSLogged ::
     forall m. (MonadSTM m, MonadThrow m, PrimMonad m)
  => StrictTMVar m MockFS
  -> StrictTVar m Errors
  -> StrictTVar m ErrorsLog
  -> HasFS m HandleMock
simErrorHasFSLogged fsVar errorsVar logVar =
    let
      hfs = simErrorHasFS fsVar errorsVar
      doLog ::
           (Errors -> Stream e)
        -> (ErrorsLog -> [e])
        -> (ErrorsLog -> [e] -> ErrorsLog)
        -> m ()
      doLog getE getL setL = do
        errors <- readTVarIO errorsVar
        logs <- readTVarIO logVar
        let s = getE errors
        let x = peek s
        atomically $ writeTVar logVar $
          setL logs $ maybe id (:) x $ getL logs
    in HasFS {
        dumpState =
             doLog dumpStateE dumpStateLog (\l x -> l { dumpStateLog = x} )
          >> dumpState hfs
         -- file operations
      , hOpen = \a b ->
             doLog hOpenE hOpenLog (\l x -> l { hOpenLog = x} )
          >> hOpen hfs a b
      , hClose = \a ->
             doLog hCloseE hCloseLog (\l x -> l { hCloseLog = x} )
          >> hClose hfs a
      , hIsOpen = \a ->
             doLog hIsOpenE hIsOpenLog (\l x -> l { hIsOpenLog = x} )
          >> hIsOpen hfs a
      , hSeek = \a b c ->
             doLog hSeekE hSeekLog (\l x -> l { hSeekLog = x} )
          >> hSeek hfs a b c
      , hGetSome = \a b ->
             doLog hGetSomeE hGetSomeLog (\l x -> l { hGetSomeLog = x} )
          >> hGetSome hfs a b
      , hGetSomeAt = \a b c ->
             doLog hGetSomeAtE hGetSomeAtLog (\l x -> l { hGetSomeAtLog = x} )
          >> hGetSomeAt hfs a b c
      , hPutSome = \a b ->
             doLog hPutSomeE hPutSomeLog (\l x -> l { hPutSomeLog = x} )
          >> hPutSome hfs a b
      , hTruncate = \a b ->
             doLog hTruncateE hTruncateLog (\l x -> l { hTruncateLog = x} )
          >> hTruncate hfs a b
      , hGetSize = \a ->
             doLog hGetSizeE hGetSizeLog (\l x -> l { hGetSizeLog = x} )
          >> hGetSize hfs a
      , createDirectory = \a ->
             doLog createDirectoryE createDirectoryLog (\l x -> l { createDirectoryLog = x} )
          >> createDirectory hfs a
      , createDirectoryIfMissing = \a b ->
             doLog createDirectoryIfMissingE createDirectoryIfMissingLog (\l x -> l { createDirectoryIfMissingLog = x} )
          >> createDirectoryIfMissing hfs a b
      , listDirectory = \a ->
             doLog listDirectoryE listDirectoryLog (\l x -> l { listDirectoryLog = x} )
          >> listDirectory hfs a
      , doesDirectoryExist = \a ->
             doLog doesDirectoryExistE doesDirectoryExistLog (\l x -> l { doesDirectoryExistLog = x} )
          >> doesDirectoryExist hfs a
      , doesFileExist = \a ->
             doLog doesFileExistE doesFileExistLog (\l x -> l { doesFileExistLog = x} )
          >> doesFileExist hfs a
      , removeDirectoryRecursive = \a ->
             doLog removeDirectoryRecursiveE removeDirectoryRecursiveLog (\l x -> l { removeDirectoryRecursiveLog = x} )
          >> removeDirectoryRecursive hfs a
      , removeFile = \a ->
             doLog removeFileE removeFileLog (\l x -> l { removeFileLog = x} )
          >> removeFile hfs a
      , renameFile = \a b ->
             doLog renameFileE renameFileLog (\l x -> l { renameFileLog = x} )
          >> renameFile hfs a b
      , mkFsErrorPath = mkFsErrorPath hfs
      , unsafeToFilePath = unsafeToFilePath hfs
      , hGetBufSome = \a b c d ->
             doLog hGetBufSomeE hGetBufSomeLog (\l x -> l { hGetBufSomeLog = x} )
          >> hGetBufSome hfs a b c d
      , hGetBufSomeAt = \a b c d e ->
             doLog hGetBufSomeAtE hGetBufSomeAtLog (\l x -> l { hGetBufSomeAtLog = x} )
          >> hGetBufSomeAt hfs a b c d e
      , hPutBufSome = \a b c d ->
             doLog hPutBufSomeE hPutBufSomeLog (\l x -> l { hPutBufSomeLog = x} )
          >> hPutBufSome hfs a b c d
      , hPutBufSomeAt = \a b c d e ->
             doLog hPutBufSomeAtE hPutBufSomeAtLog (\l x -> l { hPutBufSomeAtLog = x} )
          >> hPutBufSomeAt hfs a b c d e
      }
