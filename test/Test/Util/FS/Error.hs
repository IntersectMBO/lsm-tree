module Test.Util.FS.Error (
    -- * Errors log
    ErrorsLog
  , emptyLog
  , countNoisyErrors
    -- * Logged HasFS and HasBlockIO
  , simErrorHasBlockIOLogged
  , simErrorHasFS
  ) where

import           Control.Concurrent.Class.MonadMVar (MonadMVar)
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Data.Functor.Barbie
import           Data.Monoid
import           GHC.Generics
import           System.FS.API
import           System.FS.BlockIO.API
import           System.FS.BlockIO.Sim
import           System.FS.Sim.Error
import           System.FS.Sim.MockFS (HandleMock, MockFS)
import           System.FS.Sim.Stream

{-------------------------------------------------------------------------------
  Higher-kinded datatype
-------------------------------------------------------------------------------}

-- | A higher-kinded datatype (HKD) version of the 'Errors' type. 'Errors' is
-- equivalent to @'HKD' 'Stream'@.
--
-- 'HKD' has instances for @barbies@ classes, which lets us manipulate 'HKD'
-- with a small set of combinators like 'bpure' and 'bmap'. This makes writing
-- functions operating on 'HKD' less arduous than writing the functions out
-- fully.
--
-- TODO: admittedly, we are not getting the full benefis from higher-kinded
-- datatypes because 'Errors' and 'HasFS' are not HKDs, meaning we still have to
-- write some stuff out manually, like in 'simErrorHasFSLogged'. We could take
-- the HKD approach even further, but it is likely going to require quite a bit
-- of boilerplate to set up before we can start programming with HKDs. At that
-- point one might ask whether the costs of defining the HKD boilerplate
-- outweighs the benefits of programming with HKDs.
data HKD f = HKD {
    dumpStateHKD               :: f FsErrorType
  , hOpenHKD                   :: f FsErrorType
  , hCloseHKD                  :: f FsErrorType
  , hSeekHKD                   :: f FsErrorType
  , hGetSomeHKD                :: f (Either FsErrorType Partial)
  , hGetSomeAtHKD              :: f (Either FsErrorType Partial)
  , hPutSomeHKD                :: f (Either (FsErrorType, Maybe PutCorruption) Partial)
  , hTruncateHKD               :: f FsErrorType
  , hGetSizeHKD                :: f FsErrorType
  , createDirectoryHKD         :: f FsErrorType
  , createDirectoryIfMissingHKD:: f FsErrorType
  , listDirectoryHKD           :: f FsErrorType
  , doesDirectoryExistHKD      :: f FsErrorType
  , doesFileExistHKD           :: f FsErrorType
  , removeDirectoryRecursiveHKD:: f FsErrorType
  , removeFileHKD              :: f FsErrorType
  , renameFileHKD              :: f FsErrorType
  , hGetBufSomeHKD             :: f (Either FsErrorType Partial)
  , hGetBufSomeAtHKD           :: f (Either FsErrorType Partial)
  , hPutBufSomeHKD             :: f (Either (FsErrorType, Maybe PutCorruption) Partial)
  , hPutBufSomeAtHKD           :: f (Either (FsErrorType, Maybe PutCorruption) Partial)
  }
  deriving stock Generic

deriving anyclass instance FunctorB HKD
deriving anyclass instance ApplicativeB HKD
deriving anyclass instance TraversableB HKD
deriving anyclass instance ConstraintsB HKD

{-------------------------------------------------------------------------------
  Errors log
-------------------------------------------------------------------------------}

type ErrorsLog = HKD []

emptyLog :: ErrorsLog
emptyLog = bpure []

{-------------------------------------------------------------------------------
  Count noisy errors in the log
-------------------------------------------------------------------------------}

class IsNoisy a where
  isNoisy :: a -> Bool

instance IsNoisy FsErrorType where
  isNoisy _ = True

instance IsNoisy (Either FsErrorType b) where
  isNoisy (Left x)  = isNoisy x
  isNoisy (Right _) = False

instance IsNoisy (Either (FsErrorType, b) c) where
  isNoisy (Left (x, _)) = isNoisy x
  isNoisy (Right _)     = False

countNoisyErrors :: ErrorsLog -> Int
countNoisyErrors = getSum . bfoldMapC @IsNoisy (Sum . length . Prelude.filter isNoisy)

{-------------------------------------------------------------------------------
  Logged HasFS and HasBlockIO
-------------------------------------------------------------------------------}

-- | Like 'simErrorHasFSLogged', but also produces a simulated 'HasBlockIO'.
simErrorHasBlockIOLogged ::
     forall m. (MonadCatch m, MonadMVar m, PrimMonad m, MonadSTM m)
  => StrictTMVar m MockFS
  -> StrictTVar m Errors
  -> StrictTVar m (HKD [])
  -> m (HasFS m HandleMock, HasBlockIO m HandleMock)
simErrorHasBlockIOLogged fsVar errorsVar logVar = do
    let hfs = simErrorHasFSLogged fsVar errorsVar logVar
    hbio <- unsafeFromHasFS hfs
    pure (hfs, hbio)

-- | Produce a simulated file system with injected errors and a logger for those
-- errors.
--
-- Every time a 'HasFS' primitive is used and an error from 'Errors' is used, it
-- will be logged in 'ErrorsLog'.
simErrorHasFSLogged ::
     forall m. (MonadSTM m, MonadThrow m, PrimMonad m)
  => StrictTMVar m MockFS
  -> StrictTVar m Errors
  -> StrictTVar m ErrorsLog
  -> HasFS m HandleMock
simErrorHasFSLogged fsVar errorsVar logVar =
    HasFS {
        dumpState = do
          addToLog dumpStateE dumpStateHKD (\l x -> l { dumpStateHKD = x} )
          dumpState hfs
      , hOpen = \a b -> do
          addToLog hOpenE hOpenHKD (\l x -> l { hOpenHKD = x} )
          hOpen hfs a b
      , hClose = \a -> do
          addToLog hCloseE hCloseHKD (\l x -> l { hCloseHKD = x} )
          hClose hfs a
      , hIsOpen = \a ->
          hIsOpen hfs a
      , hSeek = \a b c -> do
          addToLog hSeekE hSeekHKD (\l x -> l { hSeekHKD = x} )
          hSeek hfs a b c
      , hGetSome = \a b -> do
          addToLog hGetSomeE hGetSomeHKD (\l x -> l { hGetSomeHKD = x} )
          hGetSome hfs a b
      , hGetSomeAt = \a b c -> do
          addToLog hGetSomeAtE hGetSomeAtHKD (\l x -> l { hGetSomeAtHKD = x} )
          hGetSomeAt hfs a b c
      , hPutSome = \a b -> do
          addToLog hPutSomeE hPutSomeHKD (\l x -> l { hPutSomeHKD = x} )
          hPutSome hfs a b
      , hTruncate = \a b -> do
          addToLog hTruncateE hTruncateHKD (\l x -> l { hTruncateHKD = x} )
          hTruncate hfs a b
      , hGetSize = \a -> do
          addToLog hGetSizeE hGetSizeHKD (\l x -> l { hGetSizeHKD = x} )
          hGetSize hfs a
      , createDirectory = \a -> do
          addToLog createDirectoryE createDirectoryHKD (\l x -> l { createDirectoryHKD = x} )
          createDirectory hfs a
      , createDirectoryIfMissing = \a b -> do
          addToLog createDirectoryIfMissingE createDirectoryIfMissingHKD (\l x -> l { createDirectoryIfMissingHKD = x} )
          createDirectoryIfMissing hfs a b
      , listDirectory = \a -> do
          addToLog listDirectoryE listDirectoryHKD (\l x -> l { listDirectoryHKD = x} )
          listDirectory hfs a
      , doesDirectoryExist = \a -> do
          addToLog doesDirectoryExistE doesDirectoryExistHKD (\l x -> l { doesDirectoryExistHKD = x} )
          doesDirectoryExist hfs a
      , doesFileExist = \a -> do
          addToLog doesFileExistE doesFileExistHKD (\l x -> l { doesFileExistHKD = x} )
          doesFileExist hfs a
      , removeDirectoryRecursive = \a -> do
          addToLog removeDirectoryRecursiveE removeDirectoryRecursiveHKD (\l x -> l { removeDirectoryRecursiveHKD = x} )
          removeDirectoryRecursive hfs a
      , removeFile = \a -> do
          addToLog removeFileE removeFileHKD (\l x -> l { removeFileHKD = x} )
          removeFile hfs a
      , renameFile = \a b -> do
          addToLog renameFileE renameFileHKD (\l x -> l { renameFileHKD = x} )
          renameFile hfs a b
      , mkFsErrorPath = mkFsErrorPath hfs
      , unsafeToFilePath = unsafeToFilePath hfs
      , hGetBufSome = \a b c d -> do
          addToLog hGetBufSomeE hGetBufSomeHKD (\l x -> l { hGetBufSomeHKD = x} )
          hGetBufSome hfs a b c d
      , hGetBufSomeAt = \a b c d e -> do
          addToLog hGetBufSomeAtE hGetBufSomeAtHKD (\l x -> l { hGetBufSomeAtHKD = x} )
          hGetBufSomeAt hfs a b c d e
      , hPutBufSome = \a b c d -> do
          addToLog hPutBufSomeE hPutBufSomeHKD (\l x -> l { hPutBufSomeHKD = x} )
          hPutBufSome hfs a b c d
      , hPutBufSomeAt = \a b c d e -> do
          addToLog hPutBufSomeAtE hPutBufSomeAtHKD (\l x -> l { hPutBufSomeAtHKD = x} )
          hPutBufSomeAt hfs a b c d e
      }
  where
    hfs = simErrorHasFS fsVar errorsVar

    -- Peek at the first element from an error stream and add it to the errors
    -- log if it is 'Just' an error.
    addToLog ::
         (Errors -> Stream e)
      -> (ErrorsLog -> [e])
      -> (ErrorsLog -> [e] -> ErrorsLog)
      -> m ()
    addToLog getE getL setL = do
        errors <- readTVarIO errorsVar
        logs <- readTVarIO logVar
        let s = getE errors
        let x = peek s
        case x of
          Nothing -> pure ()
          Just y ->
            atomically $ writeTVar logVar $
              setL logs $ (++ [y]) $ getL logs

    -- Peek at the first element from an error stream.
    peek :: Stream a -> Maybe a
    peek (UnsafeStream _ xs) = case xs of
      []    -> Nothing
      (x:_) -> x
