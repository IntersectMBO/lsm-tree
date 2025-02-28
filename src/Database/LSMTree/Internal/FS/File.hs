module Database.LSMTree.Internal.FS.File (
    File (..)
    -- * Construction
  , newFile
  , copyFile
    -- * Opening handles
  , Mode (..)
  , openHandle
  ) where

import           Control.DeepSeq
import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad)
import           Control.RefCount
import qualified System.FS.API as FS
import           System.FS.API (AllowExisting (..), FsPath, Handle, HasFS)
import qualified System.FS.API.Lazy as FSL

-- | A reference counted file.
--
-- When all references are released, the file will be deleted from disk.
--
-- INVARIANT: 'filePath' /must/ exist on disk.
data File m = File {
    filePath       :: {-# UNPACK #-} !FsPath,
    fileRefCounter :: {-# UNPACK #-} !(RefCounter m)
  }
  deriving stock Show

instance RefCounted m (File m) where
    getRefCounter = fileRefCounter

instance NFData (File m) where
  rnf (File a b) = rnf a `seq` rnf b

-- | Create a 'File' from a file path. This functions assumes the file path
-- exists.
--
-- REF: the resulting reference must be released once it is no longer used.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
fromFsPath :: PrimMonad m => HasFS m h -> FsPath -> m (Ref (File m))
fromFsPath hfs path = do
    let finaliser = FS.removeFile hfs path
    newRef finaliser $ \fileRefCounter ->
        File {
            filePath = path
          , fileRefCounter
          }

{-------------------------------------------------------------------------------
  Construction
-------------------------------------------------------------------------------}

{-# SPECIALISE newFile :: HasFS IO h -> FsPath -> IO (Ref (File IO)) #-}
{-# INLINABLE newFile #-}
-- | Create a new file.
--
-- REF: the resulting reference must be released once it is no longer used.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
newFile :: (MonadCatch m, PrimMonad m) => HasFS m h -> FsPath -> m (Ref (File m))
newFile hfs path = withCreateFile hfs path $ \_h -> fromFsPath hfs path

{-# SPECIALISE copyFile :: HasFS IO h -> FsPath -> FsPath -> IO (Ref (File IO)) #-}
{-# INLINABLE copyFile #-}
-- | @'copyFile' hfs source target@ copies the @source@ path to the /new/
-- @target@ path.
--
-- REF: the resulting reference must be released once it is no longer used.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
copyFile ::
     (MonadCatch m, PrimMonad m)
  => HasFS m h
  -> FsPath
  -> FsPath
  -> m (Ref (File m))
copyFile hfs sourcePath targetPath =
    FS.withFile hfs sourcePath FS.ReadMode $ \sourceHandle -> do
      withCreateFile hfs targetPath $ \targetHandle -> do
        bs <- FSL.hGetAll hfs sourceHandle
        void $ FSL.hPutAll hfs targetHandle bs
        fromFsPath hfs targetPath

{-# SPECIALISE withCreateFile :: HasFS IO h -> FsPath -> (Handle h -> IO a) -> IO a #-}
{-# INLINABLE withCreateFile #-}
-- | Run an action on a handle to a newly created file.
--
-- The handle is closed automatically when the action is finished, even if an
-- exception is thrown in the action.
--
-- If an exception happens in the action, the file is removed.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
withCreateFile :: MonadCatch m => HasFS m h -> FsPath -> (Handle h -> m a) -> m a
withCreateFile hfs path k = fst <$> generalBracket acquire release k
  where
    acquire = FS.hOpen hfs path (FS.WriteMode MustBeNew)
    release h = \case
      ExitCaseSuccess _ -> FS.hClose hfs h
      ExitCaseException e -> do
          FS.hClose hfs h
        `finally`
          FS.removeFile hfs path
        `finally`
          throwIO e
      ExitCaseAbort -> do
          FS.hClose hfs h
        `finally`
          FS.removeFile hfs path

{-------------------------------------------------------------------------------
  Opening handles
-------------------------------------------------------------------------------}

-- | File open mode. The file is assumed to exist already on disk.
data Mode = Read | ReadWrite | Write | Append
  deriving stock (Show, Eq)

modeOpenMode :: Mode -> FS.OpenMode
modeOpenMode = \case
    Read -> FS.ReadMode
    ReadWrite -> FS.ReadWriteMode MustExist
    Write -> FS.WriteMode MustExist
    Append -> FS.AppendMode MustExist

{-# INLINABLE openHandle #-}
-- | Open a handle to a 'File'.
--
-- REF: the resulting handle should be closed when it is no longer used.
--
-- ASYNC: this should be called with asynchronous exceptions masked because it
-- allocates/creates resources.
openHandle :: HasFS m h -> Ref (File m) -> Mode -> m (Handle h)
openHandle hfs (DeRef file) mode = FS.hOpen hfs (filePath file) (modeOpenMode mode)
