-- | Common file system operations
module Database.LSMTree.Internal.FS (
    -- * Hard links
    hardLink
  , hardLinkDirectoryRec
    -- * Copy files and directories
  , copyFile'
  , copyFile
  , copyDirectoryRec
  , copyDirectoryRec'
    -- * Hard links with copy fallback
  , hardLinkOrCopyDirectoryRec
  ) where

import           Control.ActionRegistry
import           Control.Monad (forM_, void)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad)

import           Foreign.C.Error (eXDEV)
import           GHC.Stack (HasCallStack)
import qualified System.FS.API as FS
import           System.FS.API (FsError, FsPath, HasFS)
import qualified System.FS.API.Lazy as FSL
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)
import           Text.Printf (printf)

{-------------------------------------------------------------------------------
  Hard links
-------------------------------------------------------------------------------}

{-# SPECIALISE
  hardLink ::
       HasCallStack
    => ActionRegistry IO
    -> HasFS IO h
    -> HasBlockIO IO h'
    -> FsPath
    -> FsPath
    -> IO ()
  #-}
-- | Create a hard link for a source file at a destination file /on the same
-- file system/.
--
-- If the source path and destination path are not on the same disk volume, an
-- 'FsError' with an @EXDEV@ error number will be thrown.
hardLink ::
     (HasCallStack, MonadMask m, PrimMonad m)
  => ActionRegistry m
     -- | The file system
  -> HasFS m h
     -- | The file system
  -> HasBlockIO m h'
     -- | The path to the source file
  -> FsPath
     -- | The path to the destination file
  -> FsPath
  -> m ()
hardLink reg hfs hbio srcPath dstPath = do
    withRollback_ reg
      (FS.createHardLink hbio srcPath dstPath)
      (FS.removeFile hfs dstPath)

{-# SPECIALISE
  hardLinkDirectoryRec ::
       HasCallStack
    => ActionRegistry IO
    -> HasFS IO h
    -> HasBlockIO IO h'
    -> FsPath
    -> FsPath
    -> IO ()
  #-}
-- | Recursively create hard links for a source directory and its contents at a
-- destination directory /on the same file system/.
--
-- If the source path and destination path (or recursively their directory
-- contents) are not on the same disk volume, an 'FsError' with an @EXDEV@ error
-- number will be thrown.
hardLinkDirectoryRec ::
     (HasCallStack, MonadMask m, PrimMonad m)
  => ActionRegistry m
     -- | The file system
  -> HasFS m h
     -- | The file system
  -> HasBlockIO m h'
     -- | The path to the source directory
  -> FsPath
     -- | The path to the destination directory
  -> FsPath
  -> m ()
hardLinkDirectoryRec reg hfs hbio srcPath dstPath = do
    entries <- FS.listDirectory hfs srcPath
    forM_ entries $ \entry -> do
      let srcPath' = srcPath FS.</> FS.mkFsPath [entry]
          dstPath' = dstPath FS.</> FS.mkFsPath [entry]
      isFile <- FS.doesFileExist hfs srcPath'
      if isFile then hardLink reg hfs hbio srcPath' dstPath'
      else do
        isDirectory <- FS.doesDirectoryExist hfs srcPath'
        if isDirectory then hardLinkDirectoryRec reg hfs hbio srcPath' dstPath'
        else error $ printf
                "hardLinkDirectoryRec: %s is not a file or directory"
                (show srcPath')

{-------------------------------------------------------------------------------
  Copy files and directories
-------------------------------------------------------------------------------}

{-# SPECIALISE
  copyFile' ::
       HasCallStack
    => ActionRegistry IO
    -> HasFS IO h
    -> FsPath
    -> FsPath
    -> IO ()
  #-}
-- | Copy a source file to a destination file /on the same file system/.
copyFile' ::
     (HasCallStack, MonadMask m, PrimMonad m)
  => ActionRegistry m
     -- | The file system
  -> HasFS m h
     -- | The path to the source file
  -> FS.FsPath
     -- | The path to the destination file
  -> FS.FsPath
  -> m ()
copyFile' reg hfs srcPath dstPath = copyFile reg hfs srcPath hfs dstPath

{-# SPECIALISE
  copyDirectoryRec' ::
       HasCallStack
    => ActionRegistry IO
    -> HasFS IO h
    -> FsPath
    -> FsPath
    -> IO ()
  #-}
-- | Recursively copy a source directory and its contents to a destination
-- directory /on the same file system/.
copyDirectoryRec' ::
     (HasCallStack, MonadMask m, PrimMonad m)
  => ActionRegistry m
     -- The file system
  -> HasFS m h
     -- | The path to the source directory
  -> FsPath
     -- | The path to the destination directory
  -> FsPath
  -> m ()
copyDirectoryRec' reg hfs srcPath dstPath = copyDirectoryRec reg hfs srcPath hfs dstPath

{-# SPECIALISE
  copyFile ::
       HasCallStack
    => ActionRegistry IO
    -> HasFS IO h
    -> FsPath
    -> HasFS IO h'
    -> FsPath
    -> IO ()
  #-}
-- | Copy a source file to a destination file /on potentially different file
-- systems/.
copyFile ::
     (HasCallStack, MonadMask m, PrimMonad m)
  => ActionRegistry m
     -- | The source file system
  -> HasFS m h
     -- | The path to the source file with respect to the source fileystem
  -> FsPath
     -- | The destination file system
  -> HasFS m h'
     -- | The path to the destination file with respect to the destination
     -- fileystem
  -> FsPath
  -> m ()
copyFile reg srcFS srcPath dstFS dstPath =
    FS.withFile srcFS srcPath FS.ReadMode $ \srcH ->
    -- We use the action registry to open the destination file and to track that
    -- we should remove that file even if an exception occurs after we've exited
    -- copyFile.
    --
    -- The outer bracket is used to ensure that we close the file handle after
    -- we've written to it.
    bracket (withRollback reg acq rel) (FS.hClose dstFS) $ \dstH -> do
      bs <- FSL.hGetAll srcFS srcH
      void $ FSL.hPutAll dstFS dstH bs
  where
    acq      = FS.hOpen dstFS dstPath (FS.WriteMode FS.MustBeNew)
    -- hClose is idempotent, so it does not hurt to try closing before removing
    -- even if we are pretty sure we'll close the file handle as part of the
    -- @bracket@ above.
    rel dstH = FS.hClose dstFS dstH >> FS.removeFile dstFS dstPath

{-# SPECIALISE
  copyDirectoryRec ::
       HasCallStack
    => ActionRegistry IO
    -> HasFS IO h
    -> FsPath
    -> HasFS IO h'
    -> FsPath
    -> IO ()
  #-}
-- | Recursively copy a source directory and its contents to a destination
-- directory /on potentially different file systems/
copyDirectoryRec ::
     (HasCallStack, MonadMask m, PrimMonad m)
  => ActionRegistry m
     -- The source file system
  -> HasFS m h
     -- | The path to the source directory with respect to the source fileystem
  -> FsPath
     -- | The destination file system
  -> HasFS m h'
     -- | The path to the destination directory with respect to the destination
     -- fileystem
  -> FsPath
  -> m ()
copyDirectoryRec reg srcFS srcPath dstFS dstPath = do
    entries <- FS.listDirectory srcFS srcPath
    forM_ entries $ \entry -> do
      let srcPath' = srcPath FS.</> FS.mkFsPath [entry]
          dstPath' = dstPath FS.</> FS.mkFsPath [entry]
      isFile <- FS.doesFileExist srcFS srcPath'
      if isFile then copyFile reg srcFS srcPath' dstFS dstPath'
      else do
        isDirectory <- FS.doesDirectoryExist srcFS srcPath'
        if isDirectory then copyDirectoryRec reg srcFS srcPath' dstFS dstPath'
        else error $ printf
                "copyDirectoryRec: %s is not a file or directory"
                (show srcPath')

{-------------------------------------------------------------------------------
  Hard links with copy fallback
-------------------------------------------------------------------------------}

{-# SPECIALISE
  hardLinkOrCopyDirectoryRec ::
       HasCallStack
    => ActionRegistry IO
    -> HasFS IO h
    -> HasBlockIO IO h'
    -> FsPath
    -> FsPath
    -> IO ()
  #-}
-- | Attempt to use 'hardLinkDirectoryRec', and if that fails because the source
-- and destination directory are not on the same disk volume, use
-- 'copyDirectoryRec'' instead.
hardLinkOrCopyDirectoryRec ::
     (HasCallStack, MonadMask m, PrimMonad m)
  => ActionRegistry m
     -- | The file system
  -> HasFS m h
     -- | The file system
  -> HasBlockIO m h'
     -- | The path to the source directory
  -> FsPath
     -- | The path to the destination directory
  -> FsPath
  -> m ()
hardLinkOrCopyDirectoryRec reg hfs hbio srcPath dstPath =
    catchJust ifEXDEV doHardLink (const doFallBack)
  where
    doHardLink = hardLinkDirectoryRec reg hfs hbio srcPath dstPath
    doFallBack = copyFile' reg hfs srcPath dstPath

    -- NOTE: On Windows, the error code is ERROR_NOT_SAME_DEVICE (17),
    --       but the Win32 primitive for creating hard links maps this
    --       to the POSIX error code EXDEV using the maperrno builtin.
    isEXDEV :: FsError -> Bool
    isEXDEV e = FS.fsErrorNo e == Just eXDEV
    ifEXDEV e = if isEXDEV e then Just e else Nothing
