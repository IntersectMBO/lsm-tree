module Database.LSMTree.Internal.FS (
    -- * Hard links
    hardLink
  , hardLinkDirectoryRecursive
    -- * Copy file
  , copyFile
  , copyFileToDisk
  , copyFileFromDisk
  , copyDirectoryToDiskRecursive
  , copyDirectoryFromDiskRecursive
  ) where

import           Control.ActionRegistry
import           Control.Monad (forM_, void)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimBase, PrimMonad (..), RealWorld,
                     ioToPrim, primToIO)

import qualified Data.ByteString.Lazy as BSL
import qualified System.Directory as Dir
import qualified System.FilePath as FP
import qualified System.FS.API as FS
import           System.FS.API
import qualified System.FS.API.Lazy as FSL
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)
import qualified System.IO as IO
import           Text.Printf (printf)

{-------------------------------------------------------------------------------
  Hard links
-------------------------------------------------------------------------------}

{-# SPECIALISE
  hardLink ::
       HasFS IO h
    -> HasBlockIO IO h
    -> ActionRegistry IO
    -> FS.FsPath
    -> FS.FsPath
    -> IO ()
  #-}
-- | @'hardLink' hfs hbio reg sourcePath destinationPath@ creates a hard link from
-- @sourcePath@ to @destinationPath@.
--
-- Both the source path and destination path should be on the same disk volume.
hardLink ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> HasBlockIO m h
  -> ActionRegistry m
  -> FS.FsPath
  -> FS.FsPath
  -> m ()
hardLink hfs hbio reg sourcePath destinationPath = do
    withRollback_ reg
      (FS.createHardLink hbio sourcePath destinationPath)
      (FS.removeFile hfs destinationPath)

{-# SPECIALISE
  hardLinkDirectoryRecursive ::
       HasFS IO h
    -> HasBlockIO IO h
    -> ActionRegistry IO
    -> FS.FsPath
    -> FS.FsPath
    -> IO ()
  #-}
-- | Recursively create hard links for all the directory contents of the source
-- path at the destination path.
--
-- Both the source path and destination path should be on the same disk volume.
hardLinkDirectoryRecursive ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> HasBlockIO m h
  -> ActionRegistry m
     -- | Source path
  -> FS.FsPath
     -- | Destination path
  -> FS.FsPath
  -> m ()
hardLinkDirectoryRecursive hfs hbio reg sourcePath destinationPath = do
    entries <- FS.listDirectory hfs sourcePath
    forM_ entries $ \entry -> do
      let sourcePath' = sourcePath FS.</> FS.mkFsPath [entry]
          destinationPath' = destinationPath FS.</> FS.mkFsPath [entry]
      isFile <- FS.doesFileExist hfs sourcePath'
      if isFile then
        hardLink hfs hbio reg sourcePath' destinationPath'
      else do
        isDirectory <- FS.doesDirectoryExist hfs sourcePath'
        if isDirectory then do
          hardLinkDirectoryRecursive hfs hbio reg sourcePath' destinationPath'
        else
          error $ printf
            "hardLinkDirectoryRecursive: %s is not a file or directory"
            (show sourcePath')

{-------------------------------------------------------------------------------
  Copy file
-------------------------------------------------------------------------------}

{-# SPECIALISE
  copyFile ::
       HasFS IO h
    -> ActionRegistry IO
    -> FS.FsPath
    -> FS.FsPath
    -> IO ()
  #-}
-- | @'copyFile' hfs reg sourcePath destinationPath@ copies the file contents of
-- @sourcePath@ to the @destinationPath@.
copyFile ::
     (MonadMask m, PrimMonad m)
  => HasFS m h
  -> ActionRegistry m
  -> FS.FsPath
  -> FS.FsPath
  -> m ()
copyFile hfs reg sourcePath destinationPath =
    flip (withRollback_ reg) (FS.removeFile hfs destinationPath) $
      FS.withFile hfs sourcePath FS.ReadMode $ \sourceHandle ->
        FS.withFile hfs destinationPath (FS.WriteMode FS.MustBeNew) $ \targetHandle -> do
          bs <- FSL.hGetAll hfs sourceHandle
          void $ FSL.hPutAll hfs targetHandle bs

{-# SPECIALISE
  copyFileToDisk ::
       HasFS IO h
    -> ActionRegistry IO
    -> FS.FsPath
    -> FilePath
    -> IO ()
  #-}
-- | @'copyFileToDisk' hfs reg sourcePath destinationPath@ copies the file
--   contents of @sourcePath@ to the on-disk filepath @destinationPath@.
copyFileToDisk ::
     (MonadMask m, PrimBase m, PrimState m ~ RealWorld)
  => HasFS m h
  -> ActionRegistry m
  -> FS.FsPath
  -> FilePath
  -> m ()
copyFileToDisk hfs reg sourcePath destinationPath =
  flip (withRollback_ reg) (ioToPrim $ Dir.removeFile destinationPath) $
    FS.withFile hfs sourcePath FS.ReadMode $ \sourceHandle -> do
      ioToPrim . IO.withFile destinationPath IO.WriteMode $ \destinationHandle -> do
        bs <- primToIO $ FSL.hGetAll hfs sourceHandle
        BSL.hPut destinationHandle bs

{-# SPECIALISE
  copyFileFromDisk ::
       HasFS IO h
    -> ActionRegistry IO
    -> FilePath
    -> FS.FsPath
    -> IO ()
  #-}
-- | @'copyFileFromDisk' hfs reg sourcePath destinationPath@ copies the file
--   contents of @sourcePath@ to the on-disk filepath @destinationPath@.
copyFileFromDisk ::
     (MonadMask m, PrimBase m, PrimState m ~ RealWorld)
  => HasFS m h
  -> ActionRegistry m
  -> FilePath
  -> FS.FsPath
  -> m ()
copyFileFromDisk hfs reg sourcePath destinationPath =
  flip (withRollback_ reg) (FS.removeFile hfs destinationPath) $
    ioToPrim . IO.withFile sourcePath IO.ReadMode $ \sourceHandle -> do
      primToIO . FS.withFile hfs destinationPath (FS.WriteMode FS.MustBeNew) $ \destinationHandle -> do
        bs <- ioToPrim $ BSL.hGetContents sourceHandle
        void $ FSL.hPutAll hfs destinationHandle bs

{-# SPECIALISE
  copyDirectoryToDiskRecursive ::
       HasFS IO h
    -> ActionRegistry IO
    -> FS.FsPath
    -> FilePath
    -> IO ()
  #-}
-- | Recursively copy all the directory contents from the source
-- path at the destination path.
copyDirectoryToDiskRecursive ::
  forall m h.
     (MonadMask m, PrimBase m, PrimState m ~ RealWorld)
  => HasFS m h
  -> ActionRegistry m
     -- | Source path
  -> FS.FsPath
     -- | Destination path
  -> FilePath
  -> m ()
copyDirectoryToDiskRecursive hfs reg sourcePath destinationPath = do
  entries <- FS.listDirectory hfs sourcePath
  forM_ entries $ \entry -> do
    let sourcePath' = sourcePath FS.</> FS.mkFsPath [entry]
        destinationPath' = destinationPath FP.</> entry
    isFile <- FS.doesFileExist hfs sourcePath'
    if isFile then
      copyFileToDisk hfs reg sourcePath' destinationPath'
    else do
      isDirectory <- FS.doesDirectoryExist hfs sourcePath'
      if isDirectory then do
        copyDirectoryToDiskRecursive hfs reg sourcePath' destinationPath'
      else
        error $ printf
          "copyDirectoryToDiskRecursive: %s is not a file or directory"
          (show sourcePath')


{-# SPECIALISE
  copyDirectoryFromDiskRecursive ::
       HasFS IO h
    -> ActionRegistry IO
    -> FilePath
    -> FS.FsPath
    -> IO ()
  #-}
-- | Recursively copy all the directory contents from the source
-- path at the destination path.
copyDirectoryFromDiskRecursive ::
  forall m h.
     (MonadMask m, PrimBase m, PrimState m ~ RealWorld)
  => HasFS m h
  -> ActionRegistry m
     -- | Destination path
  -> FilePath
     -- | Source path
  -> FS.FsPath
  -> m ()
copyDirectoryFromDiskRecursive hfs reg sourcePath destinationPath = do
  entries <- ioToPrim $ Dir.listDirectory sourcePath
  forM_ entries $ \entry -> do
    let sourcePath' = sourcePath FP.</> entry
        destinationPath' = destinationPath FS.</> FS.mkFsPath [entry]
    isFile <- ioToPrim $ Dir.doesFileExist sourcePath'
    if isFile then
      copyFileFromDisk hfs reg sourcePath' destinationPath'
    else do
      isDirectory <- ioToPrim $ Dir.doesDirectoryExist sourcePath'
      if isDirectory then do
        copyDirectoryFromDiskRecursive hfs reg sourcePath' destinationPath'
      else
        error $ printf
          "copyDirectoryFromDiskRecursive: %s is not a file or directory"
          (show sourcePath')
