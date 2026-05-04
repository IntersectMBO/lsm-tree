module Database.LSMTree.Internal.FS (
    -- * Hard links
    hardLink
    -- * Copy file
  , copyFile
  ) where

import           Control.ActionRegistry
import           Control.Monad (void)
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive (PrimMonad)

import qualified System.FS.API as FS
import           System.FS.API
import qualified System.FS.API.Lazy as FSL
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API (HasBlockIO)

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
-- @sourcePath@ to @targetPath@.
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
-- | @'copyFile' hfs reg source target@ copies the @source@ path to the @target@ path.
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
