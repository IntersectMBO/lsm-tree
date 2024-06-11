module Test.Util.FS (
    withTempIOHasFS
  , withTempIOHasBlockIO
  ) where

import           System.FS.API
import           System.FS.BlockIO.API
import           System.FS.BlockIO.IO
import           System.FS.IO
import           System.IO.Temp

withTempIOHasFS :: FilePath -> (HasFS IO HandleIO -> IO a) -> IO a
withTempIOHasFS path action = withSystemTempDirectory path $ \dir -> do
    let hfs = ioHasFS (MountPoint dir)
    action hfs

withTempIOHasBlockIO :: FilePath -> (HasFS IO HandleIO -> HasBlockIO IO HandleIO -> IO a) -> IO a
withTempIOHasBlockIO path action =
    withTempIOHasFS path $ \hfs -> do
      withIOHasBlockIO hfs defaultIOCtxParams (action hfs)
