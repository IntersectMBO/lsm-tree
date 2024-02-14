module Database.LSMTree.Internal.Run.FsPaths (
    RunFsPaths (..)
  , runKOpsPath
  , runBlobPath
  , runFilterPath
  , runIndexPath
  , activeRunsDir
  ) where

import qualified System.FS.API as FS
import           System.FS.API (FsPath)

-- | The (relative) file path locations of all the files used by the run:
--
-- Within the session root directory, the following files exist for active runs:
--
-- 1. @active/${n}.keyops@: the sorted run of key\/operation pairs
-- 2. @active/${n}.blobs@:  the blob values associated with the key\/operations
-- 3. @active/${n}.filter@: a Bloom filter of all the keys in the run
-- 4. @active/${n}.index@:  an index from keys to disk page numbers
-- 5. @active/${n}.checksums@: a file listing the crc32c checksums of the other
--    files
--
-- The representation doesn't store the full, name, just the number @n@. Use
-- the accessor functions to get the actual names.
--
newtype RunFsPaths = RunFsPaths { runNumber :: Int }
  deriving Show

runKOpsPath :: RunFsPaths -> FsPath
runKOpsPath = runFilePathWithExt ".keyops"

runBlobPath :: RunFsPaths -> FsPath
runBlobPath = runFilePathWithExt ".blobs"

runFilterPath :: RunFsPaths -> FsPath
runFilterPath = runFilePathWithExt ".filter"

runIndexPath :: RunFsPaths -> FsPath
runIndexPath = runFilePathWithExt ".index"

runFilePathWithExt :: String -> RunFsPaths -> FsPath
runFilePathWithExt ext (RunFsPaths n) =
    FS.mkFsPath ["active", show n <> ext]

activeRunsDir :: FsPath
activeRunsDir = FS.mkFsPath ["active"]
