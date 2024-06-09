module Database.LSMTree.Internal.Paths (
    RunFsPaths (..)
  , pathsForRunFiles
  , runKOpsPath
  , runBlobPath
  , runFilterPath
  , runIndexPath
  , runChecksumsPath
  , activeRunsDir
    -- * Checksums
  , checksumFileNamesForRunFiles
  , toChecksumsFile
  , fromChecksumsFile
    -- * ForRunFiles abstraction
  , ForRunFiles (..)
  ) where

import           Control.Applicative (Applicative (..))
import           Control.DeepSeq (NFData)
import qualified Data.ByteString.Char8 as BS
import           Data.Foldable (toList)
import qualified Data.Map as Map
import           Data.Traversable (for)
import           Prelude hiding (Applicative (..))
import qualified System.FS.API as FS
import           System.FS.API (FsPath)

import qualified Database.LSMTree.Internal.CRC32C as CRC

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
  deriving (Show, NFData)

-- | Paths to all files associated with this run, except 'runChecksumsPath'.
pathsForRunFiles :: RunFsPaths -> ForRunFiles FsPath
pathsForRunFiles fsPaths = fmap (runFilePathWithExt fsPaths) runFileExts

runKOpsPath :: RunFsPaths -> FsPath
runKOpsPath = forRunKOps . pathsForRunFiles

runBlobPath :: RunFsPaths -> FsPath
runBlobPath = forRunBlob . pathsForRunFiles

runFilterPath :: RunFsPaths -> FsPath
runFilterPath = forRunFilter . pathsForRunFiles

runIndexPath :: RunFsPaths -> FsPath
runIndexPath = forRunIndex . pathsForRunFiles

runChecksumsPath :: RunFsPaths -> FsPath
runChecksumsPath = flip runFilePathWithExt "checksums"

runFilePathWithExt :: RunFsPaths -> String -> FsPath
runFilePathWithExt (RunFsPaths n) ext =
    FS.mkFsPath ["active", show n <> "." <> ext]

activeRunsDir :: FsPath
activeRunsDir = FS.mkFsPath ["active"]

runFileExts :: ForRunFiles String
runFileExts = ForRunFiles {
      forRunKOps   = "keyops"
    , forRunBlob   = "blobs"
    , forRunFilter = "filter"
    , forRunIndex  = "index"
    }

{-------------------------------------------------------------------------------
  Checksums
-------------------------------------------------------------------------------}

checksumFileNamesForRunFiles :: ForRunFiles CRC.ChecksumsFileName
checksumFileNamesForRunFiles = fmap (CRC.ChecksumsFileName . BS.pack) runFileExts

toChecksumsFile :: ForRunFiles CRC.CRC32C -> CRC.ChecksumsFile
toChecksumsFile = Map.fromList . toList . liftA2 (,) checksumFileNamesForRunFiles

fromChecksumsFile :: CRC.ChecksumsFile -> Either String (ForRunFiles CRC.CRC32C)
fromChecksumsFile file = for checksumFileNamesForRunFiles $ \name ->
    case Map.lookup name file of
      Just crc -> Right crc
      Nothing  -> Left ("key not found: " <> show name)

{-------------------------------------------------------------------------------
  ForRunFiles abstraction
-------------------------------------------------------------------------------}

-- | Stores someting for each run file (except the checksums file), allowing to
-- easily do something for all of them without mixing them up.
data ForRunFiles a = ForRunFiles {
      forRunKOps   :: !a
    , forRunBlob   :: !a
    , forRunFilter :: !a
    , forRunIndex  :: !a
    }
  deriving (Show, Foldable, Functor, Traversable)

instance Applicative ForRunFiles where
  pure x = ForRunFiles x x x x
  ForRunFiles f1 f2 f3 f4 <*> ForRunFiles x1 x2 x3 x4 =
    ForRunFiles (f1 x1) (f2 x2) (f3 x3) (f4 x4)
