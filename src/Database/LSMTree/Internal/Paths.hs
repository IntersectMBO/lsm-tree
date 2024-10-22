module Database.LSMTree.Internal.Paths (
    SessionRoot (..)
  , lockFile
  , activeDir
  , runPath
  , snapshotsDir
  , snapshot
    -- * Table paths
  , tableBlobPath
    -- * Snapshot name
  , SnapshotName
  , mkSnapshotName
    -- * Run paths
  , RunFsPaths (..)
  , pathsForRunFiles
  , runKOpsPath
  , runBlobPath
  , runFilterPath
  , runIndexPath
  , runChecksumsPath
    -- * Checksums
  , checksumFileNamesForRunFiles
  , toChecksumsFile
  , fromChecksumsFile
    -- * ForRunFiles abstraction
  , ForRunFiles (..)
  ) where

import           Control.Applicative (Applicative (..))
import           Control.DeepSeq (NFData (..))
import qualified Data.ByteString.Char8 as BS
import           Data.Foldable (toList)
import qualified Data.Map as Map
import           Data.Maybe (fromMaybe)
import           Data.String (IsString (..))
import           Data.Traversable (for)
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.UniqCounter
import           Prelude hiding (Applicative (..))
import qualified System.FilePath.Posix
import qualified System.FilePath.Windows
import           System.FS.API

import qualified Database.LSMTree.Internal.CRC32C as CRC



newtype SessionRoot = SessionRoot { getSessionRoot :: FsPath }

lockFile :: SessionRoot -> FsPath
lockFile (SessionRoot dir) = dir </> mkFsPath ["lock"]

activeDir :: SessionRoot -> FsPath
activeDir (SessionRoot dir) = dir </> mkFsPath ["active"]

runPath :: SessionRoot -> RunNumber -> RunFsPaths
runPath root = RunFsPaths (activeDir root)

snapshotsDir :: SessionRoot -> FsPath
snapshotsDir (SessionRoot dir) = dir </> mkFsPath ["snapshots"]

snapshot :: SessionRoot -> SnapshotName -> FsPath
snapshot root (MkSnapshotName name) = snapshotsDir root </> mkFsPath [name]

{-------------------------------------------------------------------------------
  Snapshot name
-------------------------------------------------------------------------------}

newtype SnapshotName = MkSnapshotName FilePath
  deriving stock (Eq, Ord)

instance Show SnapshotName where
  showsPrec d (MkSnapshotName p) = showsPrec d p

-- | This instance uses 'mkSnapshotName', so all the restrictions on snap shot names apply here too. An invalid snapshot name will lead to an error.
instance IsString SnapshotName where
  fromString s = fromMaybe bad (mkSnapshotName s)
    where
      bad = error ("SnapshotName.fromString: invalid name " ++ show s)

-- | Create snapshot name.
--
-- The name may consist of lowercase characters, digits, dashes @-@ and underscores @_@.
-- It must be non-empty and less than 65 characters long.
-- It may not be a special filepath name.
--
-- >>> mkSnapshotName "main"
-- Just "main"
--
-- >>> mkSnapshotName "temporary-123-test_"
-- Just "temporary-123-test_"
--
-- >>> map mkSnapshotName ["UPPER", "dir/dot.exe", "..", "\\", "com1", "", replicate 100 'a']
-- [Nothing,Nothing,Nothing,Nothing,Nothing,Nothing,Nothing]
--
mkSnapshotName :: String -> Maybe SnapshotName
mkSnapshotName s
  | all isValid s
  , len > 0
  , len < 65
  , System.FilePath.Posix.isValid s
  , System.FilePath.Windows.isValid s
  = Just (MkSnapshotName s)

  | otherwise
  = Nothing
  where
    len = length s
    isValid c = ('a' <= c && c <= 'z') || ('0' <= c && c <= '9' ) || c `elem` "-_"

{-------------------------------------------------------------------------------
  Table paths
-------------------------------------------------------------------------------}

-- | The file name for a table's write buffer blob file
tableBlobPath :: SessionRoot -> Unique -> FsPath
tableBlobPath session n =
    activeDir session </> mkFsPath [show (uniqueToWord64 n)] <.> "wbblobs"

{-------------------------------------------------------------------------------
  Run paths
-------------------------------------------------------------------------------}

-- | The (relative) file path locations of all the files used by the run:
--
-- The following files exist for a run:
--
-- 1. @${n}.keyops@: the sorted run of key\/operation pairs
-- 2. @${n}.blobs@:  the blob values associated with the key\/operations
-- 3. @${n}.filter@: a Bloom filter of all the keys in the run
-- 4. @${n}.index@:  an index from keys to disk page numbers
-- 5. @${n}.checksums@: a file listing the crc32c checksums of the other
--    files
--
-- The representation doesn't store the full, name, just the number @n@. Use
-- the accessor functions to get the actual names.
--
data RunFsPaths = RunFsPaths {
    -- | The directory that run files live in.
    runDir    :: !FsPath
  , runNumber :: !RunNumber }
  deriving stock Show

instance NFData RunFsPaths where
  rnf (RunFsPaths x y) = rnf x `seq` rnf y

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
runFilePathWithExt (RunFsPaths dir n) ext =
    dir </> mkFsPath [show n] <.> ext

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
  deriving stock (Show, Foldable, Functor, Traversable)

instance Applicative ForRunFiles where
  pure x = ForRunFiles x x x x
  ForRunFiles f1 f2 f3 f4 <*> ForRunFiles x1 x2 x3 x4 =
    ForRunFiles (f1 x1) (f2 x2) (f3 x3) (f4 x4)
