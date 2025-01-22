{-# LANGUAGE PatternSynonyms #-}
module Database.LSMTree.Internal.Paths (
    SessionRoot (..)
  , lockFile
  , ActiveDir (..)
  , activeDir
  , runPath
  , snapshotsDir
  , NamedSnapshotDir (..)
  , namedSnapshotDir
  , SnapshotMetaDataFile (..)
  , snapshotMetaDataFile
  , SnapshotMetaDataChecksumFile (..)
  , snapshotMetaDataChecksumFile
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
    -- * Checksums for Run files
  , checksumFileNamesForRunFiles
  , toChecksumsFile
  , fromChecksumsFile
    -- * Checksums for WriteBuffer files
  , toChecksumsFileForWriteBufferFiles
  , fromChecksumsFileForWriteBufferFiles
    -- * ForRunFiles abstraction
  , ForKOps (..)
  , ForBlob (..)
  , ForFilter (..)
  , ForIndex (..)
  , ForRunFiles (..)
  , forRunKOpsRaw
  , forRunBlobRaw
  , forRunFilterRaw
  , forRunIndexRaw
    -- * WriteBuffer paths
  , WriteBufferFsPaths (WrapRunFsPaths, WriteBufferFsPaths, writeBufferDir, writeBufferNumber)
  , writeBufferKOpsPath
  , writeBufferBlobPath
  , writeBufferChecksumsPath
  , writeBufferFilePathWithExt
  ) where

import           Control.Applicative (Applicative (..))
import           Control.DeepSeq (NFData (..))
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.ByteString.Char8 as BS
import           Data.Foldable (toList)
import           Data.Function ((&))
import qualified Data.Map as Map
import           Data.Maybe (fromMaybe)
import           Data.String (IsString (..))
import           Data.Traversable (for)
import qualified Database.LSMTree.Internal.CRC32C as CRC
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.UniqCounter
import           Prelude hiding (Applicative (..))
import qualified System.FilePath.Posix
import qualified System.FilePath.Windows
import           System.FS.API



newtype SessionRoot = SessionRoot { getSessionRoot :: FsPath }
  deriving stock Eq

lockFile :: SessionRoot -> FsPath
lockFile (SessionRoot dir) = dir </> mkFsPath ["lock"]

newtype ActiveDir = ActiveDir { getActiveDir :: FsPath }

activeDir :: SessionRoot -> ActiveDir
activeDir (SessionRoot dir) = ActiveDir (dir </> mkFsPath ["active"])

runPath :: SessionRoot -> RunNumber -> RunFsPaths
runPath root = RunFsPaths (getActiveDir (activeDir root))

snapshotsDir :: SessionRoot -> FsPath
snapshotsDir (SessionRoot dir) = dir </> mkFsPath ["snapshots"]

-- | The directory for a specific, /named/ snapshot.
--
-- Not to be confused with the snapshot/s/ directory, which holds all named
-- snapshot directories.
newtype NamedSnapshotDir = NamedSnapshotDir { getNamedSnapshotDir :: FsPath }

namedSnapshotDir :: SessionRoot -> SnapshotName -> NamedSnapshotDir
namedSnapshotDir root (MkSnapshotName name) =
    NamedSnapshotDir (snapshotsDir root </> mkFsPath [name])

newtype SnapshotMetaDataFile = SnapshotMetaDataFile FsPath

snapshotMetaDataFile :: NamedSnapshotDir -> SnapshotMetaDataFile
snapshotMetaDataFile (NamedSnapshotDir dir) =
    SnapshotMetaDataFile (dir </> mkFsPath ["metadata"])

newtype SnapshotMetaDataChecksumFile = SnapshotMetaDataChecksumFile FsPath

snapshotMetaDataChecksumFile :: NamedSnapshotDir -> SnapshotMetaDataChecksumFile
snapshotMetaDataChecksumFile (NamedSnapshotDir dir) =
    SnapshotMetaDataChecksumFile (dir </> mkFsPath ["metadata.checksum"])

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
    getActiveDir (activeDir session) </> mkFsPath [show (uniqueToInt n)] <.> "wbblobs"

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
runKOpsPath = unForKOps . forRunKOps . pathsForRunFiles

runBlobPath :: RunFsPaths -> FsPath
runBlobPath = unForBlob . forRunBlob . pathsForRunFiles

runFilterPath :: RunFsPaths -> FsPath
runFilterPath = unForFilter . forRunFilter . pathsForRunFiles

runIndexPath :: RunFsPaths -> FsPath
runIndexPath = unForIndex . forRunIndex . pathsForRunFiles

runChecksumsPath :: RunFsPaths -> FsPath
runChecksumsPath = flip runFilePathWithExt "checksums"

runFilePathWithExt :: RunFsPaths -> String -> FsPath
runFilePathWithExt (RunFsPaths dir n) ext =
    dir </> mkFsPath [show n] <.> ext

runFileExts :: ForRunFiles String
runFileExts = ForRunFiles {
      forRunKOps   = ForKOps "keyops"
    , forRunBlob   = ForBlob "blobs"
    , forRunFilter = ForFilter "filter"
    , forRunIndex  = ForIndex "index"
    }

{-------------------------------------------------------------------------------
  Checksums For Run Files
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
  Marker newtypes for individual elements of the ForRunFiles and the
  ForWriteBufferFiles abstractions
-------------------------------------------------------------------------------}

newtype ForKOps a = ForKOps {unForKOps :: a}
  deriving stock (Show, Foldable, Functor, Traversable)

newtype ForBlob a = ForBlob {unForBlob :: a}
  deriving stock (Show, Foldable, Functor, Traversable)

newtype ForFilter a = ForFilter {unForFilter :: a}
  deriving stock (Show, Foldable, Functor, Traversable)

newtype ForIndex a = ForIndex {unForIndex :: a}
  deriving stock (Show, Foldable, Functor, Traversable)

{-------------------------------------------------------------------------------
  ForRunFiles abstraction
-------------------------------------------------------------------------------}

-- | Stores someting for each run file (except the checksums file), allowing to
-- easily do something for all of them without mixing them up.
data ForRunFiles a = ForRunFiles {
      forRunKOps   :: !(ForKOps a)
    , forRunBlob   :: !(ForBlob a)
    , forRunFilter :: !(ForFilter a)
    , forRunIndex  :: !(ForIndex a)
    }
  deriving stock (Show, Foldable, Functor, Traversable)

forRunKOpsRaw :: ForRunFiles a -> a
forRunKOpsRaw = unForKOps . forRunKOps

forRunBlobRaw :: ForRunFiles a -> a
forRunBlobRaw = unForBlob . forRunBlob

forRunFilterRaw :: ForRunFiles a -> a
forRunFilterRaw = unForFilter . forRunFilter

forRunIndexRaw :: ForRunFiles a -> a
forRunIndexRaw = unForIndex . forRunIndex

instance Applicative ForRunFiles where
  pure x = ForRunFiles (ForKOps x) (ForBlob x) (ForFilter x) (ForIndex x)
  ForRunFiles (ForKOps f1) (ForBlob f2) (ForFilter f3) (ForIndex f4) <*> ForRunFiles (ForKOps x1) (ForBlob x2) (ForFilter x3) (ForIndex x4) =
    ForRunFiles (ForKOps $ f1 x1) (ForBlob $ f2 x2) (ForFilter $ f3 x3) (ForIndex $ f4 x4)

{-------------------------------------------------------------------------------
  WriteBuffer paths
-------------------------------------------------------------------------------}

newtype WriteBufferFsPaths = WrapRunFsPaths RunFsPaths

pattern WriteBufferFsPaths :: FsPath -> RunNumber -> WriteBufferFsPaths
pattern WriteBufferFsPaths {writeBufferDir, writeBufferNumber} = WrapRunFsPaths (RunFsPaths writeBufferDir writeBufferNumber)

{-# COMPLETE WriteBufferFsPaths #-}

writeBufferKOpsExt :: String
writeBufferKOpsExt = unForKOps . forRunKOps $ runFileExts

writeBufferBlobExt :: String
writeBufferBlobExt = unForBlob . forRunBlob $ runFileExts

writeBufferKOpsPath :: WriteBufferFsPaths -> FsPath
writeBufferKOpsPath = flip writeBufferFilePathWithExt writeBufferKOpsExt

writeBufferBlobPath :: WriteBufferFsPaths -> FsPath
writeBufferBlobPath = flip writeBufferFilePathWithExt writeBufferBlobExt

writeBufferChecksumsPath :: WriteBufferFsPaths -> FsPath
writeBufferChecksumsPath = flip writeBufferFilePathWithExt "checksums"

writeBufferFilePathWithExt :: WriteBufferFsPaths -> String -> FsPath
writeBufferFilePathWithExt (WriteBufferFsPaths dir n) ext =
    dir </> mkFsPath [show n] <.> ext


{-------------------------------------------------------------------------------
  Checksums For Run Files
-------------------------------------------------------------------------------}

toChecksumsFileForWriteBufferFiles :: (ForKOps CRC.CRC32C, ForBlob CRC.CRC32C) -> CRC.ChecksumsFile
toChecksumsFileForWriteBufferFiles checksums =
  Map.fromList . toList $ checksums & bimap
    ((toChecksumsFileName writeBufferKOpsExt,) . unForKOps)
    ((toChecksumsFileName writeBufferBlobExt,) . unForBlob)
  where
    toChecksumsFileName :: String -> CRC.ChecksumsFileName
    toChecksumsFileName = CRC.ChecksumsFileName . BS.pack

fromChecksumsFileForWriteBufferFiles :: CRC.ChecksumsFile -> Either String (ForKOps CRC.CRC32C, ForBlob CRC.CRC32C)
fromChecksumsFileForWriteBufferFiles file = do
  forKOps <- maybe (Left $ "key not found: " <> writeBufferKOpsExt) Right (Map.lookup (CRC.ChecksumsFileName . fromString $ writeBufferKOpsExt) file)
  forBlob <- maybe (Left $ "key not found: " <> writeBufferBlobExt) Right (Map.lookup (CRC.ChecksumsFileName . fromString $ writeBufferBlobExt) file)
  pure (ForKOps forKOps, ForBlob forBlob)
