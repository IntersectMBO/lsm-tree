{-# OPTIONS_GHC -Wno-orphans #-}

{- HLINT ignore "Redundant if" -}

module Test.Util.FS (
    -- * Real file system
    withTempIOHasFS
  , withTempIOHasBlockIO
    -- * Simulated file system
  , withSimHasFS
  , withSimHasBlockIO
    -- * Simulated file system with errors
  , withSimErrorHasFS
  , withSimErrorHasBlockIO
    -- * Simulated file system properties
  , propTrivial
  , propNumOpenHandles
  , propNoOpenHandles
  , propNumDirEntries
  , propNoDirEntries
  , assertNoOpenHandles
  , assertNumOpenHandles
    -- * Equality
  , approximateEqStream
    -- * List directory
  , DirEntry (..)
  , getDirEntry
  , listDirectoryFiles
  , listDirectoryRecursive
  , listDirectoryRecursiveFiles
    -- * Corruption
  , flipFileBit
  , hFlipBit

  , fsPathStripPrefix
  , FsTree (..)
  , FsTreeEntry (..)
  , Folder
  , fsTreeFsPaths
  , fsTreeDirEntries

    -- * Arbitrary
  , FsPathComponent (..)
  , fsPathComponentFsPath
  , fsPathComponentString
    -- ** Modifiers
  , NoCleanupErrors (..)
    -- ** Orphans
  , isPathChar
  ) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (assert)
import           Control.Monad (void)
import           Control.Monad.Class.MonadThrow (MonadCatch, MonadThrow)
import           Control.Monad.IOSim (runSimOrThrow)
import           Control.Monad.Primitive (PrimMonad)
import           Data.Bit (MVector (..), flipBit)
import           Data.Char (isAscii, isDigit, isLetter)
import           Data.Foldable (foldlM)
import qualified Data.List as List
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Primitive.ByteArray (newPinnedByteArray, setByteArray)
import           Data.Primitive.Types (sizeOf)
import           Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as T
import           Data.Tree (Tree)
import qualified Data.Tree as Tree
import           Database.LSMTree.Extras.RunData (liftShrink2Map)
import           GHC.Stack
import           System.FS.API as FS
import           System.FS.BlockIO.API
import           System.FS.BlockIO.IO
import           System.FS.BlockIO.Sim (fromHasFS)
import           System.FS.IO
import           System.FS.Sim.Error
import           System.FS.Sim.MockFS (HandleMock, MockFS, numOpenHandles,
                     openHandles, pretty)
import           System.FS.Sim.STM
import qualified System.FS.Sim.Stream as Stream
import           System.FS.Sim.Stream (InternalInfo (..), Stream (..))
import           System.IO.Temp
import           Test.QuickCheck
import           Test.QuickCheck.Instances ()
import           Text.Printf

{-------------------------------------------------------------------------------
  Real file system
-------------------------------------------------------------------------------}

withTempIOHasFS :: FilePath -> (HasFS IO HandleIO -> IO a) -> IO a
withTempIOHasFS path action = withSystemTempDirectory path $ \dir -> do
    let hfs = ioHasFS (MountPoint dir)
    action hfs

withTempIOHasBlockIO :: FilePath -> (HasFS IO HandleIO -> HasBlockIO IO HandleIO -> IO a) -> IO a
withTempIOHasBlockIO path action =
    withTempIOHasFS path $ \hfs -> do
      withIOHasBlockIO hfs defaultIOCtxParams (action hfs)

{-------------------------------------------------------------------------------
  Simulated file system
-------------------------------------------------------------------------------}

{-# INLINABLE withSimHasFS #-}
withSimHasFS ::
     (MonadSTM m, MonadThrow m, PrimMonad m, Testable prop1, Testable prop2)
  => (MockFS -> prop1)
  -> MockFS
  -> (  HasFS m HandleMock
     -> StrictTMVar m MockFS
     -> m prop2
     )
  -> m Property
withSimHasFS post fs k = do
    var <- newTMVarIO fs
    let hfs = simHasFS var
    x <- k hfs var
    fs' <- atomically $ readTMVar var
    pure (x .&&. post fs')

{-# INLINABLE withSimHasBlockIO #-}
withSimHasBlockIO ::
     (MonadMVar m, MonadSTM m, MonadCatch m, PrimMonad m, Testable prop1, Testable prop2)
  => (MockFS -> prop1)
  -> MockFS
  -> (  HasFS m HandleMock
     -> HasBlockIO m HandleMock
     -> StrictTMVar m MockFS
     -> m prop2
     )
  -> m Property
withSimHasBlockIO post fs k = do
    withSimHasFS post fs $ \hfs fsVar -> do
      hbio <- fromHasFS hfs
      k hfs hbio fsVar

{-------------------------------------------------------------------------------
  Simulated file system with errors
-------------------------------------------------------------------------------}

{-# INLINABLE withSimErrorHasFS #-}
withSimErrorHasFS ::
     (MonadSTM m, MonadThrow m, PrimMonad m, Testable prop1, Testable prop2)
  => (MockFS -> prop1)
  -> MockFS
  -> Errors
  -> (  HasFS m HandleMock
     -> StrictTMVar m MockFS
     -> StrictTVar m Errors
     -> m prop2
     )
  -> m Property
withSimErrorHasFS post fs errs k = do
    fsVar <- newTMVarIO fs
    errVar <- newTVarIO errs
    let hfs = simErrorHasFS fsVar errVar
    x <- k hfs fsVar errVar
    fs' <- atomically $ readTMVar fsVar
    pure (x .&&. post fs')

{-# INLINABLE withSimErrorHasBlockIO #-}
withSimErrorHasBlockIO ::
     ( MonadSTM m, MonadCatch m, MonadMVar m, PrimMonad m
     , Testable prop1, Testable prop2
     )
  => (MockFS -> prop1)
  -> MockFS
  -> Errors
  -> (  HasFS m HandleMock
     -> HasBlockIO m HandleMock
     -> StrictTMVar m MockFS
     -> StrictTVar m Errors
     -> m prop2
     )
  -> m Property
withSimErrorHasBlockIO post fs errs k =
    withSimErrorHasFS post fs errs $ \hfs fsVar errsVar -> do
      hbio <- fromHasFS hfs
      k hfs hbio fsVar errsVar

{-------------------------------------------------------------------------------
  Simulated file system properties
-------------------------------------------------------------------------------}

propTrivial :: MockFS -> Property
propTrivial _ = property True

{-# INLINABLE propNumOpenHandles #-}
propNumOpenHandles :: Int -> MockFS -> Property
propNumOpenHandles expected fs =
    counterexample (printf "Expected %d open handles, but found %d" expected actual) $
    counterexample ("Open handles: " <> show (openHandles fs)) $
    printMockFSOnFailure fs $
    expected == actual
  where actual = numOpenHandles fs

{-# INLINABLE propNoOpenHandles #-}
propNoOpenHandles :: MockFS -> Property
propNoOpenHandles fs = propNumOpenHandles 0 fs

{-# INLINABLE propNumDirEntries #-}
propNumDirEntries :: FsPath -> Int -> MockFS -> Property
propNumDirEntries path expected fs =
    counterexample
      (printf "Expected %d entries in the directory at %s, but found %d"
        expected
        (show path) actual) $
    printMockFSOnFailure fs $
    expected === actual
  where
    actual =
      let (contents, _) = runSimOrThrow $
            runSimFS fs $ \hfs ->
              FS.listDirectory hfs path
      in  Set.size contents

{-# INLINABLE propNoDirEntries #-}
propNoDirEntries :: FsPath -> MockFS -> Property
propNoDirEntries path fs = propNumDirEntries path 0 fs

printMockFSOnFailure :: Testable prop => MockFS -> prop -> Property
printMockFSOnFailure fs = counterexample ("Mocked file system: " <> pretty fs)

assertNoOpenHandles :: HasCallStack => MockFS -> a -> a
assertNoOpenHandles fs = assertNumOpenHandles fs 0

assertNumOpenHandles :: HasCallStack => MockFS -> Int -> a -> a
assertNumOpenHandles fs m =
    assert $
      if n /= m then
        error (printf "Expected %d open handles, but found %d" m n)
      else
        True
  where n = numOpenHandles fs

{-------------------------------------------------------------------------------
  Equality
-------------------------------------------------------------------------------}

-- | Approximate equality for streams.
--
-- Equality is checked as follows:
-- * Infinite streams are equal: any infinity is as good as another infinity
-- * Finite streams are are checked for pointwise equality on their elements.
-- * Other streams are trivially unequal: they do not have matching finiteness
--
-- This approximate equality satisfies the __Reflexivity__, __Symmetry__,
-- __Transitivity__ and __Negation__ laws for the 'Eq' class, but not
-- __Substitutivity.
--
-- TODO: upstream to fs-sim
approximateEqStream :: Eq a => Stream a -> Stream a -> Bool
approximateEqStream (UnsafeStream infoXs xs) (UnsafeStream infoYs ys) =
    case (infoXs, infoYs) of
      (Infinite, Infinite) -> True
      (Finite, Finite)     ->  xs == ys
      (_, _)               -> False

{-------------------------------------------------------------------------------
  List directory
-------------------------------------------------------------------------------}

data DirEntry a = Directory a | File a
  deriving stock (Show, Eq, Ord, Functor)


getDirEntry :: DirEntry a -> a
getDirEntry (Directory x) = x
getDirEntry (File x)      = x

instance Arbitrary a => Arbitrary (DirEntry a) where
  arbitrary = ($) <$> elements [Directory, File] <*> arbitrary
  shrink (Directory x) = File x : (Directory <$> shrink x)
  shrink (File x)      = File <$> shrink x

-- | List all files and directories in the given directory and recusively in all
-- sub-directories.
listDirectoryRecursive ::
     Monad m
  => HasFS m h
  -> FsPath
  -> m (Set (DirEntry FsPath))
listDirectoryRecursive hfs = go Set.empty (mkFsPath [])
  where
    go !acc relPath absPath = do
        pcs <- listDirectory hfs absPath
        foldlM (\acc' -> go' acc' relPath absPath) acc pcs

    go' !acc relPath absPath pc = do
        let
          p = mkFsPath [pc]
          relPath' = relPath </> p
          absPath' = absPath </> p
        isFile <- doesFileExist hfs absPath'
        if isFile then
          pure (File relPath' `Set.insert` acc)
        else do
          isDirectory <- doesDirectoryExist hfs absPath'
          if isDirectory then
            go (Directory relPath' `Set.insert` acc) relPath' absPath'
          else
            error "impossible"

-- | List files in the given directory and recursively in all sub-directories.
listDirectoryRecursiveFiles ::
     Monad m
  => HasFS m h
  -> FsPath
  -> m (Set FsPath)
listDirectoryRecursiveFiles hfs dir = do
    dirEntries <- listDirectoryRecursive hfs dir
    foldlM f Set.empty dirEntries
  where
    f !acc (File p) = pure $ Set.insert p acc
    f !acc _        = pure acc

-- | List files in the given directory
listDirectoryFiles ::
     Monad m
  => HasFS m h
  -> FsPath
  -> m (Set FsPath)
listDirectoryFiles hfs = go Set.empty
  where
    go !acc absPath = do
        pcs <- listDirectory hfs absPath
        foldlM go' acc pcs

    go' !acc pc = do
        let path = mkFsPath [pc]
        isFile <- doesFileExist hfs path
        if isFile then
          pure (path `Set.insert` acc)
        else
          pure acc

{-------------------------------------------------------------------------------
  Corruption
-------------------------------------------------------------------------------}

-- | Flip a single bit in the given file.
flipFileBit :: (MonadThrow m, PrimMonad m) => HasFS m h -> FsPath -> Int -> m ()
flipFileBit hfs p bitOffset =
    withFile hfs p (ReadWriteMode AllowExisting) $ \h -> hFlipBit hfs h bitOffset

-- | Flip a single bit in the file pointed to by the given handle.
hFlipBit ::
     (MonadThrow m, PrimMonad m)
  => HasFS m h
  -> Handle h
  -> Int -- ^ Bit offset
  -> m ()
hFlipBit hfs h bitOffset = do
    -- Create an empty buffer initialised to all 0 bits. The buffer must have at
    -- least the size of a machine word.
    let n = sizeOf (0 :: Word)
    buf <- newPinnedByteArray n
    setByteArray buf 0 n (0 :: Word)
    -- Read the bit at the given offset
    let (byteOffset, i) = bitOffset `quotRem` 8
        bufOff = BufferOffset 0
        count = 1
        off = AbsOffset (fromIntegral byteOffset)
    void $ hGetBufExactlyAt hfs h buf bufOff count off
    -- Flip the bit in memory, and then write it back
    let bvec = BitMVec 0 8 buf
    flipBit bvec i
    void $ hPutBufExactlyAt hfs h buf bufOff count off


{-------------------------------------------------------------------------------
  Arbitrary
-------------------------------------------------------------------------------}

fsPathStripPrefix :: FsPath -> FsPath -> Maybe FsPath
fsPathStripPrefix p1 p2 = fsPathFromList <$> fsPathToList p1 `List.stripPrefix` fsPathToList p2

--
-- FsTree
--

-- | Simple in-memory representation of a file system
--
-- Copied from "System.FS.Sim.FsTree"
newtype FsTree a b = FsTree (Folder a b)
  deriving stock (Show, Eq, Functor)

data FsTreeEntry a b = FsTreeFile !b | FsTreeFolder !(Folder a b)
  deriving stock (Show, Eq, Functor)

type Folder a b = Map a (FsTreeEntry a b)

instance (Arbitrary a, Ord a, Arbitrary b) => Arbitrary (FsTree a b) where
  arbitrary = liftArbitrary2FsTree arbitrary arbitrary
  shrink = liftShrink2FsTree shrink shrink

instance (Arbitrary a, Ord a) => Arbitrary1 (FsTree a) where
  liftArbitrary genB = liftArbitrary2FsTree arbitrary genB
  liftShrink shrB = liftShrink2FsTree shrink shrB

liftArbitrary2FsTree :: Ord a => Gen a -> Gen b -> Gen (FsTree a b)
liftArbitrary2FsTree genA genB = fmap treeFsTree $ liftArbitrary $ do
    x <- genA
    y <- genB
    pure (x, y)

treeFsTree :: Ord a => Tree (a, b) -> FsTree a b
treeFsTree (Tree.Node (x, _) children) = case children of
    [] -> FsTree Map.empty
    _  -> FsTree $ consFolder x children

treeFsTreeEntry :: Ord a => Tree (a, b) -> FsTreeEntry a b
treeFsTreeEntry (Tree.Node (x, my) children) = case (children, my) of
    ([], y) -> FsTreeFile y
    (_, _)  -> FsTreeFolder $ consFolder x children

consFolder :: Ord a => a -> [Tree (a, b)] -> Folder a b
consFolder x children = Map.fromList $ fmap (\child -> (x, treeFsTreeEntry child)) children

{-
liftArbitrary2FsTree :: Ord a => Gen a -> Gen b -> Gen (FsTree a b)
liftArbitrary2FsTree genA genB = scale (`div` 20) $ sized $ \n -> do
    i <- chooseInt (0, n)
    if i == 0 then
      pure $ FsTree Map.empty
    else
      FsTree <$> liftArbitrary2Map genA (liftArbitrary2FsTreeEntry n genA genB)

liftArbitrary2FsTreeEntry :: Ord a => Int -> Gen a -> Gen b -> Gen (FsTreeEntry a b)
liftArbitrary2FsTreeEntry i genA genB
  | i < 0 = error "liftArbitrary2FsTreeEntry: n < 0"
  | i == 0 = oneof [
        FsTreeFile <$> genB
      , pure (FsTreeFolder Map.empty)
      ]
  | otherwise = oneof [
        FsTreeFile <$> genB
      , FsTreeFolder <$> liftArbitrary2Map genA (liftArbitrary2FsTreeEntry (i-1) genA genB)
      ] -}

liftShrink2FsTree :: Ord a => (a -> [a]) -> (b -> [b]) -> FsTree a b -> [FsTree a b]
liftShrink2FsTree shrA shrB (FsTree folder) =
    FsTree <$> liftShrink2Map shrA (liftShrink2FsTreeEntry shrA shrB) folder

liftShrink2FsTreeEntry :: Ord a => (a -> [a]) -> (b -> [b]) -> FsTreeEntry a b -> [FsTreeEntry a b]
liftShrink2FsTreeEntry shrA shrB = \case
    FsTreeFile file -> FsTreeFile <$> shrB file
    FsTreeFolder folder ->
      let shrFolder = FsTreeFolder <$>
            liftShrink2Folder shrA shrB folder in
      case Map.toList folder of
        [(_, e)] -> e : shrFolder
        _        -> shrFolder

liftShrink2Folder :: Ord a => (a -> [a]) -> (b -> [b]) -> Folder a b -> [Folder a b]
liftShrink2Folder shrA shrB folder = liftShrink2Map shrA (liftShrink2FsTreeEntry shrA shrB) folder

fsTreeFsPaths :: FsTree FsPathComponent b -> [FsPath]
fsTreeFsPaths (FsTree folder) = folderFsPaths folder

fsTreeEntryFsPaths :: FsTreeEntry FsPathComponent b -> [FsPath]
fsTreeEntryFsPaths = \case
    FsTreeFile _ -> []
    FsTreeFolder folder -> folderFsPaths folder

folderFsPaths :: Folder FsPathComponent b -> [FsPath]
folderFsPaths folder = concatMap (uncurry f) $ Map.toAscList folder
  where
    f pc child =
        let p = fsPathComponentFsPath pc
        in  p : fmap (p </>) (fsTreeEntryFsPaths child)

fsTreeDirEntries :: FsPath -> FsTree FsPathComponent b -> [DirEntry FsPath]
fsTreeDirEntries p (FsTree folder) = folderDirEntries p folder

fsTreeEntryDirEntries :: FsPath -> FsTreeEntry FsPathComponent b -> [DirEntry FsPath]
fsTreeEntryDirEntries p = \case
    FsTreeFile _ -> [File p]
    FsTreeFolder folder -> Directory p : folderDirEntries p folder

folderDirEntries :: FsPath -> Folder FsPathComponent b -> [DirEntry FsPath]
folderDirEntries p folder = concatMap (uncurry f) $ Map.toAscList folder
  where
    f pc child = fsTreeEntryDirEntries (p </> fsPathComponentFsPath pc) child


--
-- FsPathComponent
--

newtype FsPathComponent = FsPathComponent (NonEmpty Char)
  deriving stock (Eq, Ord)

instance Show FsPathComponent where
  show = show . fsPathComponentFsPath

fsPathComponentFsPath :: FsPathComponent -> FsPath
fsPathComponentFsPath (FsPathComponent s) = FS.mkFsPath [NE.toList s]

fsPathComponentString :: FsPathComponent -> String
fsPathComponentString (FsPathComponent s) = NE.toList s

instance Arbitrary FsPathComponent where
  arbitrary = resize 5 $ -- path components don't have to be very long
      FsPathComponent <$> liftArbitrary genPathChar
  shrink (FsPathComponent s) = FsPathComponent <$> liftShrink shrinkPathChar s

{-------------------------------------------------------------------------------
  Arbitrary: modifiers
-------------------------------------------------------------------------------}

--
-- NoCleanupErrors
--

-- | No errors on closing file handles and removing files
newtype NoCleanupErrors = NoCleanupErrors Errors
  deriving stock Show

mkNoCleanupErrors :: Errors -> NoCleanupErrors
mkNoCleanupErrors errs = NoCleanupErrors $ errs {
      hCloseE = Stream.empty
    , removeFileE = Stream.empty
    }

instance Arbitrary NoCleanupErrors where
  arbitrary = do
      errs <- arbitrary
      pure $ mkNoCleanupErrors errs

  -- The shrinker for 'Errors' does not re-introduce 'hCloseE' and 'removeFile'.
  shrink (NoCleanupErrors errs) = NoCleanupErrors <$> shrink errs

{-------------------------------------------------------------------------------
  Arbitrary: orphans
-------------------------------------------------------------------------------}

instance Arbitrary FsPath where
  arbitrary = scale (`div` 10) $ -- paths don't have to be very long
      FS.mkFsPath <$> listOf (fsPathComponentString <$> arbitrary)
  shrink p =
      let ss = T.unpack <$> fsPathToList p
      in  FS.mkFsPath <$> shrinkList shrinkAsComponent ss
    where
      shrinkAsComponent s = fsPathComponentString <$>
          shrink (FsPathComponent $ NE.fromList s)

-- >>> [ c | c <- [minBound..maxBound], isPathChar c ]
-- "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
isPathChar :: Char -> Bool
isPathChar c = isAscii c && (isLetter c || isDigit c)

genPathChar :: Gen Char
genPathChar = arbitraryASCIIChar `suchThat` isPathChar

shrinkPathChar :: Char -> [Char]
shrinkPathChar c = [ c' | c' <- shrink c, isPathChar c']

instance Arbitrary OpenMode where
  arbitrary = genOpenMode
  shrink = shrinkOpenMode

genOpenMode :: Gen OpenMode
genOpenMode = oneof [
      pure ReadMode
    , WriteMode <$> genAllowExisting
    , ReadWriteMode <$> genAllowExisting
    , AppendMode <$> genAllowExisting
    ]
  where
    _coveredAllCases x = case x of
        ReadMode{}      -> ()
        WriteMode{}     -> ()
        ReadWriteMode{} -> ()
        AppendMode{}    -> ()

shrinkOpenMode :: OpenMode -> [OpenMode]
shrinkOpenMode = \case
    ReadMode -> []
    WriteMode ae ->
        ReadMode
      : (WriteMode <$> shrinkAllowExisting ae)
    ReadWriteMode ae ->
        ReadMode
      : WriteMode ae
      : (ReadWriteMode <$> shrinkAllowExisting ae)
    AppendMode ae ->
        ReadMode
      : WriteMode ae
      : ReadWriteMode ae
      : (AppendMode <$> shrinkAllowExisting ae)

instance Arbitrary AllowExisting where
  arbitrary = genAllowExisting
  shrink = shrinkAllowExisting

genAllowExisting :: Gen AllowExisting
genAllowExisting = elements [
      AllowExisting
    , MustBeNew
    ]
  where
    _coveredAllCases x = case x of
        AllowExisting -> ()
        MustBeNew     -> ()

shrinkAllowExisting :: AllowExisting -> [AllowExisting]
shrinkAllowExisting AllowExisting = []
shrinkAllowExisting MustBeNew     = [AllowExisting]
