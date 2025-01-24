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
  , listDirectoryFiles
  , listDirectoryRecursive
  , listDirectoryRecursiveFiles
    -- * Corruption
  , flipFileBit
  , hFlipBit
    -- * Arbitrary
  , FsPathComponent (..)
  , fsPathComponentFsPath
  , fsPathComponentString
    -- ** Modifiers
  , NoCleanupErrors (..)
    -- ** Orphans
  , isPathChar
  , pathChars
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
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Primitive.ByteArray (newPinnedByteArray, setByteArray)
import           Data.Primitive.Types (sizeOf)
import           Data.Set (Set)
import qualified Data.Set as Set
import qualified Data.Text as T
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

-- TODO: test the list directory functions

data DirEntry a = Directory a | File a
  deriving stock (Show, Eq, Ord, Functor)

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
            error $ printf
              "listDirectoryRecursive: %s is not a file or directory"
              (show relPath')

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

--
-- FsPathComponent
--

-- | A single component in an 'FsPath'.
--
-- If we have a path @a/b/c/d@, then @a@, @b@ and @c@ are components, but for
-- example @a/b@ is not.
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

-- >>> all isPathChar pathChars
-- True
isPathChar :: Char -> Bool
isPathChar c = isAscii c && (isLetter c || isDigit c)

-- >>> pathChars
-- "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789"
pathChars :: [Char]
pathChars = concat [['a'..'z'], ['A'..'Z'], ['0'..'9']]

genPathChar :: Gen Char
genPathChar = elements pathChars

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
