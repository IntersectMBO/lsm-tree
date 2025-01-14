{-# OPTIONS_GHC -Wno-orphans #-}

-- TODO: upstream to fs-sim
module Test.FS (tests) where

import           Control.Concurrent.Class.MonadSTM (MonadSTM (atomically))
import           Control.Concurrent.Class.MonadSTM.Strict.TMVar
import           Control.Monad
import           Control.Monad.IOSim (runSimOrThrow)
import           Data.Char (isAsciiLower, isAsciiUpper)
import qualified Data.List as List
import qualified Data.Text as Text
import           GHC.Generics (Generic)
import           System.FS.API
import           System.FS.Sim.Error
import qualified System.FS.Sim.MockFS as MockFS
import qualified System.FS.Sim.Stream as S
import           System.FS.Sim.Stream (InternalInfo (..), Stream (..))
import           Test.QuickCheck
import           Test.QuickCheck.Classes (eqLaws)
import           Test.QuickCheck.Instances ()
import           Test.Tasty
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.FS
import           Test.Util.QC

tests :: TestTree
tests = testGroup "Test.FS" [
      -- * Simulated file system properties
      testProperty "prop_numOpenHandles" prop_numOpenHandles
    , testProperty "prop_numDirEntries" prop_numDirEntries
      -- * Equality
    , testClassLaws "Stream" $
        eqLaws (Proxy @(Stream Int))
    , testClassLaws "Errors" $
        eqLaws (Proxy @Errors)
    ]

{-------------------------------------------------------------------------------
  Simulated file system properties
-------------------------------------------------------------------------------}

newtype Path = Path FsPath
  deriving stock (Show, Eq)

newtype UniqueList a = UniqueList [a]
  deriving stock Show

instance (Arbitrary a, Eq a) => Arbitrary (UniqueList a) where
  arbitrary = do
      xs <- arbitrary
      pure (UniqueList (List.nub xs))
  shrink (UniqueList []) = []
  shrink (UniqueList xs) = UniqueList . List.nub <$> shrink xs

instance Arbitrary Path where
  arbitrary = Path . mkFsPath . (:[]) <$> ((:) <$> genChar <*> listOf genChar)
    where
      genChar = elements (['A'..'Z'] ++ ['a'..'z'])
  shrink (Path p) = case fsPathToList p of
      [] -> []
      t:_ -> [
          Path p'
        | t' <- shrink t
        , let t'' = Text.filter (\c -> isAsciiUpper c || isAsciiLower c) t'
        , not (Text.null t'')
        , let p' = fsPathFromList [t']
        ]

-- | Sanity check for 'propNoOpenHandles' and 'propNumOpenHandles'
prop_numOpenHandles :: UniqueList Path -> Property
prop_numOpenHandles (UniqueList paths) = runSimOrThrow $
    withSimHasFS propTrivial MockFS.empty $ \hfs fsVar -> do
      -- No open handles initially
      fs <- atomically $ readTMVar fsVar
      let prop = propNoOpenHandles fs

      -- Open n handles
      hs <- forM paths $ \(Path p) -> hOpen hfs p (WriteMode MustBeNew)

      -- Now there should be precisely n open handles
      fs' <- atomically $ readTMVar fsVar
      let prop' = propNumOpenHandles n fs'

      -- Close all previously opened handles
      forM_ hs $ hClose hfs

      -- No open handles again
      fs'' <- atomically $ readTMVar fsVar
      let prop'' = propNoOpenHandles fs''

      pure (prop .&&. prop' .&&. prop'')
  where
    n = length paths

-- | Sanity check for 'propNoDirEntries' and 'propNumDirEntries'
prop_numDirEntries :: Path -> InfiniteList Bool -> UniqueList Path -> Property
prop_numDirEntries (Path dir) isFiles (UniqueList paths) = runSimOrThrow $
    withSimHasFS propTrivial MockFS.empty $ \hfs fsVar -> do
      createDirectoryIfMissing hfs False dir

      -- No entries initially
      fs <- atomically $ readTMVar fsVar
      let prop = propNoDirEntries dir fs

      -- Create n entries
      forM_ xs $ \(isFile, Path p) ->
        if isFile
          then withFile hfs (dir </> p) (WriteMode MustBeNew) $ \_ -> pure ()
          else createDirectory hfs (dir </> p)

      -- Now there should be precisely n entries
      fs' <- atomically $ readTMVar fsVar
      let prop' = propNumDirEntries dir n fs'

      -- Remove n entries
      forM_ xs $ \(isFile, Path p) ->
        if isFile
          then removeFile hfs (dir </> p)
          else removeDirectoryRecursive hfs (dir </> p)

      -- No entries again
      fs'' <- atomically $ readTMVar fsVar
      let prop'' = propNoDirEntries dir fs''

      pure (prop .&&. prop' .&&. prop'')
  where
    n = length paths
    xs = zip (getInfiniteList isFiles) paths

{-------------------------------------------------------------------------------
  Equality
-------------------------------------------------------------------------------}

-- | This is not a fully lawful instance, because it uses 'approximateEqStream'.
instance Eq a => Eq (Stream a) where
  (==) = approximateEqStream

instance Arbitrary a => Arbitrary (Stream a) where
  arbitrary = oneof [
        S.genFinite arbitrary
      , S.genInfinite arbitrary
      ]
  shrink s = S.liftShrinkStream shrink s

deriving stock instance Generic (Stream a)
deriving anyclass instance CoArbitrary a => CoArbitrary (Stream a)
deriving anyclass instance Function a => Function (Stream a)

deriving stock instance Generic InternalInfo
deriving anyclass instance Function InternalInfo
deriving anyclass instance CoArbitrary InternalInfo

-- | This is not a fully lawful instance, because it uses 'approximateEqStream'.
deriving stock instance Eq Errors
deriving stock instance Generic Errors
deriving anyclass instance Function Errors
deriving anyclass instance CoArbitrary Errors

deriving stock instance Generic FsErrorType
deriving anyclass instance Function FsErrorType
deriving anyclass instance CoArbitrary FsErrorType

deriving stock instance Eq Partial
deriving stock instance Generic Partial
deriving anyclass instance Function Partial
deriving anyclass instance CoArbitrary Partial

deriving stock instance Eq PutCorruption
deriving stock instance Generic PutCorruption
deriving anyclass instance Function PutCorruption
deriving anyclass instance CoArbitrary PutCorruption

deriving stock instance Eq Blob
deriving stock instance Generic Blob
deriving anyclass instance Function Blob
deriving anyclass instance CoArbitrary Blob
