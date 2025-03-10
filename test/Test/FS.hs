{-# OPTIONS_GHC -Wno-orphans #-}

-- TODO: upstream to fs-sim
module Test.FS (tests) where

import           Control.Concurrent.Class.MonadSTM (MonadSTM (atomically))
import           Control.Concurrent.Class.MonadSTM.Strict.TMVar
import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Control.Monad.IOSim (runSimOrThrow)
import           Control.Monad.ST (runST)
import           Data.Bit (cloneFromByteString, cloneToByteString, flipBit)
import           Data.ByteString (ByteString)
import qualified Data.ByteString as BS
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Vector.Unboxed (thaw, unsafeFreeze)
import           GHC.Generics (Generic)
import           System.FS.API
import           System.FS.API.Lazy
import           System.FS.API.Strict
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
      -- * Corruption
    , testProperty "prop_flipFileBit" prop_flipFileBit
      -- * Equality
    , testClassLaws "Stream" $
        eqLaws (Proxy @(Stream Int))
    , testClassLaws "Errors" $
        eqLaws (Proxy @Errors)
    ]

{-------------------------------------------------------------------------------
  Simulated file system properties
-------------------------------------------------------------------------------}

-- | Sanity check for 'propNoOpenHandles' and 'propNumOpenHandles'
prop_numOpenHandles :: Set FsPathComponent -> Property
prop_numOpenHandles (Set.toList -> paths) = runSimOrThrow $
    withSimHasFS propTrivial MockFS.empty $ \hfs fsVar -> do
      -- No open handles initially
      fs <- atomically $ readTMVar fsVar
      let prop = propNoOpenHandles fs

      -- Open n handles
      hs <- forM paths $ \(fsPathComponentFsPath -> p) -> hOpen hfs p (WriteMode MustBeNew)

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
prop_numDirEntries ::
     FsPathComponent
  -> InfiniteList Bool
  -> Set FsPathComponent
  -> Property
prop_numDirEntries (fsPathComponentFsPath -> dir) isFiles (Set.toList -> paths) = runSimOrThrow $
    withSimHasFS propTrivial MockFS.empty $ \hfs fsVar -> do
      createDirectoryIfMissing hfs False dir

      -- No entries initially
      fs <- atomically $ readTMVar fsVar
      let prop = propNoDirEntries dir fs

      -- Create n entries
      forM_ xs $ \(isFile, fsPathComponentFsPath -> p) ->
        if isFile
          then createFile hfs (dir </> p)
          else createDirectory hfs (dir </> p)

      -- Now there should be precisely n entries
      fs' <- atomically $ readTMVar fsVar
      let prop' = propNumDirEntries dir n fs'

      -- Remove n entries
      forM_ xs $ \(isFile, fsPathComponentFsPath -> p) ->
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

createFile :: MonadThrow m => HasFS m h -> FsPath -> m ()
createFile hfs p = withFile hfs p (WriteMode MustBeNew) $ \_ -> pure ()

{-------------------------------------------------------------------------------
  Corruption
-------------------------------------------------------------------------------}

data WithBitOffset a = WithBitOffset Int a
  deriving stock Show

bitLength :: BS.ByteString -> Int
bitLength bs = BS.length bs * 8

instance Arbitrary (WithBitOffset ByteString) where
  arbitrary = do
      bs <- arbitrary `suchThat` (\bs -> BS.length bs > 0)
      bitOffset <- chooseInt (0, bitLength bs - 1)
      pure $ WithBitOffset bitOffset bs
  shrink (WithBitOffset bitOffset bs) =
      [ WithBitOffset bitOffset' bs'
      | bs' <- shrink bs
      , BS.length bs' > 0
      , let bitOffset' = max 0 $ min (bitLength bs' - 1) bitOffset
      ] ++ [
        WithBitOffset bitOffset' bs
      | bitOffset' <- max 0 <$> shrink bitOffset
      , bitOffset' >= 0
      ]

prop_flipFileBit :: WithBitOffset ByteString -> Property
prop_flipFileBit (WithBitOffset bitOffset bs) =
    ioProperty $
    withSimHasFS propTrivial MockFS.empty $ \hfs _fsVar -> do
      void $ withFile hfs path (WriteMode MustBeNew) $ \h -> hPutAllStrict hfs h bs
      flipFileBit hfs path bitOffset
      bs' <- withFile hfs path ReadMode $ \h -> BS.toStrict <$> hGetAll hfs h
      pure (spec_flipFileBit bs bitOffset === bs')
  where
    path = mkFsPath ["file"]

spec_flipFileBit :: ByteString -> Int -> ByteString
spec_flipFileBit bs bitOffset = runST $ do
    mv <- thaw $ cloneFromByteString bs
    flipBit mv bitOffset
    v <- unsafeFreeze mv
    pure $ cloneToByteString v

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
