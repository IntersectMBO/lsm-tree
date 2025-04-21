module Test.Database.LSMTree.Parallel (tests) where

import           Control.Monad.Class.MonadAsync
import           Control.Tracer
import qualified Data.Map.Strict as Map
import           Data.Semigroup
import qualified Data.Vector as V
import           Data.Void
import           Database.LSMTree
import qualified System.FS.API as FS
import           Test.Database.LSMTree.UnitTests (ignoreBlobRef)
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Parallel" [
      testProperty "prop_concurrentUpserts" $
        forAllShrink genTinyAllocNumEntries shrinkTinyAllocNumEntries
          prop_concurrentUpserts
    ]

{-------------------------------------------------------------------------------
  Concurrent upserts on one table
-------------------------------------------------------------------------------}

prop_concurrentUpserts ::
     WriteBufferAlloc
  -> ParallelShrink
  -> V.Vector (Key, Value)
  -> [V.Vector (Key, Value)]
  -> V.Vector Key
  -> Property
prop_concurrentUpserts wbAlloc (ParallelShrink n) setupBatch parBatches lookupBatch =
    conjoin $ replicate n $
    ioProperty $
    withTempIOHasBlockIO "prop_concurrentUpserts" $ \hfs hbio ->
    withSession nullTracer hfs hbio (FS.mkFsPath []) $ \sess ->
    withTableWith @_ @Key @Value @Blob conf sess $ \table -> do
      upserts table setupBatch
      forConcurrently_ parBatches $ upserts table
      vs <- lookups table lookupBatch
      pure $ V.map modelLookup lookupBatch === V.map ignoreBlobRef vs
  where
    conf = defaultTableConfig { confWriteBufferAlloc = wbAlloc }

    modelTable =
        let ms = fromBatch setupBatch : fmap fromBatch parBatches
        in  Map.unionsWith resolve ms
      where
        fromBatch = Map.fromListWith resolve . V.toList

    modelLookup k = case Map.lookup k modelTable of
        Nothing -> NotFound
        Just v  -> Found v

newtype Key = Key Int
  deriving stock (Show, Eq, Ord)
  deriving Arbitrary via Small Int
  deriving newtype SerialiseKey

newtype Value = Value Int
  deriving stock (Show, Eq, Ord)
  deriving newtype SerialiseValue
  deriving ResolveValue via ResolveViaSemigroup (Sum Int)
  deriving Arbitrary via Int

newtype Blob = Blob Void
  deriving newtype SerialiseValue

genTinyAllocNumEntries :: Gen WriteBufferAlloc
genTinyAllocNumEntries = AllocNumEntries <$> elements [1..5]

shrinkTinyAllocNumEntries :: WriteBufferAlloc -> [WriteBufferAlloc]
shrinkTinyAllocNumEntries (AllocNumEntries x) =
    [ AllocNumEntries x' | Positive x' <- shrink (Positive x), x' >= 2]

newtype ParallelShrink = ParallelShrink Int
  deriving stock Show

instance Arbitrary ParallelShrink where
  arbitrary = pure (ParallelShrink 1)
  shrink (ParallelShrink n)
    | n == 1 = [ParallelShrink 100]
    | otherwise = []
