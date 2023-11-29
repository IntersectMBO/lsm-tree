{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeApplications    #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Run.Index.Compact (tests) where

import           Control.Monad (forM)
import           Control.Monad.ST.Strict (runST)
import           Data.Word (Word64)
import           Database.LSMTree.Generators
import           Database.LSMTree.Internal.Run.Index.Compact
import           Test.QuickCheck
import           Test.Tasty (TestTree, adjustOption, testGroup)
import           Test.Tasty.QuickCheck (QuickCheckMaxSize (..), testProperty)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Run.Index.Compact" [
      -- Increasing the maximum size has the effect of generating more
      -- interesting numbers of partitioned pages. With a max size of 100, the
      -- tests are very likely to generate only 1 partitioned page.
      adjustOption (const $ QuickCheckMaxSize 5000) $
      testGroup "Contruction, searching, chunking" [
        testProperty "prop_searchMinMaxKeysAfterConstruction" $
        prop_searchMinMaxKeysAfterConstruction @Word64
      , testProperty "prop_chunksPreserveConstruction" $
          prop_chunksPreserveConstruction @Word64
      ]
    ]

{-------------------------------------------------------------------------------
  Properties
-------------------------------------------------------------------------------}

-- | After construction, searching for the minimum/maximum key of every page
-- @pageNr@ returns the @pageNr@.
--
-- Example: @search minKey (fromList rfprec [(minKey, maxKey)]) == 0@.
prop_searchMinMaxKeysAfterConstruction ::
     (SliceBits k, Integral k)
  => Pages k
  -> Property
prop_searchMinMaxKeysAfterConstruction pps@(Pages (RFPrecision rfprec) ks) =
      classify (hasClashes ci) "Compact index contains clashes"
    $ labelPages pps
    $ counterexample (show idxs)
    $ property $ all p idxs
  where
    ci = fromList rfprec ks

    f idx (minKey, maxKey) =
        ( idx
        , search minKey ci
        , search maxKey ci
        , search (minKey + (maxKey - minKey) `div` 2) ci
        )

    idxs = zipWith f [0..] ks

    p (idx, x, y, z) =
         Just idx == x && Just idx == y && Just idx == z

prop_chunksPreserveConstruction ::
     (SliceBits k, Integral k, Show k)
  => ChunkSize
  -> Pages k
  -> Property
prop_chunksPreserveConstruction
  (ChunkSize csize)
  pps@(Pages (RFPrecision rfprec) ks)  =
      labelPages pps
    $ ci === ci'
  where
    mci          = fromList' rfprec ks
    ci           = runST $ unsafeFreeze =<< mci
    nks          = length ks
    (nchunks, r) = nks `divMod` csize
    slices       = [(i * csize, csize) | i <- [0..nchunks-1]] ++ [(nchunks * csize, r)]
    ci'          = runST $ do
      mci0 <- mci
      cs <- forM slices $ \(i, n) ->
        sliceChunk i n mci0
      fc <- getFinalChunk mci0
      pure $ fromChunks cs fc

deriving instance Eq k => Eq (CompactIndex k)
deriving instance Show k => Show (CompactIndex k)
