{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TypeApplications    #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Run.Index.Compact (tests) where

import           Data.Bifunctor (Bifunctor (bimap))
import           Database.LSMTree.Generators (ChunkSize (..), Pages (..),
                     RFPrecision (..), UTxOKey (..), WithSerialised (..),
                     labelPages)
import           Database.LSMTree.Internal.Run.Index.Compact
import           Database.LSMTree.Internal.Serialise (Serialise (..))
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
          prop_searchMinMaxKeysAfterConstruction @(WithSerialised UTxOKey)
      , testProperty "prop_differentChunkSizesSameResults" $
          prop_differentChunkSizesSameResults @(WithSerialised UTxOKey)
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
     Serialise k
  => ChunkSize
  -> Pages k
  -> Property
prop_searchMinMaxKeysAfterConstruction
  (ChunkSize csize)
  pps@(Pages (RFPrecision rfprec) ks) =
      classify (hasClashes ci) "Compact index contains clashes"
    $ labelPages pps
    $ counterexample (show ci)
    $ counterexample (show idxs)
    $ property $ all p idxs
  where
    ci = fromList rfprec csize (fmap (bimap serialise serialise) ks)

    f idx (minKey, maxKey) =
        ( idx
        , search (serialise minKey) ci
        , search (serialise maxKey) ci
        )

    idxs = zipWith f [0..] ks

    p (idx, x, y) =
         Just idx == x && Just idx == y

prop_differentChunkSizesSameResults ::
     Serialise k
  => ChunkSize
  -> ChunkSize
  -> Pages k
  -> Property
prop_differentChunkSizesSameResults
  (ChunkSize csize1)
  (ChunkSize csize2)
  pps@(Pages (RFPrecision rfprec) ks) =
      labelPages pps
    $ ci1 === ci2
  where
    ksSerialised = fmap (bimap serialise serialise) ks
    ci1          = fromList rfprec csize1 ksSerialised
    ci2          = fromList rfprec csize2 ksSerialised

deriving instance Eq CompactIndex
deriving instance Show CompactIndex
