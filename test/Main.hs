-- | Tests for the @lsm-tree@ library.
--
module Main (main) where

import qualified Test.Database.LSMTree.BloomFilter
import qualified Test.Database.LSMTree.Common
import qualified Test.Database.LSMTree.Entry
import qualified Test.Database.LSMTree.Generators
import qualified Test.Database.LSMTree.Lookup
import qualified Test.Database.LSMTree.Model.Monoidal
import qualified Test.Database.LSMTree.Model.Normal
import qualified Test.Database.LSMTree.ModelIO.Monoidal
import qualified Test.Database.LSMTree.ModelIO.Normal
import qualified Test.Database.LSMTree.Normal.StateMachine
import qualified Test.Database.LSMTree.PageAcc
import qualified Test.Database.LSMTree.PageAcc1
import qualified Test.Database.LSMTree.RawOverflowPage
import qualified Test.Database.LSMTree.RawPage
import qualified Test.Database.LSMTree.Run
import qualified Test.Database.LSMTree.Run.BloomFilter
import qualified Test.Database.LSMTree.Run.Construction
import qualified Test.Database.LSMTree.Run.Index.Compact
import qualified Test.Database.LSMTree.Serialise
import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "lsm-tree"
    [ Test.Database.LSMTree.Common.tests
    , Test.Database.LSMTree.Generators.tests
    , Test.Database.LSMTree.BloomFilter.tests
    , Test.Database.LSMTree.Entry.tests
    , Test.Database.LSMTree.Lookup.tests
    , Test.Database.LSMTree.PageAcc.tests
    , Test.Database.LSMTree.PageAcc1.tests
    , Test.Database.LSMTree.RawPage.tests
    , Test.Database.LSMTree.RawOverflowPage.tests
    , Test.Database.LSMTree.Run.tests
    , Test.Database.LSMTree.Run.BloomFilter.tests
    , Test.Database.LSMTree.Run.Construction.tests
    , Test.Database.LSMTree.Run.Index.Compact.tests
    , Test.Database.LSMTree.Serialise.tests
    , Test.Database.LSMTree.Model.Normal.tests
    , Test.Database.LSMTree.Model.Monoidal.tests
    , Test.Database.LSMTree.ModelIO.Normal.tests
    , Test.Database.LSMTree.ModelIO.Monoidal.tests
    , Test.Database.LSMTree.Normal.StateMachine.tests
    ]
