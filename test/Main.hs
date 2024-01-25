-- | Tests for the @lsm-tree@ library.
--
module Main (main) where

import qualified Test.Database.LSMTree.Common
import qualified Test.Database.LSMTree.Generators
import qualified Test.Database.LSMTree.Internal.RawPage
import qualified Test.Database.LSMTree.Internal.Run
import qualified Test.Database.LSMTree.Internal.Run.BloomFilter
import qualified Test.Database.LSMTree.Internal.Run.Construction
import qualified Test.Database.LSMTree.Internal.Run.Index.Compact
import qualified Test.Database.LSMTree.Internal.Serialise
import qualified Test.Database.LSMTree.Model.Monoidal
import qualified Test.Database.LSMTree.Model.Normal
import qualified Test.Database.LSMTree.ModelIO.Monoidal
import qualified Test.Database.LSMTree.ModelIO.Normal
import qualified Test.Database.LSMTree.Normal.StateMachine
import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "lsm-tree"
    [ Test.Database.LSMTree.Common.tests
    , Test.Database.LSMTree.Generators.tests
    , Test.Database.LSMTree.Internal.Run.tests
    , Test.Database.LSMTree.Internal.Run.Construction.tests
    , Test.Database.LSMTree.Internal.Run.Index.Compact.tests
    , Test.Database.LSMTree.Internal.Serialise.tests
    , Test.Database.LSMTree.Internal.RawPage.tests
    , Test.Database.LSMTree.Model.Normal.tests
    , Test.Database.LSMTree.Model.Monoidal.tests
    , Test.Database.LSMTree.ModelIO.Normal.tests
    , Test.Database.LSMTree.ModelIO.Monoidal.tests
    , Test.Database.LSMTree.Normal.StateMachine.tests
    , Test.Database.LSMTree.Internal.Run.BloomFilter.tests
    ]
