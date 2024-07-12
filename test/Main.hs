-- | Tests for the @lsm-tree@ library.
--
module Main (main) where

import qualified Test.Control.Concurrent.Class.MonadSTM.RWVar
import qualified Test.Data.Arena
import qualified Test.Database.LSMTree.Class.Monoidal
import qualified Test.Database.LSMTree.Class.Normal
import qualified Test.Database.LSMTree.Generators
import qualified Test.Database.LSMTree.Internal
import qualified Test.Database.LSMTree.Internal.BloomFilter
import qualified Test.Database.LSMTree.Internal.Entry
import qualified Test.Database.LSMTree.Internal.IndexCompact
import qualified Test.Database.LSMTree.Internal.Lookup
import qualified Test.Database.LSMTree.Internal.Merge
import qualified Test.Database.LSMTree.Internal.Monkey
import qualified Test.Database.LSMTree.Internal.PageAcc
import qualified Test.Database.LSMTree.Internal.PageAcc1
import qualified Test.Database.LSMTree.Internal.RawOverflowPage
import qualified Test.Database.LSMTree.Internal.RawPage
import qualified Test.Database.LSMTree.Internal.Run
import qualified Test.Database.LSMTree.Internal.RunAcc
import qualified Test.Database.LSMTree.Internal.RunBuilder
import qualified Test.Database.LSMTree.Internal.RunReaders
import qualified Test.Database.LSMTree.Internal.Serialise
import qualified Test.Database.LSMTree.Internal.Serialise.Class
import qualified Test.Database.LSMTree.Internal.Vector
import qualified Test.Database.LSMTree.Model.Monoidal
import qualified Test.Database.LSMTree.Model.Normal
import qualified Test.Database.LSMTree.Normal.StateMachine
import qualified Test.System.Posix.Fcntl.NoCache
import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "lsm-tree"
    [ Test.Control.Concurrent.Class.MonadSTM.RWVar.tests
    , Test.Database.LSMTree.Class.Normal.tests
    , Test.Database.LSMTree.Class.Monoidal.tests
    , Test.Database.LSMTree.Generators.tests
    , Test.Database.LSMTree.Internal.tests
    , Test.Database.LSMTree.Internal.BloomFilter.tests
    , Test.Database.LSMTree.Internal.Entry.tests
    , Test.Database.LSMTree.Internal.Lookup.tests
    , Test.Database.LSMTree.Internal.Merge.tests
    , Test.Database.LSMTree.Internal.Monkey.tests
    , Test.Database.LSMTree.Internal.PageAcc.tests
    , Test.Database.LSMTree.Internal.PageAcc1.tests
    , Test.Database.LSMTree.Internal.RawPage.tests
    , Test.Database.LSMTree.Internal.RawOverflowPage.tests
    , Test.Database.LSMTree.Internal.Run.tests
    , Test.Database.LSMTree.Internal.RunAcc.tests
    , Test.Database.LSMTree.Internal.RunBuilder.tests
    , Test.Database.LSMTree.Internal.RunReaders.tests
    , Test.Database.LSMTree.Internal.IndexCompact.tests
    , Test.Database.LSMTree.Internal.Serialise.tests
    , Test.Database.LSMTree.Internal.Serialise.Class.tests
    , Test.Database.LSMTree.Internal.Vector.tests
    , Test.Database.LSMTree.Model.Normal.tests
    , Test.Database.LSMTree.Model.Monoidal.tests
    , Test.Database.LSMTree.Normal.StateMachine.tests
    , Test.System.Posix.Fcntl.NoCache.tests
    , Test.Data.Arena.tests
    ]
