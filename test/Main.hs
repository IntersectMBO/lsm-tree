-- | Tests for the @lsm-tree@ library.
--
module Main (main) where

import qualified Control.RefCount

import qualified Test.Data.Arena
import qualified Test.Database.LSMTree.Class
import qualified Test.Database.LSMTree.Generators
import qualified Test.Database.LSMTree.Internal
import qualified Test.Database.LSMTree.Internal.BlobFile.FS
import qualified Test.Database.LSMTree.Internal.BloomFilter
import qualified Test.Database.LSMTree.Internal.Chunk
import qualified Test.Database.LSMTree.Internal.CRC32C
import qualified Test.Database.LSMTree.Internal.Entry
import qualified Test.Database.LSMTree.Internal.FS.File
import qualified Test.Database.LSMTree.Internal.Index.Compact
import qualified Test.Database.LSMTree.Internal.Index.Ordinary
import qualified Test.Database.LSMTree.Internal.Lookup
import qualified Test.Database.LSMTree.Internal.Merge
import qualified Test.Database.LSMTree.Internal.MergingRun
import qualified Test.Database.LSMTree.Internal.MergingTree
import qualified Test.Database.LSMTree.Internal.Monkey
import qualified Test.Database.LSMTree.Internal.PageAcc
import qualified Test.Database.LSMTree.Internal.PageAcc1
import qualified Test.Database.LSMTree.Internal.RawOverflowPage
import qualified Test.Database.LSMTree.Internal.RawPage
import qualified Test.Database.LSMTree.Internal.Run
import qualified Test.Database.LSMTree.Internal.RunAcc
import qualified Test.Database.LSMTree.Internal.RunBuilder
import qualified Test.Database.LSMTree.Internal.RunReader
import qualified Test.Database.LSMTree.Internal.RunReaders
import qualified Test.Database.LSMTree.Internal.Serialise
import qualified Test.Database.LSMTree.Internal.Serialise.Class
import qualified Test.Database.LSMTree.Internal.Snapshot.Codec
import qualified Test.Database.LSMTree.Internal.Snapshot.Codec.Golden
import qualified Test.Database.LSMTree.Internal.Snapshot.FS
import qualified Test.Database.LSMTree.Internal.Vector
import qualified Test.Database.LSMTree.Internal.Vector.Growing
import qualified Test.Database.LSMTree.Internal.WriteBufferBlobs.FS
import qualified Test.Database.LSMTree.Internal.WriteBufferReader.FS
import qualified Test.Database.LSMTree.Model.Table
import qualified Test.Database.LSMTree.Monoidal
import qualified Test.Database.LSMTree.StateMachine
import qualified Test.Database.LSMTree.StateMachine.DL
import qualified Test.Database.LSMTree.UnitTests
import qualified Test.FS
import qualified Test.System.Posix.Fcntl.NoCache
import           Test.Tasty

main :: IO ()
main = do
  defaultMain $ testGroup "lsm-tree"
    [ Test.Data.Arena.tests
    , Test.Database.LSMTree.Class.tests
    , Test.Database.LSMTree.Generators.tests
    , Test.Database.LSMTree.Internal.tests
    , Test.Database.LSMTree.Internal.BlobFile.FS.tests
    , Test.Database.LSMTree.Internal.BloomFilter.tests
    , Test.Database.LSMTree.Internal.Chunk.tests
    , Test.Database.LSMTree.Internal.CRC32C.tests
    , Test.Database.LSMTree.Internal.Entry.tests
    , Test.Database.LSMTree.Internal.FS.File.tests
    , Test.Database.LSMTree.Internal.Index.Compact.tests
    , Test.Database.LSMTree.Internal.Index.Ordinary.tests
    , Test.Database.LSMTree.Internal.Lookup.tests
    , Test.Database.LSMTree.Internal.Merge.tests
    , Test.Database.LSMTree.Internal.MergingRun.tests
    , Test.Database.LSMTree.Internal.MergingTree.tests
    , Test.Database.LSMTree.Internal.Monkey.tests
    , Test.Database.LSMTree.Internal.PageAcc.tests
    , Test.Database.LSMTree.Internal.PageAcc1.tests
    , Test.Database.LSMTree.Internal.RawPage.tests
    , Test.Database.LSMTree.Internal.RawOverflowPage.tests
    , Test.Database.LSMTree.Internal.Run.tests
    , Test.Database.LSMTree.Internal.RunAcc.tests
    , Test.Database.LSMTree.Internal.RunBuilder.tests
    , Test.Database.LSMTree.Internal.RunReader.tests
    , Test.Database.LSMTree.Internal.RunReaders.tests
    , Test.Database.LSMTree.Internal.Serialise.tests
    , Test.Database.LSMTree.Internal.Serialise.Class.tests
    , Test.Database.LSMTree.Internal.Snapshot.Codec.tests
    , Test.Database.LSMTree.Internal.Snapshot.Codec.Golden.tests
    , Test.Database.LSMTree.Internal.Snapshot.FS.tests
    , Test.Database.LSMTree.Internal.Vector.tests
    , Test.Database.LSMTree.Internal.Vector.Growing.tests
    , Test.Database.LSMTree.Internal.WriteBufferBlobs.FS.tests
    , Test.Database.LSMTree.Internal.WriteBufferReader.FS.tests
    , Test.Database.LSMTree.Model.Table.tests
    , Test.Database.LSMTree.Monoidal.tests
    , Test.Database.LSMTree.UnitTests.tests
    , Test.Database.LSMTree.StateMachine.tests
    , Test.Database.LSMTree.StateMachine.DL.tests
    , Test.FS.tests
    , Test.System.Posix.Fcntl.NoCache.tests
    ]
  Control.RefCount.checkForgottenRefs
  -- This use of checkForgottenRefs is a last resort. Refs that are forgotten
  -- before being released are detected by the first Ref operation after a
  -- major GC. So they may be thrown during the run of individual tests (though
  -- depending on GC timing this may be during a subsequent test to the one
  -- that triggered the bug). As a last resort, checkForgottenRefs does a last
  -- major GC and will trigger any forgotten refs. So this will reliably catch
  -- the errors, but will not identify where they come from, not even which
  -- test!
  --
  -- If this exception occurs, it may be necessary to put proper use of
  -- checkForgottenRefs into the tests suspected of being the culprit to
  -- identiyfy which one is really failing.
