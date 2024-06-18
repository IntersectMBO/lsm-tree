module Test.Database.LSMTree.Internal.WriteBuffer (tests) where

import qualified Data.Vector as V
import           Database.LSMTree.Extras.Generators (ExampleResolveMupsert,
                     resolveMupsert)
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.WriteBuffer
import qualified Test.Database.LSMTree.Generators as Gen
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.WriteBuffer" [
      testGroup "generator" $
        Gen.prop_arbitraryAndShrinkPreserveInvariant writeBufferInvariant
    , testProperty "prop_addEntriesPreservesInvariant" $
        prop_addEntriesPreservesInvariant
    , testProperty "prop_addEntriesComposes" $
        prop_addEntriesComposes
    ]

type SerialisedKOp = (SerialisedKey, Entry SerialisedValue SerialisedBlob)

prop_addEntriesPreservesInvariant ::
     ExampleResolveMupsert -> WriteBuffer -> V.Vector SerialisedKOp -> Property
prop_addEntriesPreservesInvariant (resolveMupsert -> resolve) wb kops =
    property $ writeBufferInvariant (addEntries resolve kops wb)

prop_addEntriesComposes ::
     ExampleResolveMupsert -> WriteBuffer -> V.Vector SerialisedKOp -> V.Vector SerialisedKOp -> Property
prop_addEntriesComposes (resolveMupsert -> resolve) wb kops1 kops2 =
        addEntries resolve (kops1 V.++ kops2) wb
    === (addEntries resolve kops2 . addEntries resolve kops1) wb
