{-# OPTIONS_GHC -Wno-orphans #-}

module Test.ScheduledMergesDL (tests) where

import           ScheduledMerges as LSM
import           Test.ScheduledMergesQLS as QLS (Action (..), Model, prop_LSM)

import           Test.QuickCheck (NonNegative (..), Property)
import           Test.QuickCheck.DynamicLogic
import           Test.QuickCheck.StateModel.Lockstep hiding (ModelOp)
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

tests :: TestTree
tests = testGroup "Test.ScheduledMergesDL" [
      testProperty "prop_empty_union" prop_empty_union
    ]

instance DynLogicModel (Lockstep Model)

prop_empty_union :: Property
prop_empty_union = forAllDL dl_empty_union prop_LSM

-- | Create a table with an empty run in the union level, then run arbitrary
-- actions to see if all invariants still hold.
dl_empty_union :: DL (Lockstep Model) ()
dl_empty_union = do
    -- Create a union level that will result in an empty run. This is easy to
    -- achieve if the inputs to the union only contain deletes (which will get
    -- removed in a last level merge, which the union merges are).
    tInput <- action $ ANew conf
    _ <- action $ ADelete (unsafeMkGVar tInput opIdentity) (Right (K 0))
    tUnion <- action $ AUnions [unsafeMkGVar tInput opIdentity, unsafeMkGVar tInput opIdentity]

    -- Perform some actions so tUnion gets regular levels as well.
    anyActions_

    -- Now complete the union.
    _ <- action $ ASupplyUnion (unsafeMkGVar tUnion opIdentity) (NonNegative (UnionCredits 100))

    anyActions_
  where
    conf =
      LSMConfig
        { configMaxWriteBufferSize = 2
        , configSizeRatio = 2
        }
