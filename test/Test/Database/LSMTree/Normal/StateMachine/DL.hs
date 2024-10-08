{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE PolyKinds       #-}

{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Normal.StateMachine.DL (
    tests
  ) where

import           Control.Tracer
import           Database.LSMTree.Normal as R
import           Prelude
import           Test.Database.LSMTree.Normal.StateMachine hiding (tests)
import           Test.Database.LSMTree.Normal.StateMachine.Op
import           Test.QuickCheck
import           Test.QuickCheck.DynamicLogic
import           Test.QuickCheck.StateModel.Lockstep
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.PrettyProxy

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Normal.StateMachine.DL" [
      testProperty "prop_example" prop_example
    ]

instance DynLogicModel (Lockstep (ModelState R.TableHandle))

prop_example :: Property
prop_example = once $ noShrinking $ forAllDL example $ prop_lockstepIO_RealImpl_MockFS tr
  where tr = show `contramap` stdoutTracer

example :: DL (Lockstep (ModelState R.TableHandle)) ()
example = do
    var3 <- action $ New (PrettyProxy @((Key1, Value1, Blob1))) (TableConfig {
          confMergePolicy = MergePolicyLazyLevelling
        , confSizeRatio = Four
        , confWriteBufferAlloc = AllocNumEntries (NumEntries 1)
        , confBloomFilterAlloc = AllocFixed 10
        , confFencePointerIndex = CompactIndex
        , confDiskCachePolicy = DiskCacheNone
        , confMergeSchedule = Incremental })
    action $ Updates [
          (Key1 {_unKey1 = Small {getSmall = 0}},Delete)
        , (Key1 {_unKey1 = Small {getSmall = 0}},Delete)
        , (Key1 {_unKey1 = Small {getSmall = 0}},Delete)
        , (Key1 {_unKey1 = Small {getSmall = 0}},Delete)
        , (Key1 {_unKey1 = Small {getSmall = 0}},Delete)
        , (Key1 {_unKey1 = Small {getSmall = 0}},Delete)
        , (Key1 {_unKey1 = Small {getSmall = 0}},Delete) ]
        (unsafeMkGVar var3 (OpFromRight `OpComp` OpId))
    pure ()
