{-# LANGUAGE DataKinds         #-}
{-# LANGUAGE OverloadedLists   #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE PolyKinds         #-}

{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Normal.StateMachine.DL (
    tests
  ) where

import           Control.Tracer
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import qualified Data.Vector as V
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact (..))
import qualified Database.LSMTree.Model.Normal as Model
import qualified Database.LSMTree.Model.Normal.Session as Model
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
    , testProperty "prop_thunk" prop_thunk
    , testProperty "prop_snap" prop_snap
    , testProperty "prop_snap2" prop_snap2
    ]

instance DynLogicModel (Lockstep (ModelState R.TableHandle))

-- | An example of how dynamic logic formulas can be run.
--
-- 'dl_example' is a manually created formula, but the same method of running a
-- formula also applies to counterexamples produced by a state machine property
-- test, such as 'propLockstep_RealImpl_RealFS_IO'. Such counterexamples are
-- (often almost) valid 'DL' expression. They can be copy-pasted into a Haskell
-- module with minor tweaks to make the compiler accept the copied code, and
-- then they can be run as any other 'DL' expression.
prop_example :: Property
prop_example =
    -- Run the example ...
    forAllDL dl_example $
    -- ... with the given lockstep property
    propLockstep_RealImpl_MockFS_IO tr
  where
    -- To enable tracing, use something like @show `contramap` stdoutTracer@
    -- instead
    tr = nullTracer

-- | Create an initial "large" table, and then proceed with random actions as
-- usual.
dl_example :: DL (Lockstep (ModelState R.TableHandle)) ()
dl_example = do
    -- Create an initial table and fill it with some inserts
    var3 <- action $ New (PrettyProxy @((Key1, Value1, Blob1))) (TableConfig {
          confMergePolicy = MergePolicyLazyLevelling
        , confSizeRatio = Four
        , confWriteBufferAlloc = AllocNumEntries (NumEntries 30)
        , confBloomFilterAlloc = AllocFixed 10
        , confFencePointerIndex = CompactIndex
        , confDiskCachePolicy = DiskCacheNone
        , confMergeSchedule = OneShot })
    let ins = [ (Key1 y, Value1 y, Nothing)
              | x <- [1 .. 678]
              , let y = Small x ]
    action $ Inserts (V.fromList ins) (unsafeMkGVar var3 (OpFromRight `OpComp` OpId))
    -- This is a rather ugly assertion, and could be improved using some helper
    -- function(s). However, it does serve its purpose as checking that the
    -- insertions we just did were successful.
    assertModel "table has size 678" $ \s ->
        let (ModelState s' _) = getModel s in
        case Map.elems (Model.tableHandles s') of
          [(_, smTbl)]
            | Just tbl <- (Model.fromSomeTable @Key1 @Value1 @Blob1 smTbl)
            -> Map.size (Model.values tbl) == 678
          _ -> False
    -- Perform any sequence of actions after
    anyActions_


prop_thunk :: Property
prop_thunk = forAllDL dl_thunk $ propLockstep_RealImpl_MockFS_IO nullTracer

dl_thunk :: DL (Lockstep (ModelState R.TableHandle)) ()
dl_thunk = do
    var23 <- action $ New (PrettyProxy @((Key2,Value2,Blob2))) (TableConfig {
          confMergePolicy = MergePolicyLazyLevelling
        , confSizeRatio = Four
        , confWriteBufferAlloc = AllocNumEntries (NumEntries 2)
        , confBloomFilterAlloc = AllocFixed 10
        , confFencePointerIndex = CompactIndex
        , confDiskCachePolicy = DiskCacheNone
        , confMergeSchedule = Incremental})
    action $ Updates [
        (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)]
      (unsafeMkGVar var23 (OpFromRight `OpComp` OpId))
    pure ()

prop_snap :: Property
prop_snap = forAllDL dl_snap $ propLockstep_RealImpl_MockFS_IO nullTracer

dl_snap :: DL (Lockstep (ModelState R.TableHandle)) ()
dl_snap = do
    var3 <- action $ New (PrettyProxy @((Key2,Value2,Blob2))) (TableConfig {
        confMergePolicy = MergePolicyLazyLevelling
      , confSizeRatio = Four
      , confWriteBufferAlloc = AllocNumEntries (NumEntries 2)
      , confBloomFilterAlloc = AllocFixed 10
      , confFencePointerIndex = CompactIndex
      , confDiskCachePolicy = DiskCacheNone
      , confMergeSchedule = Incremental})
    action $ Updates [
        (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,3]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,4]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,5]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,6]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,6]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,7]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,8]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,0]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,9]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,9]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,10]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,11]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,12]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,14]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,15]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,15]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,16]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,15]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,17]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,18]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,15]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,18]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,19]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,15]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,20]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,1]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,15]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,20]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,13]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,20]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,21]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,22]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,23]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,22]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,23]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,22]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,23]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,2]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,22]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,23]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,24]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,3]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,23]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,23]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,24]}},Insert (Value2 {_unValue2 = ""}) Nothing)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,3]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,3]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)]
      (unsafeMkGVar var3 (OpFromRight `OpComp` OpId))
    action $ Snapshot (fromJust $ mkSnapshotName "snap3") (unsafeMkGVar var3 (OpFromRight `OpComp` OpId))
    action $ Updates [
        (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)
      , (Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete)]
      (unsafeMkGVar var3 (OpFromRight `OpComp` OpId))
    pure ()

prop_snap2 :: Property
prop_snap2 = forAllDL dl_snap2 $ propLockstep_RealImpl_MockFS_IO nullTracer

dl_snap2 :: DL (Lockstep (ModelState R.TableHandle)) ()
dl_snap2 = do
    var5 <- action $ New (PrettyProxy @((Key2,Value2,Blob2))) (TableConfig {confMergePolicy = MergePolicyLazyLevelling, confSizeRatio = Four, confWriteBufferAlloc = AllocNumEntries (NumEntries 3), confBloomFilterAlloc = AllocFixed 10, confFencePointerIndex = CompactIndex, confDiskCachePolicy = DiskCacheNone, confMergeSchedule = Incremental})
    action $ Updates [(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,0]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,3]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,4]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,1]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,5]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,6]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,7]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,8]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,9]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,10]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,2]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,11]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,3]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,12]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,13]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,14]}},Insert (Value2 {_unValue2 = ""}) Nothing)] (unsafeMkGVar var5 (OpFromRight `OpComp` OpId))
    action $ Updates [(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,15]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,16]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,17]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,18]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,19]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,20]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,18]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,19]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,21]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,19]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,21]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,22]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,23]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,24]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,2,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,4]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,2,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,23]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,2,0]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,24]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,25]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,26]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,27]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,28]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,26]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,28]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,5]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,28]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,29]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,2,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,30]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,28]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,1,6]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,31]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,32]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,19]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,2,1]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,31]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,33]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,34]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,35]}},Insert (Value2 {_unValue2 = ""}) Nothing),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,2,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,1]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,2]}},Delete),(Key2 {_unKey2 = KeyForIndexCompact {getKeyForIndexCompact = [0,0,0,0,0,0,0,0]}},Delete)] (unsafeMkGVar var5 (OpFromRight `OpComp` OpId))
    action $ Snapshot (fromJust $ mkSnapshotName"snap1") (unsafeMkGVar var5 (OpFromRight `OpComp` OpId))
    pure ()
