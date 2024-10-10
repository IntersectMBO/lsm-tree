{-# LANGUAGE DataKinds       #-}
{-# LANGUAGE OverloadedLists #-}
{-# LANGUAGE PolyKinds       #-}

{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Normal.StateMachine.DL (
    tests
  ) where

import           Control.Tracer
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
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
