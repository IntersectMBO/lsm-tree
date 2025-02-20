{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.StateMachine.DL (
    tests
  ) where

import           Control.RefCount (checkForgottenRefs)
import           Control.Tracer
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Database.LSMTree as R
import qualified Database.LSMTree.Model.Session as Model (fromSomeTable, tables)
import qualified Database.LSMTree.Model.Table as Model (values)
import           Prelude
import           Test.Database.LSMTree.StateMachine hiding (tests)
import           Test.Database.LSMTree.StateMachine.Op
import           Test.QuickCheck as QC
import           Test.QuickCheck.DynamicLogic
import qualified Test.QuickCheck.Gen as QC
import qualified Test.QuickCheck.Random as QC
import           Test.QuickCheck.StateModel.Lockstep
import           Test.Tasty (TestTree, testGroup, withResource)
import qualified Test.Tasty.QuickCheck as QC
import           Test.Util.PrettyProxy

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.StateMachine.DL" [
      withResource checkForgottenRefs (\_ -> checkForgottenRefs) $ \_ ->
        QC.testProperty "prop_example" prop_example
    ]

instance DynLogicModel (Lockstep (ModelState R.Table))

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
    propLockstep_RealImpl_MockFS_IO tr CheckCleanup CheckFS CheckRefs
  where
    -- To enable tracing, use something like @show `contramap` stdoutTracer@
    -- instead
    tr = nullTracer

-- | Create an initial "large" table
dl_example :: DL (Lockstep (ModelState R.Table)) ()
dl_example = do
    -- Create an initial table and fill it with some inserts
    var3 <- action $ Action Nothing $ New (PrettyProxy @((Key, Value, Blob))) (TableConfig {
          confMergePolicy = MergePolicyLazyLevelling
        , confSizeRatio = Four
        , confWriteBufferAlloc = AllocNumEntries (NumEntries 4)
        , confBloomFilterAlloc = AllocFixed 10
        , confFencePointerIndex = CompactIndex
        , confDiskCachePolicy = DiskCacheNone
        , confMergeSchedule = OneShot })
    let kvs :: Map.Map Key Value
        kvs = Map.fromList $
              QC.unGen (QC.vectorOf 37 $ (,) <$> QC.arbitrary <*> QC.arbitrary)
                       (QC.mkQCGen 42) 30
        ups :: V.Vector (Key, Update Value Blob)
        ups = V.fromList
            . map (\(k,v) -> (k, Insert v Nothing))
            . Map.toList $ kvs
    action $ Action Nothing $ Updates ups (unsafeMkGVar var3 (OpFromRight `OpComp` OpId))
    -- This is a rather ugly assertion, and could be improved using some helper
    -- function(s). However, it does serve its purpose as checking that the
    -- insertions we just did were successful.
    assertModel "table size" $ \s ->
        let (ModelState s' _) = getModel s in
        case Map.elems (Model.tables s') of
          [(_, smTbl)]
            | Just tbl <- (Model.fromSomeTable @Key @Value @Blob smTbl)
            -> Map.size (Model.values tbl) == Map.size kvs
          _ -> False
