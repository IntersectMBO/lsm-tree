{-# OPTIONS_GHC -Wno-unused-do-bind #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.StateMachine.DL (
    tests
  ) where

import           Control.Monad (void)
import           Control.RefCount
import           Control.Tracer
import qualified Data.Map.Strict as Map
import qualified Data.Vector as V
import           Database.LSMTree as R
import qualified Database.LSMTree.Model.Session as Model (fromSomeTable, tables)
import qualified Database.LSMTree.Model.Table as Model (values)
import           Prelude
import           System.FS.API.Types
import           System.FS.Sim.Error hiding (Blob)
import qualified System.FS.Sim.Stream as Stream
import           System.FS.Sim.Stream (Stream)
import           Test.Database.LSMTree.StateMachine hiding (tests)
import           Test.Database.LSMTree.StateMachine.Op
import           Test.QuickCheck as QC hiding (label)
import           Test.QuickCheck.DynamicLogic
import qualified Test.QuickCheck.Gen as QC
import qualified Test.QuickCheck.Random as QC
import           Test.QuickCheck.StateModel.Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Defaults as QLS
import           Test.QuickCheck.StateModel.Variables
import           Test.Tasty (TestTree, localOption, testGroup, withResource)
import qualified Test.Tasty.QuickCheck as QC
import           Test.Util.PrettyProxy

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.StateMachine.DL" [
      withResource checkForgottenRefs (\_ -> checkForgottenRefs) $ \_ ->
        QC.testProperty "prop_example" prop_example
    , withResource checkForgottenRefs (const ignoreForgottenRefs) $ \_ ->
        -- TODO: remove local options. We currently run with a known failing
        -- seed, because shrinking seems to loop somewhere. Bug fixes will
        -- follow later.
        localOption (QC.QuickCheckReplay (read "SMGen 601225290552401160 8241196702162822795",50)) $
        localOption (QC.QuickCheckTests 1000) $
          QC.testProperty "prop_noSwallowedExceptions" $
            QC.expectFailure $ -- TODO: remove once the property succeeds
            prop_noSwallowedExceptions
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
    action $ Action Nothing $ Updates (Left $ unsafeMkGVar var3 (OpFromRight `OpComp` OpId)) ups
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

{-------------------------------------------------------------------------------
  Swallowed exceptions
-------------------------------------------------------------------------------}

{-
--quickcheck-replay="(SMGen 601225290552401160 8241196702162822795,50)"

--quickcheck-replay="(SMGen 1262390567463183010 1470447921673677017,56)"

--quickcheck-replay="(SMGen 1761811726072439171 16251490036421965979,69)"
-}



-- | Test that the @lsm-tree@ library does not swallow exceptions.
--
-- A functional requirement for the @lsm-tree@ library is that all exceptions
-- are properly communicated to the user. An alternative way of stating this
-- requirement is that no exceptions should be /swallowed/ by the library. We
-- test this requirement by running the state machine test with injected disk
-- errors using @fs-sim@, and asserting that no exceptions are swallowed.
--
-- The state machine test compares the SUT against a model by checking that
-- their responses to @lsm-tree@ actions are the same. As of writing this
-- property, not all of these actions on the SUT are guaranteed to be fully
-- exception safe. As a result, an exception might leave the database (i.e.,
-- session, tables, cursors) in an inconsistent state. The results of subsequent
-- operations on the inconsistent database should be considered undefined. As
-- such, it is highly likely that the SUT and model will thereafter disagree,
-- leading to a failing property.
--
-- Still, we want to run the swallowed error assertion on /all/ operations,
-- regardless of whether they are exception safe. We overcome this problem by
-- /definitely/ injecting errors (and running a swallowed error assertion) for
-- the last action in a sequence of actions. This may leave the final database
-- state inconsistent, but that is okay. However, we'll also have to disable
-- sanity checks like 'NoCheckCleanup', 'NoCheckFS', and 'NoCheckRefs', because
-- they are likely to fail if the database is an inconsistent state.
--
-- TODO: running only one swallowed exception assertion per action sequence is
-- restrictive, but this automatically improves as we make more actions
-- exceptions safe. When we generate injected errors for these errors by default
-- (in @arbitraryWithVars@), the swallowed exception assertion automatically
-- runs for those actions as well.
prop_noSwallowedExceptions :: Property
prop_noSwallowedExceptions = forAllDL dl_noSwallowExceptions runner
  where
    -- disable all file system and reference checks
    runner = propLockstep_RealImpl_MockFS_IO tr NoCheckCleanup NoCheckFS NoCheckRefs
    tr = nullTracer

-- | Run any number of actions using the default actions generator, and finally
-- run a single action with errors *definitely* enabled.
dl_noSwallowExceptions :: DL (Lockstep (ModelState R.Table)) ()
dl_noSwallowExceptions = do
    -- Run any number of actions as normal
    anyActions_

    -- Generate a single action as normal
    varCtx <- getVarContextDL
    st <- getModelStateDL
    let
      gen = QLS.arbitraryAction varCtx st
      predicate (Some a) = QLS.precondition st a
      shr (Some a) = QLS.shrinkAction varCtx st a
    Some a <- forAllQ $ withGenQ gen predicate shr

    -- Overwrite the maybe errors of the generated action with *definitely* just
    -- errors.
    case a of
      Action _merrs a' -> do
        HasNoVariables errs <-
          forAllQ $ hasNoVariablesQ $ withGenQ arbitraryErrors (\_ -> True) shrink
        -- Run the modified action
        void $ action $ Action (Just errs) a'

-- | Generate an 'Errors' with arbitrary probabilities of exceptions.
--
-- The default 'genErrors' from @fs-sim@ generates streams of 'Maybe' exceptions
-- with a fixed probability for a 'Just' or 'Nothing'. The version here
-- generates an arbitrary probability for each stream, which should generate a
-- larger variety of 'Errors' structures.
arbitraryErrors :: Gen Errors
arbitraryErrors = do
    dumpStateE                <- genStream arbitrary
    hCloseE                   <- genStream arbitrary
    hTruncateE                <- genStream arbitrary
    doesDirectoryExistE       <- genStream arbitrary
    doesFileExistE            <- genStream arbitrary
    hOpenE                    <- genStream arbitrary
    hSeekE                    <- genStream arbitrary
    hGetSomeE                 <- genErrorStreamGetSome
    hGetSomeAtE               <- genErrorStreamGetSome
    hPutSomeE                 <- genErrorStreamPutSome
    hGetSizeE                 <- genStream arbitrary
    createDirectoryE          <- genStream arbitrary
    createDirectoryIfMissingE <- genStream arbitrary
    listDirectoryE            <- genStream arbitrary
    removeDirectoryRecursiveE <- genStream arbitrary
    removeFileE               <- genStream arbitrary
    renameFileE               <- genStream arbitrary
    hGetBufSomeE              <- genErrorStreamGetSome
    hGetBufSomeAtE            <- genErrorStreamGetSome
    hPutBufSomeE              <- genErrorStreamPutSome
    hPutBufSomeAtE            <- genErrorStreamPutSome
    return Errors {..}
  where
    -- Generate a stream using 'genLikelihoods' for its 'Maybe' elements.
    genStream :: forall a. Gen a -> Gen (Stream a)
    genStream genA = do
        (pNothing, pJust) <- genLikelihoods
        Stream.genInfinite $ Stream.genMaybe pNothing pJust genA

    -- Generate two integer likelihoods for 'Nothing' and 'Just' constructors.
    genLikelihoods :: Gen (Int, Int)
    genLikelihoods = do
      NonNegative pNothing <- arbitrary
      NonNegative pJust <- arbitrary
      if pNothing == 0 then
        pure (0, 1)
      else if pJust == 0 then
        pure (1, 0)
      else
        pure (pNothing, pJust)

    -- TODO: tweak distribution of stream elements
    genErrorStreamGetSome :: Gen ErrorStreamGetSome
    genErrorStreamGetSome = genStream $ liftArbitrary2 arbitrary arbitrary

    -- TODO: tweak distribution of stream elements
    genErrorStreamPutSome :: Gen ErrorStreamPutSome
    genErrorStreamPutSome = genStream $ flip liftArbitrary2 arbitrary $ do
        errorType <- arbitrary
        maybePutCorruption <- liftArbitrary genPutCorruption
        pure (errorType, maybePutCorruption)

    -- TODO: tweak distribution
    genPutCorruption :: Gen PutCorruption
    genPutCorruption = oneof [
          PartialWrite <$> arbitrary
        , SubstituteWithJunk <$> arbitrary
        ]
      where
        _coveredAllCases x = case x of
          PartialWrite{}       -> pure ()
          SubstituteWithJunk{} -> pure ()

deriving stock instance Enum FsErrorType
deriving stock instance Bounded FsErrorType

instance Arbitrary FsErrorType where
  arbitrary = arbitraryBoundedEnum
  shrink = shrinkBoundedEnum
