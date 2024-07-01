{-# LANGUAGE CPP                      #-}
{-# LANGUAGE ConstraintKinds          #-}
{-# LANGUAGE DerivingStrategies       #-}
{-# LANGUAGE EmptyDataDeriving        #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE InstanceSigs             #-}
{-# LANGUAGE LambdaCase               #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE NamedFieldPuns           #-}
{-# LANGUAGE RankNTypes               #-}
{-# LANGUAGE ScopedTypeVariables      #-}
{-# LANGUAGE StandaloneDeriving       #-}
{-# LANGUAGE StandaloneKindSignatures #-}
{-# LANGUAGE TypeApplications         #-}
{-# LANGUAGE TypeFamilies             #-}
{-# LANGUAGE UndecidableInstances     #-}

#if MIN_VERSION_GLASGOW_HASKELL(9,8,1,0)
{-# LANGUAGE TypeAbstractions         #-}
#endif

{-# OPTIONS_GHC -Wno-orphans #-}

{- HLINT ignore "Use camelCase" -}

{-
  TODO: improve generation and shrinking of dependencies. See
  https://github.com/IntersectMBO/lsm-tree/pull/4#discussion_r1334295154.

  TODO: The 'RetrieveBlobs' action will currently only retrieve blob references
  that come from a single batch lookup or range lookup. It would be interesting
  to also retrieve blob references from a mix of different batch lookups and/or
  range lookups. This would require some non-trivial changes, such as changes to
  'Op' to also include expressions for manipulating lists, such that we can map
  @'Var' ['R.BlobRef' blob]@ to @'Var' ('R.BlobRef' blob)@. 'RetrieveBlobs'
  would then hold a list of variables (e.g., @['Var' ('R.BlobRef blob')]@)
  instead of a variable of a list (@'Var' ['R.BlobRef' blob]@).

  TODO: it is currently not correctly modelled what happens if blob references
  are retrieved from an incorrect table handle.

  TODO: run lockstep tests with IOSim too
-}
module Test.Database.LSMTree.Normal.StateMachine (tests) where

import           Control.Monad ((<=<))
import           Control.Monad.Class.MonadThrow (Handler (..), MonadCatch (..),
                     MonadThrow (..))
import           Control.Monad.Reader (ReaderT (..))
import           Data.Bifunctor (Bifunctor (..))
import qualified Data.ByteString as BS
import           Data.Constraint (Dict (..))
import           Data.Kind (Type)
import           Data.Maybe (fromJust)
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Typeable (Proxy (..), Typeable, cast)
import qualified Data.Vector as V
import           Data.Word (Word64)
import qualified Database.LSMTree.Class.Normal as Class
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact)
import           Database.LSMTree.Internal (LSMTreeError (..))
import qualified Database.LSMTree.Internal as R.Internal
import qualified Database.LSMTree.Model.Normal.Session as Model
import qualified Database.LSMTree.ModelIO.Normal as M
import qualified Database.LSMTree.Normal as R
import           GHC.IO.Exception (IOErrorType (..), IOException (..))
import           Prelude hiding (init)
import           System.Directory (removeDirectoryRecursive)
import           System.FS.API (HasFS, MountPoint (..), mkFsPath)
import           System.FS.BlockIO.API (HasBlockIO, defaultIOCtxParams)
import           System.FS.BlockIO.IO (ioHasBlockIO)
import           System.FS.IO (HandleIO, ioHasFS)
import           System.IO.Error
import           System.IO.Temp (createTempDirectory,
                     getCanonicalTemporaryDirectory)
import           Test.Database.LSMTree.Normal.StateMachine.Op
                     (HasBlobRef (getBlobRef), Op (..))
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary, Gen)
import           Test.QuickCheck.StateModel hiding (Var)
import           Test.QuickCheck.StateModel.Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Defaults as Lockstep.Defaults
import qualified Test.QuickCheck.StateModel.Lockstep.Run as Lockstep.Run
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.TypeFamilyWrappers (WrapBlob (..), WrapBlobRef (..),
                     WrapSession (..), WrapTableHandle (..))

{-------------------------------------------------------------------------------
  Test tree
-------------------------------------------------------------------------------}

tests :: TestTree
tests = testGroup "Normal.StateMachine" [
      testCase "labelledExamples" $
        QC.labelledExamples $ Lockstep.Run.tagActions (Proxy @(ModelState R.TableHandle))
    , propLockstepIO_ModelIOImpl
    , propLockstepIO_RealImpl_RealFS
{- TODO: temporarily disabled until we start on I/O fault testing.
    , propLockstepIO_RealImpl_MockFS
-}
    ]

propLockstepIO_ModelIOImpl :: TestTree
propLockstepIO_ModelIOImpl = testProperty "propLockstepIO_ModelIOImpl" $
    Lockstep.Run.runActionsBracket
      (Proxy @(ModelState M.TableHandle))
      acquire
      release
      (\r session -> runReaderT r (session, handler))
  where
    acquire :: IO (WrapSession M.TableHandle IO)
    acquire = WrapSession <$> M.openSession

    release :: WrapSession M.TableHandle IO -> IO ()
    release (WrapSession session) = M.closeSession session

    handler :: Handler IO (Maybe Model.Err)
    handler = Handler $ pure . handler'
      where
        handler' :: IOError -> Maybe Model.Err
        handler' err
          | isDoesNotExistError err
          , ioeGetLocation err == "open"
          = Just Model.ErrSnapshotDoesNotExist

          | isIllegalOperation err
          , ioe_description err == "table handle closed"
          = Just Model.ErrTableHandleClosed

          | isAlreadyExistsError err
          , ioe_location err == "snapshot"
          = Just Model.ErrSnapshotExists

          | ioeGetErrorType err == InappropriateType
          , ioe_location err == "open"
          = Just Model.ErrSnapshotWrongType

          | isDoesNotExistError err
          , ioe_location err == "deleteSnapshot"
          = Just Model.ErrSnapshotDoesNotExist

          | isIllegalOperation err
          , ioe_description err == "blob reference invalidated"
          = Just Model.ErrBlobRefInvalidated

          | otherwise
          = Nothing

propLockstepIO_RealImpl_RealFS :: TestTree
propLockstepIO_RealImpl_RealFS = testProperty "propLockstepIO_RealImpl_RealFS" $
    Lockstep.Run.runActionsBracket
      (Proxy @(ModelState R.TableHandle))
      acquire
      release
      (\r (_, session) -> runReaderT r (session, handler))
  where
    acquire :: IO (FilePath, WrapSession R.TableHandle IO)
    acquire = do
        (tmpDir, hasFS, hasBlockIO) <- createSystemTempDirectory "propLockstepIO_RealIO"
        session <- R.openSession hasFS hasBlockIO (mkFsPath [])
        pure (tmpDir, WrapSession session)

    release :: (FilePath, WrapSession R.TableHandle IO) -> IO ()
    release (tmpDir, WrapSession session) = do
        R.closeSession session
        removeDirectoryRecursive tmpDir

    handler :: Handler IO (Maybe Model.Err)
    handler = Handler $ pure . handler'
      where
        handler' :: LSMTreeError -> Maybe Model.Err
        handler' ErrTableClosed = Just Model.ErrTableHandleClosed
        handler' (ErrSnapshotNotExists _snap) = Just Model.ErrSnapshotDoesNotExist
        handler' (ErrSnapshotExists _snap) = Just Model.ErrSnapshotExists
        handler' (ErrSnapshotWrongType _snap) = Just Model.ErrSnapshotWrongType
        handler' _ = Nothing

{- TODO: temporarily disabled until we start on I/O fault testing.
propLockstepIO_RealImpl_MockFS :: TestTree
propLockstepIO_RealImpl_MockFS = testProperty "propLockstepIO_RealImpl_MockFS" $
   QC.expectFailure $ -- TODO: remove once we have a real implementation
    Lockstep.Run.runActionsBracket
      (Proxy @(ModelState R.TableHandle))
      acquire
      release
      (\r session -> runReaderT r (session, handler))
  where
    acquire :: IO (WrapSession R.TableHandle IO)
    acquire = do
        someHasFS <- SomeHasFS <$> simHasFS' MockFS.empty
        WrapSession <$> R.openSession someHasFS (mkFsPath [])

    release :: WrapSession R.TableHandle IO -> IO ()
    release (WrapSession session) = R.closeSession session

    handler :: Handler IO (Maybe Model.Err)
    handler = Handler $ pure . handler'
      where
        handler' :: IOError -> Maybe Model.Err
        handler' _err = Nothing
-}

createSystemTempDirectory ::  [Char] -> IO (FilePath, HasFS IO HandleIO, HasBlockIO IO HandleIO)
createSystemTempDirectory prefix = do
    systemTempDir <- getCanonicalTemporaryDirectory
    tempDir <- createTempDirectory systemTempDir prefix
    let hasFS = ioHasFS (MountPoint tempDir)
    hasBlockIO <- ioHasBlockIO hasFS defaultIOCtxParams
    pure (tempDir, hasFS, hasBlockIO)

instance Arbitrary M.TableConfig where
  arbitrary :: Gen M.TableConfig
  arbitrary = pure M.TableConfig

-- |  This always generates 'Nothing' for the 'R.confResolveMupsert'
-- field.
instance Arbitrary R.TableConfig where
  arbitrary :: Gen R.TableConfig
  arbitrary = pure $ R.TableConfig {
        R.confMergePolicy      = R.MergePolicyLazyLevelling
      , R.confSizeRatio        = R.Four
      , R.confWriteBufferAlloc = R.AllocNumEntries (R.NumEntries 30)
      , R.confBloomFilterAlloc = R.AllocRequestFPR 0.02
      , R.confResolveMupsert   = Nothing
      }

instance Eq R.TableConfig where
  R.TableConfig pol1 size1 wbAlloc1 bfAlloc1 mups1
    == R.TableConfig pol2 size2 wbAlloc2 bfAlloc2 mups2
    = and [
        pol1 == pol2
      , size1 == size2
      , wbAlloc1 == wbAlloc2
      , bfAlloc1 == bfAlloc2
      , mups1 == mups2 -- Errors if either is 'Just'. This is intended behaviour.
      ]

-- | 'R.ResolveMupsert's are arbitrary functions, so they don't have a
-- sensible 'Eq' instance. This (bottom) instance is only included to make
-- @quickheck-dynamic@ happy. Do not use this instance!
instance Eq R.Internal.ResolveMupsert where
  _ == _ = error "(==) on ResolveMupserts should not be used!"

{-------------------------------------------------------------------------------
  Key and value types
-------------------------------------------------------------------------------}

-- TODO: maybe use reference impl generators here?

newtype Key1   = Key1   { _unKey1 :: QC.Small Word64 }
  deriving newtype (Show, Eq, Ord, Arbitrary, R.SerialiseKey)
newtype Value1 = Value1 { _unValue1 :: QC.Small Word64 }
  deriving newtype (Show, Eq, Ord, Arbitrary, R.SerialiseValue)
newtype Blob1  = Blob1  { _unBlob1 :: QC.Small Word64 }
  deriving newtype (Show, Eq, Ord, Arbitrary, R.SerialiseValue)

instance R.Labellable (Key1, Value1, Blob1) where
  makeSnapshotLabel _ = "Key1 Value1 Blob1"

newtype Key2   = Key2   { _unKey2   :: KeyForIndexCompact }
  deriving newtype (Show, Eq, Ord, Arbitrary, R.SerialiseKey)
newtype Value2 = Value2 { _unValue2 :: BS.ByteString }
  deriving newtype (Show, Eq, Ord, Arbitrary, R.SerialiseValue)
newtype Blob2  = Blob2  { _unBlob2  :: BS.ByteString }
  deriving newtype (Show, Eq, Ord, Arbitrary, R.SerialiseValue)

instance R.Labellable (Key2, Value2, Blob2) where
  makeSnapshotLabel _ = "Key2 Value2 Blob2"

{-------------------------------------------------------------------------------
  Model state
-------------------------------------------------------------------------------}

type ModelState :: ((Type -> Type) -> Type -> Type -> Type -> Type) -> Type
data ModelState h = ModelState Model.Model Stats
  deriving Show

initModelState :: ModelState h
initModelState = ModelState Model.initModel initStats

{-------------------------------------------------------------------------------
  Type synonyms
-------------------------------------------------------------------------------}

type Act h a = Action (Lockstep (ModelState h)) (Either Model.Err a)
type Var h a = ModelVar (ModelState h) a
type Val h a = ModelValue (ModelState h) a
type Obs h a = Observable (ModelState h) a

type K a = (
    Model.C_ a
  , R.SerialiseKey a
  , Arbitrary a
  )

type V a = (
    Model.C_ a
  , R.SerialiseValue a
  , Arbitrary a
  )

-- | Common constraints for keys, values and blobs
type C k v blob = (K k, V v, V blob)

{-------------------------------------------------------------------------------
  StateModel
-------------------------------------------------------------------------------}

instance ( Show (Class.TableConfig h)
         , Eq (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => StateModel (Lockstep (ModelState h)) where
  data instance Action (Lockstep (ModelState h)) a where
    -- Tables
    New :: C k v blob
        => Class.TableConfig h
        -> Act h (WrapTableHandle h IO k v blob)
    Close :: C k v blob
          => Var h (WrapTableHandle h IO k v blob)
          -> Act h ()
    -- Table queries
    Lookups :: C k v blob
            => V.Vector k -> Var h (WrapTableHandle h IO k v blob)
            -> Act h (V.Vector (R.LookupResult v (WrapBlobRef h IO blob)))
    RangeLookup :: C k v blob
                => R.Range k -> Var h (WrapTableHandle h IO k v blob)
                -> Act h (V.Vector (R.RangeLookupResult k v (WrapBlobRef h IO blob)))
    -- Updates
    Updates :: C k v blob
            => V.Vector (k, R.Update v blob) -> Var h (WrapTableHandle h IO k v blob)
            -> Act h ()
    Inserts :: C k v blob
            => V.Vector (k, v, Maybe blob) -> Var h (WrapTableHandle h IO k v blob)
            -> Act h ()
    Deletes :: C k v blob
            => V.Vector k -> Var h (WrapTableHandle h IO k v blob)
            -> Act h ()
    -- Blobs
    RetrieveBlobs :: V blob
                  => Var h (V.Vector (WrapBlobRef h IO blob))
                  -> Act h (V.Vector (WrapBlob blob))
    -- Snapshots
    Snapshot :: (C k v blob, R.Labellable (k, v, blob))
             => R.SnapshotName -> Var h (WrapTableHandle h IO k v blob)
             -> Act h ()
    Open     :: (C k v blob, R.Labellable (k, v, blob))
             => R.SnapshotName
             -> Act h (WrapTableHandle h IO k v blob)
    DeleteSnapshot :: R.SnapshotName -> Act h ()
    ListSnapshots  :: Act h [R.SnapshotName]
    -- Multiple writable table handles
    Duplicate :: C k v blob
              => Var h (WrapTableHandle h IO k v blob)
              -> Act h (WrapTableHandle h IO k v blob)

  initialState    = Lockstep.Defaults.initialState initModelState
  nextState       = Lockstep.Defaults.nextState
  precondition    = Lockstep.Defaults.precondition
  arbitraryAction = Lockstep.Defaults.arbitraryAction
  shrinkAction    = Lockstep.Defaults.shrinkAction

-- TODO: show instance does not show key-value-blob types. Example:
--
-- Normal.StateMachine
--   propLockstepIO_ModelIOImpl: FAIL
--     *** Failed! Exception: 'open: inappropriate type (table type mismatch)' (after 25 tests and 2 shrinks):
--     do action $ New TableConfig
--        action $ Snapshot "snap" (GVar var1 (FromRight . id))
--        action $ Open "snap"
--        pure ()
deriving instance Show (Class.TableConfig h)
               => Show (LockstepAction (ModelState h) a)

instance ( Eq (Class.TableConfig h)
         , Typeable h
         ) => Eq (LockstepAction (ModelState h) a) where
  (==) :: LockstepAction (ModelState h) a -> LockstepAction (ModelState h) a -> Bool
  x == y = go x y
    where
      go :: LockstepAction (ModelState h) a -> LockstepAction (ModelState h) a -> Bool
      go (New conf1)                (New conf2) =
          conf1 == conf2
      go (Close var1)               (Close var2) =
          Just var1 == cast var2
      go (Lookups ks1 var1)         (Lookups ks2 var2) =
          Just ks1 == cast ks2 && Just var1 == cast var2
      go (RangeLookup range1 var1)  (RangeLookup range2 var2) =
          range1 == range2 && var1 == var2
      go (Updates ups1 var1)        (Updates ups2 var2) =
          Just ups1 == cast ups2 && Just var1 == cast var2
      go (Inserts inss1 var1)       (Inserts inss2 var2) =
          Just inss1 == cast inss2 && Just var1 == cast var2
      go (Deletes ks1 var1)         (Deletes ks2 var2) =
          Just ks1 == cast ks2 && Just var1 == cast var2
      go (RetrieveBlobs vars1) (RetrieveBlobs vars2) =
          Just vars1 == cast vars2
      go (Snapshot name1 var1)      (Snapshot name2 var2) =
          name1 == name2 && Just var1 == cast var2
      go (Open name1)               (Open name2) =
          name1 == name2
      go (DeleteSnapshot name1)     (DeleteSnapshot name2) =
          name1 == name2
      go ListSnapshots ListSnapshots =
          True
      go (Duplicate var1) (Duplicate var2) =
          Just var1 == cast var2
      go _  _ = False

      _coveredAllCases :: LockstepAction (ModelState h) a -> ()
      _coveredAllCases = \case
          New{} -> ()
          Close{} -> ()
          Lookups{} -> ()
          RangeLookup{} -> ()
          Updates{} -> ()
          Inserts{} -> ()
          Deletes{} -> ()
          RetrieveBlobs{} -> ()
          Snapshot{} -> ()
          DeleteSnapshot{} -> ()
          ListSnapshots{} -> ()
          Open{} -> ()
          Duplicate{} -> ()

{-------------------------------------------------------------------------------
  InLockstep
-------------------------------------------------------------------------------}

instance ( Eq (Class.TableConfig h)
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => InLockstep (ModelState h) where
  type instance ModelOp (ModelState h) = Op

  data instance ModelValue (ModelState h) a where
    MTableHandle :: Model.TableHandle k v blob
                 -> Val h (WrapTableHandle h IO k v blob)
    MBlobRef :: Model.C_ blob
             => Model.BlobRef blob -> Val h (WrapBlobRef h IO blob)

    MLookupResult :: (Model.C_ v, Model.C_ blob)
                  => Model.LookupResult v (Val h (WrapBlobRef h IO blob))
                  -> Val h (R.LookupResult v (WrapBlobRef h IO blob))
    MRangeLookupResult :: Model.C k v blob
                       => Model.RangeLookupResult k v (Val h (WrapBlobRef h IO blob))
                       -> Val h (R.RangeLookupResult k v (WrapBlobRef h IO blob))

    MBlob :: (Show blob, Typeable blob, Eq blob)
          => WrapBlob blob -> Val h (WrapBlob blob)
    MSnapshotName :: R.SnapshotName -> Val h R.SnapshotName
    MErr :: Model.Err -> Val h Model.Err

    MUnit   :: () -> Val h ()
    MPair   :: (Val h a, Val h b) -> Val h (a, b)
    MEither :: Either (Val h a) (Val h b) -> Val h (Either a b)
    MList   :: [Val h a] -> Val h [a]
    MVector :: V.Vector (Val h a) -> Val h (V.Vector a)

  data instance Observable (ModelState h) a where
    OTableHandle :: Obs h (WrapTableHandle h IO k v blob)
    OBlobRef :: Obs h (WrapBlobRef h IO blob)

    OLookupResult :: (Model.C_ v, Model.C_ blob)
                  => Model.LookupResult v (Obs h (WrapBlobRef h IO blob))
                  -> Obs h (R.LookupResult v (WrapBlobRef h IO blob))
    ORangeLookupResult :: Model.C k v blob
                       => Model.RangeLookupResult k v (Obs h (WrapBlobRef h IO blob))
                       -> Obs h (R.RangeLookupResult k v (WrapBlobRef h IO blob))
    OBlob :: (Show blob, Typeable blob, Eq blob)
          => WrapBlob blob -> Obs h (WrapBlob blob)

    OId :: (Show a, Typeable a, Eq a) => a -> Obs h a

    OPair   :: (Obs h a, Obs h b) -> Obs h (a, b)
    OEither :: Either (Obs h a) (Obs h b) -> Obs h (Either a b)
    OList   :: [Obs h a] -> Obs h [a]
    OVector :: V.Vector (Obs h a) -> Obs h (V.Vector a)

  observeModel :: Val h a -> Obs h a
  observeModel = \case
      MTableHandle _       -> OTableHandle
      MBlobRef _           -> OBlobRef
      MLookupResult x      -> OLookupResult $ fmap observeModel x
      MRangeLookupResult x -> ORangeLookupResult $ fmap observeModel x
      MSnapshotName x      -> OId x
      MBlob x              -> OId x
      MErr x               -> OId x
      MUnit x              -> OId x
      MPair x              -> OPair $ bimap observeModel observeModel x
      MEither x            -> OEither $ bimap observeModel observeModel x
      MList x              -> OList $ map observeModel x
      MVector x            -> OVector $ V.map observeModel x

  modelNextState ::  forall a.
       LockstepAction (ModelState h) a
    -> ModelLookUp (ModelState h)
    -> ModelState h
    -> (ModelValue (ModelState h) a, ModelState h)
  modelNextState action lookUp (ModelState mock stats) =
      auxStats $ runModel lookUp action mock
    where
      auxStats :: (Val h a, Model.Model) -> (Val h a, ModelState h)
      auxStats (result, state') =
          (result, ModelState state' $ updateStats action result stats)

  usedVars :: LockstepAction (ModelState h) a -> [AnyGVar (ModelOp (ModelState h))]
  usedVars = \case
      New _                           -> []
      Close tableVar                  -> [SomeGVar tableVar]
      Lookups _ tableVar              -> [SomeGVar tableVar]
      RangeLookup _ tableVar          -> [SomeGVar tableVar]
      Updates _ tableVar              -> [SomeGVar tableVar]
      Inserts _ tableVar              -> [SomeGVar tableVar]
      Deletes _ tableVar              -> [SomeGVar tableVar]
      RetrieveBlobs blobsVar          -> [SomeGVar blobsVar]
      Snapshot _ tableVar             -> [SomeGVar tableVar]
      Open _                          -> []
      DeleteSnapshot _                -> []
      ListSnapshots                   -> []
      Duplicate tableVar              -> [SomeGVar tableVar]

  arbitraryWithVars ::
       ModelFindVariables (ModelState h)
    -> ModelState h
    -> Gen (Any (LockstepAction (ModelState h)))
  arbitraryWithVars findVars st = QC.oneof [
        arbitraryActionWithVars (Proxy @(Key1, Value1, Blob1)) findVars st
      , arbitraryActionWithVars (Proxy @(Key2, Value2, Blob2)) findVars st
      ]

  shrinkWithVars ::
       ModelFindVariables (ModelState h)
    -> ModelState h
    -> LockstepAction (ModelState h) a
    -> [Any (LockstepAction (ModelState h))]
  shrinkWithVars = shrinkActionWithVars

  tagStep ::
       (ModelState h, ModelState h)
    -> LockstepAction (ModelState h) a
    -> Val h a
    -> [String]
  tagStep states action = map show . tagStep' states action

deriving instance Show (Class.TableConfig h) => Show (Val h a)
deriving instance Show (Obs h a)

instance Eq (Obs h a) where
  obsReal == obsModel = case (obsReal, obsModel) of
      -- The model is conservative about blob retrieval: the model invalidates a
      -- blob reference immediately after an update to the table, and if the SUT
      -- returns a blob, then that's okay. If both return a blob or both return
      -- an error, then those must match exactly.
      (OEither (Right (OBlob (WrapBlob _))), OEither (Left (OId y)))
        | Just Model.ErrBlobRefInvalidated <- cast y -> True
      -- default equalities
      (OTableHandle, OTableHandle) -> True
      (OBlobRef, OBlobRef) -> True
      (OLookupResult x, OLookupResult y) -> x == y
      (ORangeLookupResult x, ORangeLookupResult y) -> x == y
      (OBlob x, OBlob y) -> x == y
      (OId x, OId y) -> x == y
      (OPair x, OPair y) -> x == y
      (OEither x, OEither y) -> x == y
      (OList x, OList y) -> x == y
      (OVector x, OVector y) -> x == y
      (_, _) -> False
    where
      _coveredAllCases :: Obs h a -> ()
      _coveredAllCases = \case
          OTableHandle{} -> ()
          OBlobRef{} -> ()
          OLookupResult{} -> ()
          ORangeLookupResult{} -> ()
          OBlob{} -> ()
          OId{} -> ()
          OPair{} -> ()
          OEither{} -> ()
          OList{} -> ()
          OVector{} -> ()

{-------------------------------------------------------------------------------
  Real monad
-------------------------------------------------------------------------------}

-- | Uses 'WrapSession' such that we can define class instances that mention
-- 'RealMonad' in the class head. This is necessary because class heads can not
-- mention type families directly.
--
-- Also carries an exception handle that is specific to the table implementation
-- identified by the table handle @h@.
type RealMonad h m = ReaderT (WrapSession h m, Handler m (Maybe Model.Err)) m

{-------------------------------------------------------------------------------
  RunLockstep
-------------------------------------------------------------------------------}

instance ( Eq (Class.TableConfig h)
         , Class.IsTableHandle h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => RunLockstep (ModelState h) (RealMonad h IO) where
  observeReal ::
       Proxy (RealMonad h IO)
    -> LockstepAction (ModelState h) a
    -> Realized (RealMonad h IO) a
    -> Obs h a
  observeReal _proxy action result = case action of
      New{}            -> OEither $ bimap OId (const OTableHandle) result
      Close{}          -> OEither $ bimap OId OId result
      Lookups{}        -> OEither $
          bimap OId (OVector . fmap (OLookupResult . fmap (const OBlobRef))) result
      RangeLookup{}    -> OEither $
          bimap OId (OVector . fmap (ORangeLookupResult . fmap (const OBlobRef))) result
      Updates{}        -> OEither $ bimap OId OId result
      Inserts{}        -> OEither $ bimap OId OId result
      Deletes{}        -> OEither $ bimap OId OId result
      RetrieveBlobs{}  -> OEither $ bimap OId (OVector . fmap OId) result
      Snapshot{}       -> OEither $ bimap OId OId result
      Open{}           -> OEither $ bimap OId (const OTableHandle) result
      DeleteSnapshot{} -> OEither $ bimap OId OId result
      ListSnapshots{}  -> OEither $ bimap OId (OList . fmap OId) result
      Duplicate{}      -> OEither $ bimap OId (const OTableHandle) result

  showRealResponse ::
       Proxy (RealMonad h IO)
    -> LockstepAction (ModelState h) a
    -> Maybe (Dict (Show (Realized (RealMonad h IO) a)))
  showRealResponse _ = \case
      New{}            -> Nothing
      Close{}          -> Just Dict
      Lookups{}        -> Nothing
      RangeLookup{}    -> Nothing
      Updates{}        -> Just Dict
      Inserts{}        -> Just Dict
      Deletes{}        -> Just Dict
      RetrieveBlobs{}  -> Just Dict
      Snapshot{}       -> Just Dict
      Open{}           -> Nothing
      DeleteSnapshot{} -> Just Dict
      ListSnapshots    -> Just Dict
      Duplicate{}      -> Nothing

{- TODO: temporarily disabled until we start on I/O fault testing.
instance ( Eq (Class.TableConfig h)
         , Class.IsTableHandle h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => RunLockstep (ModelState h) (RealMonad h (IOSim s)) where
  observeReal ::
       Proxy (RealMonad h (IOSim s))
    -> LockstepAction (ModelState h) a
    -> Realized (RealMonad h (IOSim s)) a
    -> Obs h a
  observeReal _proxy action result = case action of
      New{}            -> OEither $ bimap OId (const OTableHandle) result
      Close{}          -> OEither $ bimap OId OId result
      Lookups{}        -> OEither $
          bimap OId (OVector . fmap (OLookupResult . fmap (const OBlobRef))) result
      RangeLookup{}    -> OEither $
          bimap OId (OVector . fmap (ORangeLookupResult . fmap (const OBlobRef))) result
      Updates{}        -> OEither $ bimap OId OId result
      Inserts{}        -> OEither $ bimap OId OId result
      Deletes{}        -> OEither $ bimap OId OId result
      RetrieveBlobs{}  -> OEither $ bimap OId (OVector . fmap OId) result
      Snapshot{}       -> OEither $ bimap OId OId result
      Open{}           -> OEither $ bimap OId (const OTableHandle) result
      DeleteSnapshot{} -> OEither $ bimap OId OId result
      ListSnapshots{}  -> OEither $ bimap OId OId result
      Duplicate{}      -> OEither $ bimap OId (const OTableHandle) result

  showRealResponse ::
       Proxy (RealMonad h (IOSim s))
    -> LockstepAction (ModelState h) a
    -> Maybe (Dict (Show (Realized (RealMonad h (IOSim s)) a)))
  showRealResponse _ = \case
      New{}            -> Nothing
      Close{}          -> Just Dict
      Lookups{}        -> Nothing
      RangeLookup{}    -> Nothing
      Updates{}        -> Just Dict
      Inserts{}        -> Just Dict
      Deletes{}        -> Just Dict
      RetrieveBlobs{}  -> Just Dict
      Snapshot{}       -> Just Dict
      Open{}           -> Nothing
      DeleteSnapshot{} -> Just Dict
      ListSnapshots    -> Just Dict
      Duplicate{}      -> Nothing
-}

{-------------------------------------------------------------------------------
  RunModel
-------------------------------------------------------------------------------}

instance ( Eq (Class.TableConfig h)
         , Class.IsTableHandle h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => RunModel (Lockstep (ModelState h)) (RealMonad h IO) where
  perform _     = runIO
  postcondition = Lockstep.Defaults.postcondition
  monitoring    = Lockstep.Defaults.monitoring (Proxy @(RealMonad h IO))

{- TODO: temporarily disabled until we start on I/O fault testing.
instance ( Eq (Class.TableConfig h)
         , Class.IsTableHandle h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => RunModel (Lockstep (ModelState h)) (RealMonad h (IOSim s)) where
  perform _     = runIOSim
  postcondition = Lockstep.Defaults.postcondition
  monitoring    = Lockstep.Defaults.monitoring (Proxy @(RealMonad h (IOSim s)))
-}

{-------------------------------------------------------------------------------
  Interpreter for the model
-------------------------------------------------------------------------------}

runModel ::
     ModelLookUp (ModelState h)
  -> LockstepAction (ModelState h) a
  -> Model.Model -> (Val h a, Model.Model)
runModel lookUp = \case
    New _cfg -> wrap MTableHandle .
      Model.runModelM (Model.new Model.TableConfig)
    Close tableVar -> wrap MUnit .
      Model.runModelM (Model.close (getTableHandle $ lookUp tableVar))
    Lookups ks tableVar -> wrap (MVector . fmap (MLookupResult . fmap MBlobRef)) .
      Model.runModelM (Model.lookups ks (getTableHandle $ lookUp tableVar))
    RangeLookup range tableVar -> wrap (MVector . fmap (MRangeLookupResult . fmap MBlobRef)) .
      Model.runModelM (Model.rangeLookup range (getTableHandle $ lookUp tableVar))
    Updates kups tableVar -> wrap MUnit .
      Model.runModelM (Model.updates kups (getTableHandle $ lookUp tableVar))
    Inserts kins tableVar -> wrap MUnit .
      Model.runModelM (Model.inserts kins (getTableHandle $ lookUp tableVar))
    Deletes kdels tableVar -> wrap MUnit .
      Model.runModelM (Model.deletes kdels (getTableHandle $ lookUp tableVar))
    RetrieveBlobs blobsVar -> wrap (MVector . fmap (MBlob . WrapBlob)) .
      Model.runModelM (Model.retrieveBlobs (getBlobRefs . lookUp $ blobsVar))
    Snapshot name tableVar -> wrap MUnit .
      Model.runModelM (Model.snapshot name (getTableHandle $ lookUp tableVar))
    Open name -> wrap MTableHandle .
      Model.runModelM (Model.open name)
    DeleteSnapshot name -> wrap MUnit .
      Model.runModelM (Model.deleteSnapshot name)
    ListSnapshots -> wrap (MList . fmap MSnapshotName) .
      Model.runModelM Model.listSnapshots
    Duplicate tableVar -> wrap MTableHandle .
      Model.runModelM (Model.duplicate (getTableHandle $ lookUp tableVar))
  where
    getTableHandle ::
         ModelValue (ModelState h) (WrapTableHandle h IO k v blob)
      -> Model.TableHandle k v blob
    getTableHandle (MTableHandle th) = th

    getBlobRefs :: ModelValue (ModelState h) (V.Vector (WrapBlobRef h IO blob)) -> V.Vector (Model.BlobRef blob)
    getBlobRefs (MVector brs) = fmap (\(MBlobRef br) -> br) brs

wrap ::
     (a -> Val h b)
  -> (Either Model.Err a, Model.Model)
  -> (Val h (Either Model.Err b), Model.Model)
wrap f = first (MEither . bimap MErr f)

{-------------------------------------------------------------------------------
  Interpreters for @'IOLike' m@
-------------------------------------------------------------------------------}

runIO ::
     forall a h. Class.IsTableHandle h
  => LockstepAction (ModelState h) a
  -> LookUp (RealMonad h IO)
  -> RealMonad h IO (Realized (RealMonad h IO) a)
runIO action lookUp = ReaderT $ \(session, handler) ->
    aux (unwrapSession session) handler action
  where
    aux ::
         Class.Session h IO
      -> Handler IO (Maybe Model.Err)
      -> LockstepAction (ModelState h) a
      -> IO (Realized IO a)
    aux session handler = \case
        New cfg -> catchErr handler $
          WrapTableHandle <$> Class.new session cfg
        Close tableVar -> catchErr handler $
          Class.close (unwrapTableHandle $ lookUp' tableVar)
        Lookups ks tableVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.lookups (unwrapTableHandle $ lookUp' tableVar) ks
        RangeLookup range tableVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.rangeLookup (unwrapTableHandle $ lookUp' tableVar) range
        Updates kups tableVar -> catchErr handler $
          Class.updates (unwrapTableHandle $ lookUp' tableVar) kups
        Inserts kins tableVar -> catchErr handler $
          Class.inserts (unwrapTableHandle $ lookUp' tableVar) kins
        Deletes kdels tableVar -> catchErr handler $
          Class.deletes (unwrapTableHandle $ lookUp' tableVar) kdels
        RetrieveBlobs blobRefsVar -> catchErr handler $
          fmap WrapBlob <$> Class.retrieveBlobs (Proxy @h) session (unwrapBlobRef <$> lookUp' blobRefsVar)
        Snapshot name tableVar -> catchErr handler $
          Class.snapshot name (unwrapTableHandle $ lookUp' tableVar)
        Open name -> catchErr handler $
          WrapTableHandle <$> Class.open session name
        DeleteSnapshot name -> catchErr handler $
          Class.deleteSnapshot session name
        ListSnapshots -> catchErr handler $
          Class.listSnapshots session
        Duplicate tableVar -> catchErr handler $
          WrapTableHandle <$> Class.duplicate (unwrapTableHandle $ lookUp' tableVar)

    lookUp' :: Var j x -> Realized IO x
    lookUp' = lookUpGVar (Proxy @(RealMonad h IO)) lookUp

{- TODO: temporarily disabled until we start on I/O fault testing.
runIOSim ::
     forall s a h. Class.IsTableHandle h
  => LockstepAction (ModelState h) a
  -> LookUp (RealMonad h (IOSim s))
  -> RealMonad h (IOSim s) (Realized (RealMonad h (IOSim s)) a)
runIOSim action lookUp = ReaderT $ \(session, handler) ->
    aux (unwrapSession session) handler action
  where
    aux ::
         Class.Session h (IOSim s)
      -> Handler (IOSim s) (Maybe Model.Err)
      -> LockstepAction (ModelState h) a
      -> IOSim s (Realized (IOSim s) a)
    aux session handler = \case
        New cfg -> catchErr handler $
          WrapTableHandle <$> Class.new session cfg
        Close tableVar -> catchErr handler $
          Class.close (unwrapTableHandle $ lookUp' tableVar)
        Lookups ks tableVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.lookups (unwrapTableHandle $ lookUp' tableVar) ks
        RangeLookup range tableVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.rangeLookup (unwrapTableHandle $ lookUp' tableVar) range
        Updates kups tableVar -> catchErr handler $
          Class.updates (unwrapTableHandle $ lookUp' tableVar) kups
        Inserts kins tableVar -> catchErr handler $
          Class.inserts (unwrapTableHandle $ lookUp' tableVar) kins
        Deletes kdels tableVar -> catchErr handler $
          Class.deletes (unwrapTableHandle $ lookUp' tableVar) kdels
        RetrieveBlobs blobRefsVar -> catchErr handler $
          fmap WrapBlob <$> Class.retrieveBlobs (Proxy @h) session (unwrapBlobRef <$> lookUp' blobRefsVar)
        Snapshot name tableVar -> catchErr handler $
          Class.snapshot name (unwrapTableHandle $ lookUp' tableVar)
        Open name -> catchErr handler $
          WrapTableHandle <$> Class.open session name
        DeleteSnapshot name -> catchErr handler $
          Class.deleteSnapshot session name
        ListSnapshots -> catchErr handler $
          Class.listSnapshots session
        Duplicate tableVar -> catchErr handler $
          WrapTableHandle <$> Class.duplicate (unwrapTableHandle $ lookUp' tableVar)
    lookUp' :: Var h x -> Realized (IOSim s) x
    lookUp' = lookUpGVar (Proxy @(RealMonad h (IOSim s))) lookUp
-}

catchErr ::
     forall m a. MonadCatch m
  => Handler m (Maybe Model.Err) -> m a -> m (Either Model.Err a)
catchErr (Handler f) action = catch (Right <$> action) f'
  where
    f' e = maybe (throwIO e) (pure . Left) =<< f e

{-------------------------------------------------------------------------------
  Generator and shrinking
-------------------------------------------------------------------------------}

arbitraryActionWithVars ::
     forall h k v blob. (
       C k v blob
     , R.Labellable (k, v, blob)
     , Eq (Class.TableConfig h)
     , Arbitrary (Class.TableConfig h)
     , Typeable h
     )
  => Proxy (k, v, blob)
  -> ModelFindVariables (ModelState h)
  -> ModelState h
  -> Gen (Any (LockstepAction (ModelState h)))
arbitraryActionWithVars _ findVars _st = QC.frequency $ concat [
      withoutVars
    , case findVars (Proxy @(Either Model.Err (WrapTableHandle h IO k v blob))) of
        []   -> []
        vars -> withVars (QC.elements vars)
    , case findBlobRefsVars of
        []    -> []
        vars' -> withVars' (QC.elements vars')
    ]
  where
    _coveredAllCases :: LockstepAction (ModelState h) a -> ()
    _coveredAllCases = \case
        New{} -> ()
        Close{} -> ()
        Lookups{} -> ()
        RangeLookup{} -> ()
        Updates{} -> ()
        Inserts{} -> ()
        Deletes{} -> ()
        RetrieveBlobs{} -> ()
        Snapshot{} -> ()
        DeleteSnapshot{} -> ()
        ListSnapshots{} -> ()
        Open{} -> ()
        Duplicate{} -> ()

    findBlobRefsVars :: [Var h (Either Model.Err (V.Vector (WrapBlobRef h IO blob)))]
    findBlobRefsVars = fmap fromLookupResults vars1 ++ fmap fromRangeLookupResults vars2
      where
        vars1 = findVars (Proxy @(Either Model.Err (V.Vector (R.LookupResult v (WrapBlobRef h IO blob)))))
        vars2 = findVars (Proxy @(Either Model.Err (V.Vector (R.RangeLookupResult k v (WrapBlobRef h IO blob)))))

        fromLookupResults ::
             Var h (Either Model.Err (V.Vector (R.LookupResult v (WrapBlobRef h IO blob))))
          -> Var h (Either Model.Err (V.Vector (WrapBlobRef h IO blob)))
        fromLookupResults = mapGVar (\op -> OpRight `OpComp` OpLookupResults `OpComp` OpFromRight `OpComp` op)

        fromRangeLookupResults ::
             Var h (Either Model.Err (V.Vector (R.RangeLookupResult k v (WrapBlobRef h IO blob))))
          -> Var h (Either Model.Err (V.Vector (WrapBlobRef h IO blob)))
        fromRangeLookupResults = mapGVar (\op -> OpRight `OpComp` OpRangeLookupResults `OpComp` OpFromRight `OpComp` op)

    withoutVars :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    withoutVars = [
          (5, Some . New @k @v @blob <$> QC.arbitrary)
        , (3, fmap Some $ Open @k @v @blob <$> genSnapshotName)
        , (1, fmap Some $ DeleteSnapshot <$> genSnapshotName)
        , (1, pure $ Some ListSnapshots)
        ]

    withVars ::
         Gen (Var h (Either Model.Err (WrapTableHandle h IO k v blob)))
      -> [(Int, Gen (Any (LockstepAction (ModelState h))))]
    withVars genVar = [
          (2, fmap Some $ Close <$> (fromRight <$> genVar))
        , (10, fmap Some $ Lookups <$> genLookupKeys <*> (fromRight <$> genVar))
        -- TODO: enable generators as we implement the actions for the /real/ lsm-tree
        -- , fmap Some $ RangeLookup <$> genRange <*> (fromRight <$> genVar)
        , (10, fmap Some $ Updates <$> genUpdates <*> (fromRight <$> genVar))
        , (10, fmap Some $ Inserts <$> genInserts <*> (fromRight <$> genVar))
        , (10, fmap Some $ Deletes <$> genDeletes <*> (fromRight <$> genVar))
        , (3, fmap Some $ Snapshot <$> genSnapshotName <*> (fromRight <$> genVar))
        -- TODO: enable generators as we implement the actions for the /real/ lsm-tree
        -- , fmap Some $ Duplicate <$> (fromRight <$> genVar)
        ]

    withVars' ::
         Gen (Var h (Either Model.Err (V.Vector (WrapBlobRef h IO blob))))
      -> [(Int, Gen (Any (LockstepAction (ModelState h))))]
    withVars' _genBlobRefsVar = [
          -- TODO: enable generators as we implement the actions for the /real/ lsm-tree
          -- fmap Some $ RetrieveBlobs <$> (fromRight <$> genBlobRefsVar)
        ]

    fromRight ::
         Var h (Either Model.Err a)
      -> Var h a
    fromRight = mapGVar (\op -> OpFromRight `OpComp` op)

    genLookupKeys :: Gen (V.Vector k)
    genLookupKeys = QC.arbitrary

    _genRange :: Gen (R.Range k)
    _genRange = QC.oneof [
          R.FromToExcluding <$> QC.arbitrary <*> QC.arbitrary
        , R.FromToIncluding <$> QC.arbitrary <*> QC.arbitrary
        ]
      where
        _coveredAllCases :: R.Range k -> ()
        _coveredAllCases = \case
            R.FromToExcluding{} -> ()
            R.FromToIncluding{} -> ()

    genUpdates :: Gen (V.Vector (k, R.Update v blob))
    genUpdates = QC.liftArbitrary ((,) <$> QC.arbitrary <*> QC.oneof [
          R.Insert <$> QC.arbitrary <*> genBlob
        , pure R.Delete
        ])
      where
        _coveredAllCases :: R.Update v blob -> ()
        _coveredAllCases = \case
            R.Insert{} -> ()
            R.Delete{} -> ()

    genInserts :: Gen (V.Vector (k, v, Maybe blob))
    genInserts = QC.liftArbitrary ((,,) <$> QC.arbitrary <*> QC.arbitrary <*> genBlob)

    genDeletes :: Gen (V.Vector k)
    genDeletes = QC.arbitrary

    -- TODO: generate @Just blob@ once blob references are implemented
    genBlob :: Gen (Maybe blob)
    genBlob = Nothing <$ QC.arbitrary @blob

    genSnapshotName :: Gen R.SnapshotName
    genSnapshotName = QC.elements [
        fromJust $ R.mkSnapshotName "snap1"
      , fromJust $ R.mkSnapshotName "snap2"
      , fromJust $ R.mkSnapshotName "snap3"
      ]

shrinkActionWithVars ::
     forall h a. (
       Eq (Class.TableConfig h)
     , Arbitrary (Class.TableConfig h)
     , Typeable h
     )
  => ModelFindVariables (ModelState h)
  -> ModelState h
  -> LockstepAction (ModelState h) a
  -> [Any (LockstepAction (ModelState h))]
shrinkActionWithVars _ _ = \case
    New @k @v @blob conf -> [ Some $ New @k @v @blob conf' | conf' <- QC.shrink conf ]
    Inserts kins tableVar -> [ Some $ Inserts kins' tableVar | kins' <- QC.shrink kins ]
    Lookups ks tableVar -> [ Some $ Lookups ks' tableVar | ks' <- QC.shrink ks ]
    _ -> []

{-------------------------------------------------------------------------------
  Interpret 'Op' against 'ModelValue'
-------------------------------------------------------------------------------}

instance InterpretOp Op (ModelValue (ModelState h)) where
  intOp = \case
    OpId                 -> Just
    OpFst                -> \case MPair   x -> Just (fst x)
    OpSnd                -> \case MPair   x -> Just (snd x)
    OpLeft               -> Just . MEither . Left
    OpRight              -> Just . MEither . Right
    OpFromLeft           -> \case MEither x -> either Just (const Nothing) x
    OpFromRight          -> \case MEither x -> either (const Nothing) Just x
    OpComp g f           -> intOp g <=< intOp f
    OpLookupResults      -> Just . MVector
                          . V.mapMaybe (\case MLookupResult x -> getBlobRef x)
                          . \case MVector x -> x
    OpRangeLookupResults -> Just . MVector
                          . V.mapMaybe (\case MRangeLookupResult x -> getBlobRef x)
                          . \case MVector x -> x

{-------------------------------------------------------------------------------
  Statistics, labelling/tagging
-------------------------------------------------------------------------------}

data ConstructorName =
    CNew
  | CClose
  | CLookups
  | CRangeLookup
  | CUpdates
  | CInserts
  | CDeletes
  | CRetrieveBlobs
  | CSnapshot
  | COpen
  | CDeleteSnapshot
  | CListSnapshots
  | CDuplicate
  deriving (Show, Eq, Ord, Enum, Bounded)

toConstructorName :: LockstepAction (ModelState h) a -> ConstructorName
toConstructorName = \case
    New{} -> CNew
    Close{} -> CClose
    Lookups{} -> CLookups
    RangeLookup{} -> CRangeLookup
    Updates{} -> CUpdates
    Inserts{} -> CInserts
    Deletes{} -> CDeletes
    RetrieveBlobs{} -> CRetrieveBlobs
    Snapshot{} -> CSnapshot
    Open{} -> COpen
    DeleteSnapshot{} -> CDeleteSnapshot
    ListSnapshots{} -> CListSnapshots
    Duplicate{} -> CDuplicate

newtype Stats = Stats {
    seenActions :: Set ConstructorName
  }
  deriving Show

initStats :: Stats
initStats = Stats {
      seenActions = Set.empty
    }

updateStats :: LockstepAction (ModelState h) a -> Val h a -> Stats -> Stats
updateStats action _result Stats{seenActions} = Stats {
      seenActions = Set.insert (toConstructorName action) seenActions
    }

data Tag =
    -- | Trivial tag
    Top
    -- | All actions were seen at least once
  | All
  deriving Show

tagStep' ::
     (ModelState h, ModelState h)
  -> LockstepAction (ModelState h) a
  -> Val h a
  -> [Tag]
tagStep' (_before, ModelState _ statsAfter) _action _result = Top :
    [All | seenActions statsAfter == Set.fromList [minBound .. maxBound]]
