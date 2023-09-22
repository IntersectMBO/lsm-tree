{-# LANGUAGE ConstraintKinds       #-}
{-# LANGUAGE EmptyDataDeriving     #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}
{-# LANGUAGE NamedFieldPuns        #-}
{-# LANGUAGE RankNTypes            #-}
{-# LANGUAGE ScopedTypeVariables   #-}
{-# LANGUAGE StandaloneDeriving    #-}
{-# LANGUAGE TypeApplications      #-}
{-# LANGUAGE TypeFamilies          #-}

{-
  TODO: improve generation and shrinking of dependencies. See
  https://github.com/input-output-hk/lsm-tree/pull/4#discussion_r1334295154.

  TODO: The 'RetrieveBlobs' action will currently only retrieve blob references
  that come from a single batch lookup or range lookup. It would be interesting
  to also retrieve blob references from a mix of different batch lookups and/or
  range lookups. This would require some non-trivial changes, such as changes to
  'Op' to also include expressions for manipulating lists, such that we can map
  @'Var' ['SUT.BlobRef' blob]@ to @'Var' ('SUT.BlobRef' blob)@. 'RetrieveBlobs'
  would then hold a list of variables (e.g., @['Var' ('SUT.BlobRef blob')]@)
  instead of a variable of a list (@'Var' ['SUT.BlobRef' blob]@).

  TODO: it is currently not correctly modelled what happens if blob references
  are retrieved from an incorrect table handle.
-}
module Test.Database.LSMTree.Normal.StateMachine (tests) where

import           Control.Exception (SomeException (..))
import           Control.Monad ((<=<))
import           Control.Monad.Class.MonadThrow (MonadCatch (..),
                     MonadThrow (..))
import           Control.Monad.IOSim (IOSim)
import           Control.Monad.Reader (ReaderT (..))
import           Data.Bifunctor (Bifunctor (..))
import           Data.Maybe (fromJust, mapMaybe)
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Typeable (Proxy (..), Typeable, cast)
import           Data.Word (Word64)
import qualified Database.LSMTree.Common as SUT
import qualified Database.LSMTree.Model.Normal.Session as Model
import qualified Database.LSMTree.Normal as SUT
import           System.FS.API (MountPoint (..), SomeHasFS (..))
import           System.FS.IO (ioHasFS)
import           System.IO.Temp (createTempDirectory,
                     getCanonicalTemporaryDirectory)
import           Test.Database.LSMTree.Normal.StateMachine.Op
                     (HasBlobRef (getBlobRef), Op (..))
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary, Gen)
import           Test.QuickCheck.StateModel hiding (Var, vars)
import           Test.QuickCheck.StateModel.Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Defaults as Lockstep.Defaults
import qualified Test.QuickCheck.StateModel.Lockstep.Run as Lockstep.Run
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.HUnit (testCase)
import           Test.Util.Orphans (Wrap (..))

{-------------------------------------------------------------------------------
  Test tree
-------------------------------------------------------------------------------}

tests :: TestTree
tests = testGroup "Normal.StateMachine" [
      testCase "labelledExamples" $
        QC.labelledExamples $ Lockstep.Run.tagActions (Proxy @ModelState)
      -- TODO: enable once we at least have an implementation for SUT sessions.
    --  , testProperty "propLockstep @IO" $
    --     Lockstep.Run.runActionsBracket (Proxy @ModelState)
    --       (createSystemTempDirectory "Lockstep")
    --       (removeDirectoryRecursive . fst)
    --       (\r (_, someHasFS) -> SUT.newSession someHasFS (mkFsPath []) >>= runReaderT r)
    -- , testProperty "propLockstep @IOSim" $
    --     Lockstep.Run.runActionsBracket (Proxy @ModelState)
    --       (SomeHasFS <$> simHasFS' MockFS.empty)
    --       (const (pure ()))
    --       (\r someHasFS -> SUT.newSession someHasFS (mkFsPath []) >>= runReaderT r)
    ]

-- TODO: enable once we at least have an implementation for SUT sessions.
_createSystemTempDirectory ::  [Char] -> IO (FilePath, SomeHasFS IO)
_createSystemTempDirectory prefix = do
    systemTempDir <- getCanonicalTemporaryDirectory
    tempDir <- createTempDirectory systemTempDir prefix
    pure (tempDir, SomeHasFS $ ioHasFS (MountPoint tempDir))

{-------------------------------------------------------------------------------
  Model state
-------------------------------------------------------------------------------}

data ModelState = ModelState Model.Model Stats
  deriving Show

initModelState :: ModelState
initModelState = ModelState Model.initModel initStats

{-------------------------------------------------------------------------------
  Type synonyms
-------------------------------------------------------------------------------}

type Act a = Action (Lockstep ModelState) (Either Model.Err a)
type Var a = ModelVar ModelState a
type Val a = ModelValue ModelState a
type Obs a = Observable ModelState a

type RealMonad m = ReaderT (SUT.Session m) m

-- | Common constraints for keys, values and blobs
type C k v blob = (
    Model.C k v blob
  , Model.SomeSerialisationConstraint k
  , Model.SomeSerialisationConstraint v
  , Model.SomeSerialisationConstraint blob
  , SUT.SomeSerialisationConstraint k
  , SUT.SomeSerialisationConstraint v
  , SUT.SomeSerialisationConstraint blob
  )

{-------------------------------------------------------------------------------
  StateModel
-------------------------------------------------------------------------------}

instance StateModel (Lockstep ModelState) where
  data instance Action (Lockstep ModelState) a where
    -- Tables
    New :: C k v blob
        => SUT.TableConfig -> Act (SUT.TableHandle IO k v blob)
    Close :: C k v blob
          => Var (SUT.TableHandle IO k v blob) -> Act ()
    -- Table queries
    Lookups :: C k v blob
            => [k] -> Var (SUT.TableHandle IO k v blob)
            -> Act [SUT.LookupResult k v (SUT.BlobRef blob)]
    RangeLookup :: C k v blob
                => SUT.Range k -> Var (SUT.TableHandle IO k v blob)
                -> Act [SUT.RangeLookupResult k v (SUT.BlobRef blob)]
    -- Updates
    Updates :: C k v blob
            => [(k, SUT.Update v blob)] -> Var (SUT.TableHandle IO k v blob) -> Act ()
    Inserts :: C k v blob
            => [(k, v, Maybe blob)] -> Var (SUT.TableHandle IO k v blob) -> Act ()
    Deletes :: C k v blob
            => [k] -> Var (SUT.TableHandle IO k v blob) -> Act ()
    -- Blobs
    RetrieveBlobs :: C k v blob
                  => Var (SUT.TableHandle IO k v blob) -> Var [SUT.BlobRef blob]
                  -> Act [Wrap blob]
    -- Snapshots
    Snapshot :: C k v blob
             => SUT.SnapshotName -> Var (SUT.TableHandle IO k v blob) -> Act ()
    Open     :: C k v blob
             => SUT.SnapshotName -> Act (SUT.TableHandle IO k v blob)
    -- Multiple writable table handles
    Duplicate :: C k v blob
              => Var (SUT.TableHandle IO k v blob) -> Act (SUT.TableHandle IO k v blob)

  initialState    = Lockstep.Defaults.initialState initModelState
  nextState       = Lockstep.Defaults.nextState
  precondition    = Lockstep.Defaults.precondition
  arbitraryAction = Lockstep.Defaults.arbitraryAction
  shrinkAction    = Lockstep.Defaults.shrinkAction

deriving instance Show (LockstepAction ModelState a)

instance Eq (LockstepAction ModelState a) where
  (==) :: LockstepAction ModelState a -> LockstepAction ModelState a -> Bool
  x == y = go x y
    where
      go :: LockstepAction ModelState a -> LockstepAction ModelState a -> Bool
      go (New conf1)                (New conf2) =
          conf1 == conf2
      go (Close var1)               (Close var2) =
          Just var1 == cast var2
      go (Lookups ks1 var1)         (Lookups ks2 var2) =
          ks1 == ks2 && var1 == var2
      go (RangeLookup range1 var1)  (RangeLookup range2 var2) =
          range1 == range2 && var1 == var2
      go (Updates ups1 var1)        (Updates ups2 var2) =
          Just ups1 == cast ups2 && Just var1 == cast var2
      go (Inserts inss1 var1)       (Inserts inss2 var2) =
          Just inss1 == cast inss2 && Just var1 == cast var2
      go (Deletes ks1 var1)         (Deletes ks2 var2) =
          Just ks1 == cast ks2 && Just var1 == cast var2
      go (RetrieveBlobs var1 vars1) (RetrieveBlobs var2 vars2) =
          Just var1 == cast var2 && Just vars1 == cast vars2
      go (Snapshot name1 var1)      (Snapshot name2 var2) =
          name1 == name2 && Just var1 == cast var2
      go (Open name1)               (Open name2) =
          name1 == name2
      go (Duplicate var1) (Duplicate var2) =
          Just var1 == cast var2
      go _  _ = False

      _coveredAllCases :: LockstepAction ModelState a -> ()
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
          Open{} -> ()
          Duplicate{} -> ()

{-------------------------------------------------------------------------------
  InLockstep
-------------------------------------------------------------------------------}

instance InLockstep ModelState where
  type instance ModelOp ModelState = Op

  data instance ModelValue ModelState a where
    MTableHandle :: Model.TableHandle k v blob -> Val (SUT.TableHandle IO k v blob)
    MBlobRef :: Model.BlobRef blob -> Val (SUT.BlobRef blob)

    MLookupResult :: Model.C k v blob
                  => Model.LookupResult k v (Val (SUT.BlobRef blob))
                  -> Val (SUT.LookupResult k v (SUT.BlobRef blob))
    MRangeLookupResult :: Model.C k v blob
                       => Model.RangeLookupResult k v (Val (SUT.BlobRef blob))
                       -> Val (SUT.RangeLookupResult k v (SUT.BlobRef blob))

    MBlob :: (Show blob, Typeable blob, Eq blob) => Wrap blob -> Val (Wrap blob)
    MErr :: Model.Err -> Val Model.Err

    MUnit   :: () -> Val ()
    MPair   :: (Val a, Val b) -> Val (a, b)
    MEither :: Either (Val a) (Val b) -> Val (Either a b)
    MList   :: [Val a] -> Val [a]

  data instance Observable ModelState a where
    OTableHandle :: Obs (SUT.TableHandle IO k v blob)
    OBlobRef :: Obs (SUT.BlobRef blob)

    -- TODO: can we use OId for lookup results and range lookup results instead,
    -- or are these separate constructors necessary?
    OLookupResult :: Model.C k v blob
                  => Model.LookupResult k v (Obs (SUT.BlobRef blob))
                  -> Obs (SUT.LookupResult k v (SUT.BlobRef blob))
    ORangeLookupResult :: Model.C k v blob
                       => Model.RangeLookupResult k v (Obs (SUT.BlobRef blob))
                       -> Obs (SUT.RangeLookupResult k v (SUT.BlobRef blob))

    OId :: (Show a, Typeable a, Eq a) => a -> Obs a

    OPair   :: (Obs a, Obs b) -> Obs (a, b)
    OEither :: Either (Obs a) (Obs b) -> Obs (Either a b)
    OList   :: [Obs a] -> Obs [a]

  observeModel :: Val a -> Obs a
  observeModel = \case
      MTableHandle _       -> OTableHandle
      MBlobRef _           -> OBlobRef
      MLookupResult x      -> OLookupResult $ fmap observeModel x
      MRangeLookupResult x -> ORangeLookupResult $ fmap observeModel x
      MBlob x              -> OId x
      MErr x               -> OId x
      MUnit x              -> OId x
      MPair x              -> OPair $ bimap observeModel observeModel x
      MEither x            -> OEither $ bimap observeModel observeModel x
      MList x              -> OList $ map observeModel x

  modelNextState ::  forall a.
       LockstepAction ModelState a
    -> ModelLookUp ModelState
    -> ModelState
    -> (ModelValue ModelState a, ModelState)
  modelNextState action lookUp (ModelState mock stats) =
      auxStats $ runModel lookUp action mock
    where
      auxStats :: (Val a, Model.Model) -> (Val a, ModelState)
      auxStats (result, state') =
          (result, ModelState state' $ updateStats action result stats)

  usedVars :: LockstepAction ModelState a -> [AnyGVar (ModelOp ModelState)]
  usedVars = \case
      New _                           -> []
      Close tableVar                  -> [SomeGVar tableVar]
      Lookups _ tableVar              -> [SomeGVar tableVar]
      RangeLookup _ tableVar          -> [SomeGVar tableVar]
      Updates _ tableVar              -> [SomeGVar tableVar]
      Inserts _ tableVar              -> [SomeGVar tableVar]
      Deletes _ tableVar              -> [SomeGVar tableVar]
      RetrieveBlobs tableVar blobsVar -> [SomeGVar tableVar, SomeGVar blobsVar]
      Snapshot _ tableVar             -> [SomeGVar tableVar]
      Open _                          -> []
      Duplicate tableVar              -> [SomeGVar tableVar]

  arbitraryWithVars ::
       ModelFindVariables ModelState
    -> ModelState
    -> Gen (Any (LockstepAction ModelState))
  arbitraryWithVars = arbitraryActionWithVars (Proxy @(Word64, Word64, Word64))

  shrinkWithVars ::
       ModelFindVariables ModelState
    -> ModelState
    -> LockstepAction ModelState a
    -> [Any (LockstepAction ModelState)]
  shrinkWithVars = shrinkActionWithVars

  tagStep ::
       (ModelState, ModelState)
    -> LockstepAction ModelState a
    -> Val a
    -> [String]
  tagStep states action = map show . tagStep' states action

deriving instance Show (Val a)
deriving instance Eq (Obs a)
deriving instance Show (Obs a)

{-------------------------------------------------------------------------------
  RunLockstep
-------------------------------------------------------------------------------}

instance  RunLockstep ModelState (RealMonad IO) where
  observeReal ::
       Proxy (RealMonad IO)
    -> LockstepAction ModelState a
    -> Realized (RealMonad IO) a
    -> Obs a
  observeReal _proxy action result = case action of
      New{}           -> OEither $ bimap OId (const OTableHandle) result
      Close{}         -> OEither $ bimap OId OId result
      Lookups{}       -> OEither $
          bimap OId (OList . fmap (OLookupResult . fmap (const OBlobRef))) result
      RangeLookup{}   -> OEither $
          bimap OId (OList . fmap (ORangeLookupResult . fmap (const OBlobRef))) result
      Updates{}       -> OEither $ bimap OId OId result
      Inserts{}       -> OEither $ bimap OId OId result
      Deletes{}       -> OEither $ bimap OId OId result
      RetrieveBlobs{} -> OEither $ bimap OId (OList . fmap OId) result
      Snapshot{}      -> OEither $ bimap OId OId result
      Open{}          -> OEither $ bimap OId (const OTableHandle) result
      Duplicate{}     -> OEither $ bimap OId (const OTableHandle) result

instance  RunLockstep ModelState (RealMonad (IOSim s)) where
  observeReal ::
       Proxy (RealMonad (IOSim s))
    -> LockstepAction ModelState a
    -> Realized (RealMonad (IOSim s)) a
    -> Obs a
  observeReal _proxy action result = case action of
      New{}           -> OEither $ bimap OId (const OTableHandle) result
      Close{}         -> OEither $ bimap OId OId result
      Lookups{}       -> OEither $
          bimap OId (OList . fmap (OLookupResult . fmap (const OBlobRef))) result
      RangeLookup{}   -> OEither $
          bimap OId (OList . fmap (ORangeLookupResult . fmap (const OBlobRef))) result
      Updates{}       -> OEither $ bimap OId OId result
      Inserts{}       -> OEither $ bimap OId OId result
      Deletes{}       -> OEither $ bimap OId OId result
      RetrieveBlobs{} -> OEither $ bimap OId (OList . fmap OId) result
      Snapshot{}      -> OEither $ bimap OId OId result
      Open{}          -> OEither $ bimap OId (const OTableHandle) result
      Duplicate{}     -> OEither $ bimap OId (const OTableHandle) result

{-------------------------------------------------------------------------------
  RunModel
-------------------------------------------------------------------------------}

instance RunModel (Lockstep ModelState) (RealMonad IO) where
  perform _     = runIO
  postcondition = Lockstep.Defaults.postcondition
  monitoring    = Lockstep.Defaults.monitoring (Proxy @(RealMonad IO))

instance RunModel (Lockstep ModelState) (RealMonad (IOSim s)) where
  perform _     = runIOSim
  postcondition = Lockstep.Defaults.postcondition
  monitoring    = Lockstep.Defaults.monitoring (Proxy @(RealMonad (IOSim s)))

{-------------------------------------------------------------------------------
  Interpreter for the model
-------------------------------------------------------------------------------}

runModel ::
     ModelLookUp ModelState
  -> LockstepAction ModelState a
  -> Model.Model -> (Val a, Model.Model)
runModel lookUp = \case
    New cfg -> wrap MTableHandle .
      Model.runModelM (Model.new cfg)
    Close tableVar -> wrap MUnit .
      Model.runModelM (Model.close (getTableHandle $ lookUp tableVar))
    Lookups ks tableVar -> wrap (MList . fmap (MLookupResult . fmap MBlobRef)) .
      Model.runModelM (Model.lookups ks (getTableHandle $ lookUp tableVar))
    RangeLookup range tableVar -> wrap (MList . fmap (MRangeLookupResult . fmap MBlobRef)) .
      Model.runModelM (Model.rangeLookup range (getTableHandle $ lookUp tableVar))
    Updates kups tableVar -> wrap MUnit .
      Model.runModelM (Model.updates kups (getTableHandle $ lookUp tableVar))
    Inserts kins tableVar -> wrap MUnit .
      Model.runModelM (Model.inserts kins (getTableHandle $ lookUp tableVar))
    Deletes kdels tableVar -> wrap MUnit .
      Model.runModelM (Model.deletes kdels (getTableHandle $ lookUp tableVar))
    RetrieveBlobs tableVar blobsVar -> wrap (MList . fmap (MBlob . Wrap)) .
      Model.runModelM (Model.retrieveBlobs (getTableHandle $ lookUp tableVar) (getBlobRefs . lookUp $ blobsVar))
    Snapshot name tableVar -> wrap MUnit .
      Model.runModelM (Model.snapshot name (getTableHandle $ lookUp tableVar))
    Open name -> wrap MTableHandle .
      Model.runModelM (Model.open name)
    Duplicate tableVar -> wrap MTableHandle .
      Model.runModelM (Model.duplicate (getTableHandle $ lookUp tableVar))
  where
    wrap ::
         (a -> Val b)
      -> (Either Model.Err a, Model.Model)
      -> (Val (Either Model.Err b), Model.Model)
    wrap f = first (MEither . bimap MErr f)

    getTableHandle ::
         ModelValue ModelState (SUT.TableHandle IO k v blob)
      -> Model.TableHandle k v blob
    getTableHandle (MTableHandle th) = th

    getBlobRefs :: ModelValue ModelState [SUT.BlobRef blob] -> [Model.BlobRef blob]
    getBlobRefs (MList brs) = fmap (\(MBlobRef br) -> br) brs


{-------------------------------------------------------------------------------
  Interpreters for @'IOLike' m@
-------------------------------------------------------------------------------}

runIO :: forall a.
     LockstepAction ModelState a
  -> LookUp (RealMonad IO)
  -> RealMonad IO (Realized (RealMonad IO) a)
runIO action lookUp = ReaderT $ \session -> aux session action
  where
    aux :: SUT.Session IO -> LockstepAction ModelState a -> IO (Realized IO a)
    aux session = \case
        New cfg -> catchErr $
          SUT.new session cfg
        Close tableVar -> catchErr $
          SUT.close (lookUp' tableVar)
        Lookups ks tableVar -> catchErr $
          SUT.lookups ks (lookUp' tableVar)
        RangeLookup range tableVar -> catchErr $
          SUT.rangeLookup range (lookUp' tableVar)
        Updates kups tableVar -> catchErr $
          SUT.updates kups (lookUp' tableVar)
        Inserts kins tableVar -> catchErr $
          SUT.inserts kins (lookUp' tableVar)
        Deletes kdels tableVar -> catchErr $
          SUT.deletes kdels (lookUp' tableVar)
        RetrieveBlobs tableVar blobRefsVar -> catchErr $
          fmap Wrap <$> SUT.retrieveBlobs (lookUp' tableVar) (lookUp' blobRefsVar)
        Snapshot name tableVar -> catchErr $
          SUT.snapshot name (lookUp' tableVar)
        Open name -> catchErr $
          SUT.open session name
        Duplicate tableVar -> catchErr $
          SUT.duplicate (lookUp' tableVar)

    lookUp' :: Var x -> Realized IO x
    lookUp' = lookUpGVar (Proxy @(RealMonad IO)) lookUp

runIOSim :: forall s a.
     LockstepAction ModelState a
  -> LookUp (RealMonad (IOSim s))
  -> RealMonad (IOSim s) (Realized (RealMonad (IOSim s)) a)
runIOSim action lookUp = ReaderT $ \session -> aux session action
  where
    aux :: SUT.Session (IOSim s) -> LockstepAction ModelState a -> IOSim s (Realized (IOSim s) a)
    aux session = \case
        New cfg -> catchErr $
          SUT.new session cfg
        Close tableVar -> catchErr $
          SUT.close (lookUp' tableVar)
        Lookups ks tableVar -> catchErr $
          SUT.lookups ks (lookUp' tableVar)
        RangeLookup range tableVar -> catchErr $
          SUT.rangeLookup range (lookUp' tableVar)
        Updates kups tableVar -> catchErr $
          SUT.updates kups (lookUp' tableVar)
        Inserts kins tableVar -> catchErr $
          SUT.inserts kins (lookUp' tableVar)
        Deletes kdels tableVar -> catchErr $
          SUT.deletes kdels (lookUp' tableVar)
        RetrieveBlobs tableVar blobRefsVar -> catchErr $
          fmap Wrap <$> SUT.retrieveBlobs (lookUp' tableVar) (lookUp' blobRefsVar)
        Snapshot name tableVar -> catchErr $
          SUT.snapshot name (lookUp' tableVar)
        Open name -> catchErr $
          SUT.open session name
        Duplicate tableVar -> catchErr $
          SUT.duplicate (lookUp' tableVar)

    lookUp' :: Var x -> Realized (IOSim s) x
    lookUp' = lookUpGVar (Proxy @(RealMonad (IOSim s))) lookUp

catchErr :: forall m a. MonadCatch m => m a -> m (Either Model.Err a)
catchErr action = catch (Right <$> action) handler
  where
    handler :: SomeException -> m (Either Model.Err b)
    handler (SomeException e) = maybe (throwIO e) (pure . Left) Nothing

{-------------------------------------------------------------------------------
  Generator and shrinking
-------------------------------------------------------------------------------}

arbitraryActionWithVars ::
     forall k v blob. (C k v blob, Arbitrary k, Arbitrary v, Arbitrary blob)
  => Proxy (k, v, blob)
  -> ModelFindVariables ModelState
  -> ModelState
  -> Gen (Any (LockstepAction ModelState))
arbitraryActionWithVars _ findVars _st = QC.oneof $ concat [
      withoutVars
    , case findVars (Proxy @(Either Model.Err (SUT.TableHandle IO k v blob))) of
        []   -> []
        vars -> withVars (QC.elements vars)
    , case ( findVars (Proxy @(Either Model.Err (SUT.TableHandle IO k v blob)))
           , findBlobRefsVars
           ) of
        ([], _ )      -> []
        (_ , [])      -> []
        (vars, vars') -> withVars' (QC.elements vars) (QC.elements vars')
    ]
  where
    findBlobRefsVars :: [Var (Either Model.Err [SUT.BlobRef blob])]
    findBlobRefsVars = fmap fromLookupResults vars1 ++ fmap fromRangeLookupResults vars2
      where
        vars1 = findVars (Proxy @(Either Model.Err [SUT.LookupResult k v (SUT.BlobRef blob)]))
        vars2 = findVars (Proxy @(Either Model.Err [SUT.RangeLookupResult k v (SUT.BlobRef blob)]))

        fromLookupResults ::
             Var (Either Model.Err [SUT.LookupResult k v (SUT.BlobRef blob)])
          -> Var (Either Model.Err [SUT.BlobRef blob])
        fromLookupResults = mapGVar (\op -> OpRight `OpComp` OpLookupResults `OpComp` OpFromRight `OpComp` op)

        fromRangeLookupResults ::
             Var (Either Model.Err [SUT.RangeLookupResult k v (SUT.BlobRef blob)])
          -> Var (Either Model.Err [SUT.BlobRef blob])
        fromRangeLookupResults = mapGVar (\op -> OpRight `OpComp` OpRangeLookupResults `OpComp` OpFromRight `OpComp` op)

    withoutVars :: [Gen (Any (LockstepAction ModelState))]
    withoutVars = [
          Some . New @k @v @blob <$> genTableConfig
        ]

    withVars ::
         Gen (Var (Either Model.Err (SUT.TableHandle IO k v blob)))
      -> [Gen (Any (LockstepAction ModelState))]
    withVars genVar = [
          fmap Some $ Close <$> (fromRight <$> genVar)
        , fmap Some $ Lookups <$> genLookupKeys <*> (fromRight <$> genVar)
        , fmap Some $ RangeLookup <$> genRange <*> (fromRight <$> genVar)
        , fmap Some $ Updates <$> genUpdates <*> (fromRight <$> genVar)
        , fmap Some $ Inserts <$> genInserts <*> (fromRight <$> genVar)
        , fmap Some $ Deletes <$> genDeletes <*> (fromRight <$> genVar)
        , fmap Some $ Snapshot <$> genSnapshotName <*> (fromRight <$> genVar)
        , fmap Some $ Open @k @v @blob <$> genSnapshotName
        , fmap Some $ Duplicate <$> (fromRight <$> genVar)
        ]

    withVars' ::
         Gen (Var (Either Model.Err (SUT.TableHandle IO k v blob)))
      -> Gen (Var (Either Model.Err [SUT.BlobRef blob]))
      -> [Gen (Any (LockstepAction ModelState))]
    withVars' genTableHandleVar genBlobRefsVar = [
          fmap Some $ RetrieveBlobs <$> (fromRight <$> genTableHandleVar) <*> (fromRight <$> genBlobRefsVar)
        ]

    fromRight ::
         Var (Either Model.Err a)
      -> Var a
    fromRight = mapGVar (\op -> OpFromRight `OpComp` op)

    -- TODO: improve, more types of configs
    genTableConfig :: Gen SUT.TableConfig
    genTableConfig = pure $ SUT.TableConfig {
          SUT.tcMaxBufferMemory = 2 * 1024 * 1024
        , SUT.tcMaxBloomFilterMemory = 2 * 1024 * 1024
        , SUT.tcBitPrecision = ()
        }

    -- TODO: improve
    genLookupKeys :: Gen [k]
    genLookupKeys = QC.arbitrary

    -- TODO: improve
    genRange :: Gen (SUT.Range k)
    genRange = QC.oneof [
          SUT.FromToExcluding <$> QC.arbitrary <*> QC.arbitrary
        , SUT.FromToIncluding <$> QC.arbitrary <*> QC.arbitrary
        ]
      where
        _coveredAllCases :: SUT.Range k -> ()
        _coveredAllCases = \case
            SUT.FromToExcluding{} -> ()
            SUT.FromToIncluding{} -> ()

    -- TODO: improve
    genUpdates :: Gen [(k, SUT.Update v blob)]
    genUpdates = QC.listOf $ (,) <$> QC.arbitrary <*> QC.oneof [
          SUT.Insert <$> QC.arbitrary <*> QC.arbitrary
        , pure SUT.Delete
        ]
      where
        _coveredAllCases :: SUT.Update v blob -> ()
        _coveredAllCases = \case
            SUT.Insert{} -> ()
            SUT.Delete{} -> ()

    -- TODO: improve
    genInserts :: Gen [(k, v, Maybe blob)]
    genInserts = QC.arbitrary

    -- TODO: improve
    genDeletes :: Gen [k]
    genDeletes = QC.arbitrary

    -- TODO: improve, actual snapshot names
    genSnapshotName :: Gen SUT.SnapshotName
    genSnapshotName = pure $ fromJust $ SUT.mkSnapshotName "snap"

shrinkActionWithVars ::
       ModelFindVariables ModelState
    -> ModelState
    -> LockstepAction ModelState a
    -> [Any (LockstepAction ModelState)]
shrinkActionWithVars _ _ _ = []

{-------------------------------------------------------------------------------
  Interpret 'Op' against 'ModelValue'
-------------------------------------------------------------------------------}

instance InterpretOp Op (ModelValue ModelState) where
  intOp = \case
    OpId                 -> Just
    OpFst                -> \case MPair   x -> Just (fst x)
    OpSnd                -> \case MPair   x -> Just (snd x)
    OpLeft               -> Just . MEither . Left
    OpRight              -> Just . MEither . Right
    OpFromLeft           -> \case MEither x -> either Just (const Nothing) x
    OpFromRight          -> \case MEither x -> either (const Nothing) Just x
    OpComp g f           -> intOp g <=< intOp f
    OpLookupResults      -> Just . MList
                          . mapMaybe (\case MLookupResult x -> getBlobRef x)
                          . \case MList x -> x
    OpRangeLookupResults -> Just . MList
                          . mapMaybe (\case MRangeLookupResult x -> getBlobRef x)
                          . \case MList x -> x

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
  | CDuplicate
  deriving (Show, Eq, Ord, Enum, Bounded)

toConstructorName :: LockstepAction ModelState a -> ConstructorName
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
    Duplicate{} -> CDuplicate

newtype Stats = Stats {
    seenActions :: Set ConstructorName
  }
  deriving Show

initStats :: Stats
initStats = Stats {
      seenActions = Set.empty
    }

updateStats :: LockstepAction ModelState a -> Val a -> Stats -> Stats
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
     (ModelState, ModelState)
  -> LockstepAction ModelState a
  -> Val a
  -> [Tag]
tagStep' (_before, ModelState _ statsAfter) _action _result = Top :
    [All | seenActions statsAfter == Set.fromList [minBound .. maxBound]]
