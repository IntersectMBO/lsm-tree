{-# LANGUAGE CPP                      #-}
{-# LANGUAGE ConstraintKinds          #-}
{-# LANGUAGE DataKinds                #-}
{-# LANGUAGE DerivingStrategies       #-}
{-# LANGUAGE EmptyDataDeriving        #-}
{-# LANGUAGE FlexibleContexts         #-}
{-# LANGUAGE FlexibleInstances        #-}
{-# LANGUAGE GADTs                    #-}
{-# LANGUAGE InstanceSigs             #-}
{-# LANGUAGE LambdaCase               #-}
{-# LANGUAGE MultiParamTypeClasses    #-}
{-# LANGUAGE NamedFieldPuns           #-}
{-# LANGUAGE OverloadedStrings        #-}
{-# LANGUAGE QuantifiedConstraints    #-}
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

{- HLINT ignore "Evaluate" -}
{- HLINT ignore "Use camelCase" -}
{- HLINT ignore "Redundant fmap" -}

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
  are retrieved from an incorrect table.
-}
module Test.Database.LSMTree.Normal.StateMachine (
    tests
  , labelledExamples
    -- * Properties
  , propLockstep_ModelIOImpl
  , propLockstep_RealImpl_RealFS_IO
  , propLockstep_RealImpl_MockFS_IO
  , propLockstep_RealImpl_MockFS_IOSim
    -- * Lockstep
  , ModelState (..)
  , Key (..)
  , Value (..)
  , Blob (..)
  , StateModel (..)
  , Action (..)
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Monad (forM_, void, (<=<))
import           Control.Monad.Class.MonadThrow (Handler (..), MonadCatch (..),
                     MonadThrow (..))
import           Control.Monad.IOSim
import           Control.Monad.Reader (ReaderT (..))
import           Control.RefCount (checkForgottenRefs)
import           Control.Tracer (Tracer, nullTracer)
import           Data.Bifunctor (Bifunctor (..))
import           Data.Constraint (Dict (..))
import           Data.Either (partitionEithers)
import           Data.Kind (Type)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes, fromJust, fromMaybe)
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Typeable (Proxy (..), Typeable, cast, eqT,
                     type (:~:) (Refl))
import qualified Data.Vector as V
import           Database.LSMTree.Class.Normal (LookupResult (..),
                     QueryResult (..))
import qualified Database.LSMTree.Class.Normal as Class
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact)
import           Database.LSMTree.Extras.NoThunks (assertNoThunks)
import           Database.LSMTree.Internal (LSMTreeError (..))
import qualified Database.LSMTree.Internal as R.Internal
import           Database.LSMTree.Internal.Serialise (SerialisedBlob,
                     SerialisedValue)
import qualified Database.LSMTree.Model.IO.Normal as ModelIO
import qualified Database.LSMTree.Model.Session as Model
import qualified Database.LSMTree.Normal as R
import           NoThunks.Class
import           Prelude hiding (init)
import           System.Directory (removeDirectoryRecursive)
import           System.FS.API (HasFS, MountPoint (..), mkFsPath)
import           System.FS.BlockIO.API (HasBlockIO, defaultIOCtxParams)
import           System.FS.BlockIO.IO (ioHasBlockIO)
import           System.FS.BlockIO.Sim (simHasBlockIO)
import           System.FS.IO (HandleIO, ioHasFS)
import qualified System.FS.Sim.MockFS as MockFS
import           System.FS.Sim.MockFS (MockFS)
import           System.IO.Temp (createTempDirectory,
                     getCanonicalTemporaryDirectory)
import           Test.Database.LSMTree.Normal.StateMachine.Op
                     (HasBlobRef (getBlobRef), Op (..))
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary, Gen, Property)
import qualified Test.QuickCheck.Extras as QD
import qualified Test.QuickCheck.Monadic as QC
import           Test.QuickCheck.Monadic (PropertyM)
import qualified Test.QuickCheck.StateModel as QD
import           Test.QuickCheck.StateModel hiding (Var)
import           Test.QuickCheck.StateModel.Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Defaults as Lockstep.Defaults
import qualified Test.QuickCheck.StateModel.Lockstep.Run as Lockstep.Run
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)
import           Test.Util.FS (assertNoOpenHandles, assertNumOpenHandles)
import           Test.Util.PrettyProxy
import           Test.Util.TypeFamilyWrappers (WrapBlob (..), WrapBlobRef (..),
                     WrapCursor (..), WrapSession (..), WrapTable (..))

{-------------------------------------------------------------------------------
  Test tree
-------------------------------------------------------------------------------}

tests :: TestTree
tests = testGroup "Normal.StateMachine" [
      testProperty "propLockstep_ModelIOImpl"
        propLockstep_ModelIOImpl

    , testProperty "propLockstep_RealImpl_RealFS_IO" $
        propLockstep_RealImpl_RealFS_IO nullTracer

    , testProperty "propLockstep_RealImpl_MockFS_IO" $
        propLockstep_RealImpl_MockFS_IO nullTracer

    , testProperty "propLockstep_RealImpl_MockFS_IOSim" $
        propLockstep_RealImpl_MockFS_IOSim nullTracer
    ]

labelledExamples :: IO ()
labelledExamples = QC.labelledExamples $ Lockstep.Run.tagActions (Proxy @(ModelState R.Table))

instance Arbitrary Model.TableConfig where
  arbitrary :: Gen Model.TableConfig
  arbitrary = pure Model.TableConfig

deriving via AllowThunk (ModelIO.Session IO)
    instance NoThunks (ModelIO.Session IO)

propLockstep_ModelIOImpl ::
     Actions (Lockstep (ModelState ModelIO.Table))
  -> QC.Property
propLockstep_ModelIOImpl =
    runActionsBracket'
      (Proxy @(ModelState ModelIO.Table))
      acquire
      release
      (\r session -> runReaderT r (session, handler))
      tagFinalState'
  where
    acquire :: IO (WrapSession ModelIO.Table IO)
    acquire = WrapSession <$> Class.openSession ModelIO.NoSessionArgs

    release :: WrapSession ModelIO.Table IO -> IO ()
    release (WrapSession session) = Class.closeSession session

    handler :: Handler IO (Maybe Model.Err)
    handler = Handler $ pure . handler'
      where
        handler' :: ModelIO.Err -> Maybe Model.Err
        handler' (ModelIO.Err err) = Just err

instance Arbitrary R.TableConfig where
  arbitrary = do
    confMergeSchedule <- QC.frequency [
          (1, pure R.OneShot)
        , (4, pure R.Incremental)
        ]
    confWriteBufferAlloc <- QC.arbitrary
    pure $ R.TableConfig {
        R.confMergePolicy       = R.MergePolicyLazyLevelling
      , R.confSizeRatio         = R.Four
      , confWriteBufferAlloc
      , R.confBloomFilterAlloc  = R.AllocFixed 10
      , R.confFencePointerIndex = R.CompactIndex
      , R.confDiskCachePolicy   = R.DiskCacheNone
      , confMergeSchedule
      }

  shrink R.TableConfig{..} =
      [ R.TableConfig {
            confWriteBufferAlloc = confWriteBufferAlloc'
          , ..
          }
      | confWriteBufferAlloc' <- QC.shrink confWriteBufferAlloc
      ]

-- TODO: the current generator is suboptimal, and should be improved. There are
-- some aspects to consider, and since they are at tension with each other, we
-- should try find a good balance.
--
-- Say the write buffer allocation is @n@.
--
-- * If @n@ is too large, then the tests degrade to only testing the write
--   buffer, because there are no flushes/merges.
--
-- * If @n@ is too small, then the resulting runs are also small. They might
--   consist of only a few disk pages, or they might consist of only 1 underfull
--   disk page. This means there are some parts of the code that we would rarely
--   cover in the state machine tests.
--
-- * If @n@ is too small, then we flush/merge very frequently, which may exhaust
--   the number of open file handles when using the real file system. Specially
--   if the test fails and the shrinker kicks in, then @n@ can currently become
--   very small. This is good for finding a minimal counterexample, but it might
--   also make the real filesystem run out of file descriptors. Arguably, you
--   could overcome this specific issue by only generating or shrinking to small
--   @n@ when we use the mocked file system. This would require some boilerplate
--   to add type level tags to distinguish between the two cases.
instance Arbitrary R.WriteBufferAlloc where
  arbitrary = QC.scale (max 30) $ do
      QC.Positive x <- QC.arbitrary
      pure (R.AllocNumEntries (R.NumEntries x))

  shrink (R.AllocNumEntries (R.NumEntries x)) =
      [ R.AllocNumEntries (R.NumEntries x')
      | QC.Positive x' <- QC.shrink (QC.Positive x)
      ]

propLockstep_RealImpl_RealFS_IO ::
     Tracer IO R.LSMTreeTrace
  -> Actions (Lockstep (ModelState R.Table))
  -> QC.Property
propLockstep_RealImpl_RealFS_IO tr =
    runActionsBracket'
      (Proxy @(ModelState R.Table))
      acquire
      release
      (\r (_, session) -> runReaderT r (session, realHandler @IO))
      tagFinalState'
  where
    acquire :: IO (FilePath, WrapSession R.Table IO)
    acquire = do
        (tmpDir, hasFS, hasBlockIO) <- createSystemTempDirectory "prop_lockstepIO_RealImpl_RealFS"
        session <- R.openSession tr hasFS hasBlockIO (mkFsPath [])
        pure (tmpDir, WrapSession session)

    release :: (FilePath, WrapSession R.Table IO) -> IO ()
    release (tmpDir, WrapSession session) = do
        R.closeSession session
        removeDirectoryRecursive tmpDir

propLockstep_RealImpl_MockFS_IO ::
     Tracer IO R.LSMTreeTrace
  -> Actions (Lockstep (ModelState R.Table))
  -> QC.Property
propLockstep_RealImpl_MockFS_IO tr =
    runActionsBracket'
      (Proxy @(ModelState R.Table))
      (acquire_RealImpl_MockFS tr)
      release_RealImpl_MockFS
      (\r (_, session) -> runReaderT r (session, realHandler @IO))
      tagFinalState'

propLockstep_RealImpl_MockFS_IOSim ::
     (forall s. Tracer (IOSim s) R.LSMTreeTrace)
  -> Actions (Lockstep (ModelState R.Table))
  -> QC.Property
propLockstep_RealImpl_MockFS_IOSim tr actions =
    monadicIOSim_ prop
  where
    prop :: forall s. PropertyM (IOSim s) Property
    prop = do
        (fsVar, session) <- QC.run (acquire_RealImpl_MockFS tr)
        void $ QD.runPropertyReaderT
                (QD.runActions @(Lockstep (ModelState R.Table)) actions)
                (session, realHandler @(IOSim s))
        QC.run $ release_RealImpl_MockFS (fsVar, session)
        pure $ tagFinalState actions tagFinalState' $ QC.property True

acquire_RealImpl_MockFS ::
     R.IOLike m
  => Tracer m R.LSMTreeTrace
  -> m (StrictTMVar m MockFS, WrapSession R.Table m)
acquire_RealImpl_MockFS tr = do
    fsVar <- newTMVarIO MockFS.empty
    (hfs, hbio) <- simHasBlockIO fsVar
    session <- R.openSession tr hfs hbio (mkFsPath [])
    pure (fsVar, WrapSession session)

release_RealImpl_MockFS ::
     R.IOLike m
  => (StrictTMVar m MockFS, WrapSession R.Table m)
  -> m ()
release_RealImpl_MockFS (fsVar, WrapSession session) = do
    sts <- getAllSessionTables session
    forM_ sts $ \(SomeTable t) -> R.close t
    scs <- getAllSessionCursors session
    forM_ scs $ \(SomeCursor c) -> R.closeCursor c
    mockfs1 <- atomically $ readTMVar fsVar
    assertNumOpenHandles mockfs1 1 $ pure ()
    R.closeSession session
    mockfs2 <- atomically $ readTMVar fsVar
    assertNoOpenHandles mockfs2 $ pure ()

data SomeTable m = SomeTable (forall k v b. R.Table m k v b)
data SomeCursor m = SomeCursor (forall k v b. R.Cursor m k v b)

getAllSessionTables ::
     (MonadSTM m, MonadThrow m, MonadMVar m)
  => R.Session m
  -> m [SomeTable m]
getAllSessionTables (R.Internal.Session' s) = do
    R.Internal.withOpenSession s $ \seshEnv -> do
      ts <- readMVar (R.Internal.sessionOpenTables seshEnv)
      pure ((\x -> SomeTable (R.Internal.NormalTable x))  <$> Map.elems ts)

getAllSessionCursors ::
     (MonadSTM m, MonadThrow m, MonadMVar m)
  => R.Session m
  -> m [SomeCursor m]
getAllSessionCursors (R.Internal.Session' s) =
    R.Internal.withOpenSession s $ \seshEnv -> do
      cs <- readMVar (R.Internal.sessionOpenCursors seshEnv)
      pure ((\x -> SomeCursor (R.Internal.NormalCursor x))  <$> Map.elems cs)

realHandler :: Monad m => Handler m (Maybe Model.Err)
realHandler = Handler $ pure . handler'
  where
    handler' :: LSMTreeError -> Maybe Model.Err
    handler' ErrTableClosed               = Just Model.ErrTableClosed
    handler' ErrCursorClosed              = Just Model.ErrCursorClosed
    handler' (ErrSnapshotNotExists _snap) = Just Model.ErrSnapshotDoesNotExist
    handler' (ErrSnapshotExists _snap)    = Just Model.ErrSnapshotExists
    handler' ErrSnapshotWrongTableType{}  = Just Model.ErrSnapshotWrongType
    handler' (ErrBlobRefInvalid _)        = Just Model.ErrBlobRefInvalidated
    handler' _                            = Nothing

createSystemTempDirectory ::  [Char] -> IO (FilePath, HasFS IO HandleIO, HasBlockIO IO HandleIO)
createSystemTempDirectory prefix = do
    systemTempDir <- getCanonicalTemporaryDirectory
    tempDir <- createTempDirectory systemTempDir prefix
    let hasFS = ioHasFS (MountPoint tempDir)
    hasBlockIO <- ioHasBlockIO hasFS defaultIOCtxParams
    pure (tempDir, hasFS, hasBlockIO)

{-------------------------------------------------------------------------------
  Key and value types
-------------------------------------------------------------------------------}

newtype Key = Key KeyForIndexCompact
  deriving stock (Show, Eq, Ord)
  deriving newtype (Arbitrary, R.SerialiseKey)

newtype Value = Value SerialisedValue
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary, R.SerialiseValue)

newtype Blob = Blob SerialisedBlob
  deriving stock (Show, Eq)
  deriving newtype (Arbitrary, R.SerialiseValue)

instance R.Labellable (Key, Value, Blob) where
  makeSnapshotLabel _ = "Key Value Blob"

{-------------------------------------------------------------------------------
  Model state
-------------------------------------------------------------------------------}

type ModelState :: ((Type -> Type) -> Type -> Type -> Type -> Type) -> Type
data ModelState h = ModelState Model.Model Stats
  deriving stock Show

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
    Class.C_ a
  , R.SerialiseKey a
  , Arbitrary a
  )

type V a = (
    Class.C_ a
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
        => {-# UNPACK #-} !(PrettyProxy (k, v, blob))
        -> Class.TableConfig h
        -> Act h (WrapTable h IO k v blob)
    Close :: C k v blob
          => Var h (WrapTable h IO k v blob)
          -> Act h ()
    -- Queries
    Lookups :: C k v blob
            => V.Vector k -> Var h (WrapTable h IO k v blob)
            -> Act h (V.Vector (LookupResult v (WrapBlobRef h IO blob)))
    RangeLookup :: (C k v blob, Ord k)
                => R.Range k -> Var h (WrapTable h IO k v blob)
                -> Act h (V.Vector (QueryResult k v (WrapBlobRef h IO blob)))
    -- Cursor
    NewCursor :: C k v blob
              => Maybe k
              -> Var h (WrapTable h IO k v blob)
              -> Act h (WrapCursor h IO k v blob)
    CloseCursor :: C k v blob
                => Var h (WrapCursor h IO k v blob)
                -> Act h ()
    ReadCursor :: C k v blob
               => Int
               -> Var h (WrapCursor h IO k v blob)
               -> Act h (V.Vector (QueryResult k v (WrapBlobRef h IO blob)))
    -- Updates
    Updates :: C k v blob
            => V.Vector (k, R.Update v blob) -> Var h (WrapTable h IO k v blob)
            -> Act h ()
    Inserts :: C k v blob
            => V.Vector (k, v, Maybe blob) -> Var h (WrapTable h IO k v blob)
            -> Act h ()
    Deletes :: C k v blob
            => V.Vector k -> Var h (WrapTable h IO k v blob)
            -> Act h ()
    -- Blobs
    RetrieveBlobs :: V blob
                  => Var h (V.Vector (WrapBlobRef h IO blob))
                  -> Act h (V.Vector (WrapBlob blob))
    -- Snapshots
    CreateSnapshot :: (C k v blob, R.Labellable (k, v, blob))
                   => R.SnapshotName -> Var h (WrapTable h IO k v blob)
                   -> Act h ()
    OpenSnapshot   :: (C k v blob, R.Labellable (k, v, blob))
                   => R.SnapshotName
                   -> Act h (WrapTable h IO k v blob)
    DeleteSnapshot :: R.SnapshotName -> Act h ()
    ListSnapshots  :: Act h [R.SnapshotName]
    -- Multiple writable tables
    Duplicate :: C k v blob
              => Var h (WrapTable h IO k v blob)
              -> Act h (WrapTable h IO k v blob)

  initialState    = Lockstep.Defaults.initialState initModelState
  nextState       = Lockstep.Defaults.nextState
  precondition    = Lockstep.Defaults.precondition
  arbitraryAction = Lockstep.Defaults.arbitraryAction
  shrinkAction    = Lockstep.Defaults.shrinkAction

-- TODO: show instance does not show key-value-blob types. Example:
--
-- Normal.StateMachine
--   prop_lockstepIO_ModelIOImpl: FAIL
--     *** Failed! Exception: 'open: inappropriate type (table type mismatch)' (after 25 tests and 2 shrinks):
--     do action $ New TableConfig
--        action $ CreateSnapshot "snap" (GVar var1 (FromRight . id))
--        action $ OpenSnapshot "snap"
--        pure ()
deriving stock instance Show (Class.TableConfig h)
                     => Show (LockstepAction (ModelState h) a)

instance ( Eq (Class.TableConfig h)
         , Typeable h
         ) => Eq (LockstepAction (ModelState h) a) where
  (==) :: LockstepAction (ModelState h) a -> LockstepAction (ModelState h) a -> Bool
  x == y = go x y
    where
      go :: LockstepAction (ModelState h) a -> LockstepAction (ModelState h) a -> Bool
      go (New (PrettyProxy :: PrettyProxy kvb1) conf1) (New (PrettyProxy :: PrettyProxy kvb2) conf2) =
          eqT @kvb1 @kvb2 == Just Refl && conf1 == conf2
      go (Close var1)               (Close var2) =
          Just var1 == cast var2
      go (Lookups ks1 var1)         (Lookups ks2 var2) =
          Just ks1 == cast ks2 && Just var1 == cast var2
      go (RangeLookup range1 var1)  (RangeLookup range2 var2) =
          range1 == range2 && var1 == var2
      go (NewCursor k1 var1) (NewCursor k2 var2) =
          k1 == k2 && var1 == var2
      go (CloseCursor var1) (CloseCursor var2) =
          Just var1 == cast var2
      go (ReadCursor n1 var1) (ReadCursor n2 var2) =
          n1 == n2 && var1 == var2
      go (Updates ups1 var1)        (Updates ups2 var2) =
          Just ups1 == cast ups2 && Just var1 == cast var2
      go (Inserts inss1 var1)       (Inserts inss2 var2) =
          Just inss1 == cast inss2 && Just var1 == cast var2
      go (Deletes ks1 var1)         (Deletes ks2 var2) =
          Just ks1 == cast ks2 && Just var1 == cast var2
      go (RetrieveBlobs vars1) (RetrieveBlobs vars2) =
          Just vars1 == cast vars2
      go (CreateSnapshot name1 var1) (CreateSnapshot name2 var2) =
          name1 == name2 && Just var1 == cast var2
      go (OpenSnapshot name1) (OpenSnapshot name2) =
          name1 == name2
      go (DeleteSnapshot name1) (DeleteSnapshot name2) =
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
          NewCursor{} -> ()
          CloseCursor{} -> ()
          ReadCursor{} -> ()
          Updates{} -> ()
          Inserts{} -> ()
          Deletes{} -> ()
          RetrieveBlobs{} -> ()
          CreateSnapshot{} -> ()
          OpenSnapshot{} -> ()
          DeleteSnapshot{} -> ()
          ListSnapshots{} -> ()
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
    MTable :: Model.Table k v blob
                 -> Val h (WrapTable h IO k v blob)
    MCursor :: Model.Cursor k v blob -> Val h (WrapCursor h IO k v blob)
    MBlobRef :: Class.C_ blob
             => Model.BlobRef blob -> Val h (WrapBlobRef h IO blob)

    MLookupResult :: (Class.C_ v, Class.C_ blob)
                  => LookupResult v (Val h (WrapBlobRef h IO blob))
                  -> Val h (LookupResult v (WrapBlobRef h IO blob))
    MQueryResult :: Class.C k v blob
                 => QueryResult k v (Val h (WrapBlobRef h IO blob))
                 -> Val h (QueryResult k v (WrapBlobRef h IO blob))

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
    OTable :: Obs h (WrapTable h IO k v blob)
    OCursor :: Obs h (WrapCursor h IO k v blob)
    OBlobRef :: Obs h (WrapBlobRef h IO blob)

    OLookupResult :: (Class.C_ v, Class.C_ blob)
                  => LookupResult v (Obs h (WrapBlobRef h IO blob))
                  -> Obs h (LookupResult v (WrapBlobRef h IO blob))
    OQueryResult :: Class.C k v blob
                 => QueryResult k v (Obs h (WrapBlobRef h IO blob))
                 -> Obs h (QueryResult k v (WrapBlobRef h IO blob))
    OBlob :: (Show blob, Typeable blob, Eq blob)
          => WrapBlob blob -> Obs h (WrapBlob blob)

    OId :: (Show a, Typeable a, Eq a) => a -> Obs h a

    OPair   :: (Obs h a, Obs h b) -> Obs h (a, b)
    OEither :: Either (Obs h a) (Obs h b) -> Obs h (Either a b)
    OList   :: [Obs h a] -> Obs h [a]
    OVector :: V.Vector (Obs h a) -> Obs h (V.Vector a)

  observeModel :: Val h a -> Obs h a
  observeModel = \case
      MTable _       -> OTable
      MCursor _            -> OCursor
      MBlobRef _           -> OBlobRef
      MLookupResult x      -> OLookupResult $ fmap observeModel x
      MQueryResult x       -> OQueryResult $ fmap observeModel x
      MSnapshotName x      -> OId x
      MBlob x              -> OBlob x
      MErr x               -> OId x
      MUnit x              -> OId x
      MPair x              -> OPair $ bimap observeModel observeModel x
      MEither x            -> OEither $ bimap observeModel observeModel x
      MList x              -> OList $ map observeModel x
      MVector x            -> OVector $ V.map observeModel x

  modelNextState ::  forall a.
       LockstepAction (ModelState h) a
    -> ModelVarContext (ModelState h)
    -> ModelState h
    -> (ModelValue (ModelState h) a, ModelState h)
  modelNextState action ctx (ModelState state stats) =
      auxStats $ runModel (lookupVar ctx) action state
    where
      auxStats :: (Val h a, Model.Model) -> (Val h a, ModelState h)
      auxStats (result, state') = (result, ModelState state' stats')
        where
          stats' = updateStats action (lookupVar ctx) state state' result stats

  usedVars :: LockstepAction (ModelState h) a -> [AnyGVar (ModelOp (ModelState h))]
  usedVars = \case
      New _ _                         -> []
      Close tableVar                  -> [SomeGVar tableVar]
      Lookups _ tableVar              -> [SomeGVar tableVar]
      RangeLookup _ tableVar          -> [SomeGVar tableVar]
      NewCursor _ tableVar            -> [SomeGVar tableVar]
      CloseCursor cursorVar           -> [SomeGVar cursorVar]
      ReadCursor _ cursorVar          -> [SomeGVar cursorVar]
      Updates _ tableVar              -> [SomeGVar tableVar]
      Inserts _ tableVar              -> [SomeGVar tableVar]
      Deletes _ tableVar              -> [SomeGVar tableVar]
      RetrieveBlobs blobsVar          -> [SomeGVar blobsVar]
      CreateSnapshot _ tableVar       -> [SomeGVar tableVar]
      OpenSnapshot _                  -> []
      DeleteSnapshot _                -> []
      ListSnapshots                   -> []
      Duplicate tableVar              -> [SomeGVar tableVar]

  arbitraryWithVars ::
       ModelVarContext (ModelState h)
    -> ModelState h
    -> Gen (Any (LockstepAction (ModelState h)))
  arbitraryWithVars ctx st =
    QC.scale (max 100) $
    arbitraryActionWithVars (Proxy @(Key, Value, Blob)) ctx st

  shrinkWithVars ::
       ModelVarContext (ModelState h)
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

deriving stock instance Show (Class.TableConfig h) => Show (Val h a)
deriving stock instance Show (Obs h a)

instance Eq (Obs h a) where
  obsReal == obsModel = case (obsReal, obsModel) of
      -- The model is conservative about blob retrieval: the model invalidates a
      -- blob reference immediately after an update to the table, and if the SUT
      -- returns a blob, then that's okay. If both return a blob or both return
      -- an error, then those must match exactly.
      (OEither (Right (OVector vec)), OEither (Left (OId y)))
        | Just (OBlob (WrapBlob _), _) <- V.uncons vec
        , Just Model.ErrBlobRefInvalidated <- cast y -> True
      -- default equalities
      (OTable, OTable) -> True
      (OCursor, OCursor) -> True
      (OBlobRef, OBlobRef) -> True
      (OLookupResult x, OLookupResult y) -> x == y
      (OQueryResult x, OQueryResult y) -> x == y
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
          OTable{} -> ()
          OCursor{} -> ()
          OBlobRef{} -> ()
          OLookupResult{} -> ()
          OQueryResult{} -> ()
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
-- identified by the table @h@.
type RealMonad h m = ReaderT (WrapSession h m, Handler m (Maybe Model.Err)) m

{-------------------------------------------------------------------------------
  RunLockstep
-------------------------------------------------------------------------------}

instance ( Eq (Class.TableConfig h)
         , Class.IsTable h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         , NoThunks (Class.Session h IO)
         ) => RunLockstep (ModelState h) (RealMonad h IO) where
  observeReal ::
       Proxy (RealMonad h IO)
    -> LockstepAction (ModelState h) a
    -> Realized (RealMonad h IO) a
    -> Obs h a
  observeReal _proxy action result = case action of
      New{}            -> OEither $ bimap OId (const OTable) result
      Close{}          -> OEither $ bimap OId OId result
      Lookups{}        -> OEither $
          bimap OId (OVector . fmap (OLookupResult . fmap (const OBlobRef))) result
      RangeLookup{}    -> OEither $
          bimap OId (OVector . fmap (OQueryResult . fmap (const OBlobRef))) result
      NewCursor{}      -> OEither $ bimap OId (const OCursor) result
      CloseCursor{}    -> OEither $ bimap OId OId result
      ReadCursor{}     -> OEither $
          bimap OId (OVector . fmap (OQueryResult . fmap (const OBlobRef))) result
      Updates{}        -> OEither $ bimap OId OId result
      Inserts{}        -> OEither $ bimap OId OId result
      Deletes{}        -> OEither $ bimap OId OId result
      RetrieveBlobs{}  -> OEither $ bimap OId (OVector . fmap OBlob) result
      CreateSnapshot{} -> OEither $ bimap OId OId result
      OpenSnapshot{}   -> OEither $ bimap OId (const OTable) result
      DeleteSnapshot{} -> OEither $ bimap OId OId result
      ListSnapshots{}  -> OEither $ bimap OId (OList . fmap OId) result
      Duplicate{}      -> OEither $ bimap OId (const OTable) result

  showRealResponse ::
       Proxy (RealMonad h IO)
    -> LockstepAction (ModelState h) a
    -> Maybe (Dict (Show (Realized (RealMonad h IO) a)))
  showRealResponse _ = \case
      New{}            -> Nothing
      Close{}          -> Just Dict
      Lookups{}        -> Nothing
      RangeLookup{}    -> Nothing
      NewCursor{}      -> Nothing
      CloseCursor{}    -> Just Dict
      ReadCursor{}     -> Nothing
      Updates{}        -> Just Dict
      Inserts{}        -> Just Dict
      Deletes{}        -> Just Dict
      RetrieveBlobs{}  -> Just Dict
      CreateSnapshot{} -> Just Dict
      OpenSnapshot{}   -> Nothing
      DeleteSnapshot{} -> Just Dict
      ListSnapshots    -> Just Dict
      Duplicate{}      -> Nothing

instance ( Eq (Class.TableConfig h)
         , Class.IsTable h
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
      New{}            -> OEither $ bimap OId (const OTable) result
      Close{}          -> OEither $ bimap OId OId result
      Lookups{}        -> OEither $
          bimap OId (OVector . fmap (OLookupResult . fmap (const OBlobRef))) result
      RangeLookup{}    -> OEither $
          bimap OId (OVector . fmap (OQueryResult . fmap (const OBlobRef))) result
      NewCursor{}      -> OEither $ bimap OId (const OCursor) result
      CloseCursor{}    -> OEither $ bimap OId OId result
      ReadCursor{}     -> OEither $
          bimap OId (OVector . fmap (OQueryResult . fmap (const OBlobRef))) result
      Updates{}        -> OEither $ bimap OId OId result
      Inserts{}        -> OEither $ bimap OId OId result
      Deletes{}        -> OEither $ bimap OId OId result
      RetrieveBlobs{}  -> OEither $ bimap OId (OVector . fmap OBlob) result
      CreateSnapshot{} -> OEither $ bimap OId OId result
      OpenSnapshot{}   -> OEither $ bimap OId (const OTable) result
      DeleteSnapshot{} -> OEither $ bimap OId OId result
      ListSnapshots{}  -> OEither $ bimap OId (OList . fmap OId) result
      Duplicate{}      -> OEither $ bimap OId (const OTable) result

  showRealResponse ::
       Proxy (RealMonad h (IOSim s))
    -> LockstepAction (ModelState h) a
    -> Maybe (Dict (Show (Realized (RealMonad h (IOSim s)) a)))
  showRealResponse _ = \case
      New{}            -> Nothing
      Close{}          -> Just Dict
      Lookups{}        -> Nothing
      RangeLookup{}    -> Nothing
      NewCursor{}      -> Nothing
      CloseCursor{}    -> Just Dict
      ReadCursor{}     -> Nothing
      Updates{}        -> Just Dict
      Inserts{}        -> Just Dict
      Deletes{}        -> Just Dict
      RetrieveBlobs{}  -> Just Dict
      CreateSnapshot{} -> Just Dict
      OpenSnapshot{}   -> Nothing
      DeleteSnapshot{} -> Just Dict
      ListSnapshots    -> Just Dict
      Duplicate{}      -> Nothing

{-------------------------------------------------------------------------------
  RunModel
-------------------------------------------------------------------------------}

instance ( Eq (Class.TableConfig h)
         , Class.IsTable h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         , NoThunks (Class.Session h IO)
         ) => RunModel (Lockstep (ModelState h)) (RealMonad h IO) where
  perform _     = runIO
  postcondition = Lockstep.Defaults.postcondition
  monitoring    = Lockstep.Defaults.monitoring (Proxy @(RealMonad h IO))

instance ( Eq (Class.TableConfig h)
         , Class.IsTable h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => RunModel (Lockstep (ModelState h)) (RealMonad h (IOSim s)) where
  perform _     = runIOSim
  postcondition = Lockstep.Defaults.postcondition
  monitoring    = Lockstep.Defaults.monitoring (Proxy @(RealMonad h (IOSim s)))

{-------------------------------------------------------------------------------
  Interpreter for the model
-------------------------------------------------------------------------------}

runModel ::
     ModelLookUp (ModelState h)
  -> LockstepAction (ModelState h) a
  -> Model.Model -> (Val h a, Model.Model)
runModel lookUp = \case
    New _ _cfg ->
      wrap MTable
      . Model.runModelM (Model.new Model.TableConfig)
    Close tableVar ->
      wrap MUnit
      . Model.runModelM (Model.close (getTable $ lookUp tableVar))
    Lookups ks tableVar ->
      wrap (MVector . fmap (MLookupResult . fmap MBlobRef . ModelIO.convLookupResult))
      . Model.runModelM (Model.lookups ks (getTable $ lookUp tableVar))
    RangeLookup range tableVar ->
      wrap (MVector . fmap (MQueryResult . fmap MBlobRef . ModelIO.convQueryResult))
      . Model.runModelM (Model.rangeLookup range (getTable $ lookUp tableVar))
    NewCursor offset tableVar ->
      wrap MCursor
      . Model.runModelM (Model.newCursor offset (getTable $ lookUp tableVar))
    CloseCursor cursorVar ->
      wrap MUnit
      . Model.runModelM (Model.closeCursor (getCursor $ lookUp cursorVar))
    ReadCursor n cursorVar ->
      wrap (MVector . fmap (MQueryResult . fmap MBlobRef . ModelIO.convQueryResult))
      . Model.runModelM (Model.readCursor n (getCursor $ lookUp cursorVar))
    Updates kups tableVar ->
      wrap MUnit
      . Model.runModelM (Model.updates Model.noResolve (fmap ModelIO.convUpdate <$> kups) (getTable $ lookUp tableVar))
    Inserts kins tableVar ->
      wrap MUnit
      . Model.runModelM (Model.inserts Model.noResolve kins (getTable $ lookUp tableVar))
    Deletes kdels tableVar ->
      wrap MUnit
      . Model.runModelM (Model.deletes Model.noResolve kdels (getTable $ lookUp tableVar))
    RetrieveBlobs blobsVar ->
      wrap (MVector . fmap (MBlob . WrapBlob))
      . Model.runModelM (Model.retrieveBlobs (getBlobRefs . lookUp $ blobsVar))
    CreateSnapshot name tableVar ->
      wrap MUnit
      . Model.runModelM (Model.createSnapshot name (getTable $ lookUp tableVar))
    OpenSnapshot name ->
      wrap MTable
      . Model.runModelM (Model.openSnapshot name)
    DeleteSnapshot name ->
      wrap MUnit
      . Model.runModelM (Model.deleteSnapshot name)
    ListSnapshots ->
      wrap (MList . fmap MSnapshotName)
      . Model.runModelM Model.listSnapshots
    Duplicate tableVar ->
      wrap MTable
      . Model.runModelM (Model.duplicate (getTable $ lookUp tableVar))
  where
    getTable ::
         ModelValue (ModelState h) (WrapTable h IO k v blob)
      -> Model.Table k v blob
    getTable (MTable t) = t

    getCursor ::
         ModelValue (ModelState h) (WrapCursor h IO k v blob)
      -> Model.Cursor k v blob
    getCursor (MCursor t) = t

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
     forall a h. (Class.IsTable h, NoThunks (Class.Session h IO))
  => LockstepAction (ModelState h) a
  -> LookUp (RealMonad h IO)
  -> RealMonad h IO (Realized (RealMonad h IO) a)
runIO action lookUp = ReaderT $ \(session, handler) -> do
    x <- aux (unwrapSession session) handler action
    case session of
      WrapSession sesh ->
        assertNoThunks sesh $ pure ()
    pure x
  where
    aux ::
         Class.Session h IO
      -> Handler IO (Maybe Model.Err)
      -> LockstepAction (ModelState h) a
      -> IO (Realized IO a)
    aux session handler = \case
        New _ cfg -> catchErr handler $
          WrapTable <$> Class.new session cfg
        Close tableVar -> catchErr handler $
          Class.close (unwrapTable $ lookUp' tableVar)
        Lookups ks tableVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.lookups (unwrapTable $ lookUp' tableVar) ks
        RangeLookup range tableVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.rangeLookup (unwrapTable $ lookUp' tableVar) range
        NewCursor offset tableVar -> catchErr handler $
          WrapCursor <$> Class.newCursor offset (unwrapTable $ lookUp' tableVar)
        CloseCursor cursorVar -> catchErr handler $
          Class.closeCursor (Proxy @h) (unwrapCursor $ lookUp' cursorVar)
        ReadCursor n cursorVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.readCursor (Proxy @h) n (unwrapCursor $ lookUp' cursorVar)
        Updates kups tableVar -> catchErr handler $
          Class.updates (unwrapTable $ lookUp' tableVar) kups
        Inserts kins tableVar -> catchErr handler $
          Class.inserts (unwrapTable $ lookUp' tableVar) kins
        Deletes kdels tableVar -> catchErr handler $
          Class.deletes (unwrapTable $ lookUp' tableVar) kdels
        RetrieveBlobs blobRefsVar -> catchErr handler $
          fmap WrapBlob <$> Class.retrieveBlobs (Proxy @h) session (unwrapBlobRef <$> lookUp' blobRefsVar)
        CreateSnapshot name tableVar -> catchErr handler $
          Class.createSnapshot name (unwrapTable $ lookUp' tableVar)
        OpenSnapshot name -> catchErr handler $
          WrapTable <$> Class.openSnapshot session name
        DeleteSnapshot name -> catchErr handler $
          Class.deleteSnapshot session name
        ListSnapshots -> catchErr handler $
          Class.listSnapshots session
        Duplicate tableVar -> catchErr handler $
          WrapTable <$> Class.duplicate (unwrapTable $ lookUp' tableVar)

    lookUp' :: Var h x -> Realized IO x
    lookUp' = lookUpGVar (Proxy @(RealMonad h IO)) lookUp

runIOSim ::
     forall s a h. Class.IsTable h
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
        New _ cfg -> catchErr handler $
          WrapTable <$> Class.new session cfg
        Close tableVar -> catchErr handler $
          Class.close (unwrapTable $ lookUp' tableVar)
        Lookups ks tableVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.lookups (unwrapTable $ lookUp' tableVar) ks
        RangeLookup range tableVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.rangeLookup (unwrapTable $ lookUp' tableVar) range
        NewCursor offset tableVar -> catchErr handler $
          WrapCursor <$> Class.newCursor offset (unwrapTable $ lookUp' tableVar)
        CloseCursor cursorVar -> catchErr handler $
          Class.closeCursor (Proxy @h) (unwrapCursor $ lookUp' cursorVar)
        ReadCursor n cursorVar -> catchErr handler $
          fmap (fmap WrapBlobRef) <$> Class.readCursor (Proxy @h) n (unwrapCursor $ lookUp' cursorVar)
        Updates kups tableVar -> catchErr handler $
          Class.updates (unwrapTable $ lookUp' tableVar) kups
        Inserts kins tableVar -> catchErr handler $
          Class.inserts (unwrapTable $ lookUp' tableVar) kins
        Deletes kdels tableVar -> catchErr handler $
          Class.deletes (unwrapTable $ lookUp' tableVar) kdels
        RetrieveBlobs blobRefsVar -> catchErr handler $
          fmap WrapBlob <$> Class.retrieveBlobs (Proxy @h) session (unwrapBlobRef <$> lookUp' blobRefsVar)
        CreateSnapshot name tableVar -> catchErr handler $
          Class.createSnapshot name (unwrapTable $ lookUp' tableVar)
        OpenSnapshot name -> catchErr handler $
          WrapTable <$> Class.openSnapshot session name
        DeleteSnapshot name -> catchErr handler $
          Class.deleteSnapshot session name
        ListSnapshots -> catchErr handler $
          Class.listSnapshots session
        Duplicate tableVar -> catchErr handler $
          WrapTable <$> Class.duplicate (unwrapTable $ lookUp' tableVar)

    lookUp' :: Var h x -> Realized (IOSim s) x
    lookUp' = lookUpGVar (Proxy @(RealMonad h (IOSim s))) lookUp

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
     , Ord k
     , R.Labellable (k, v, blob)
     , Eq (Class.TableConfig h)
     , Show (Class.TableConfig h)
     , Arbitrary (Class.TableConfig h)
     , Typeable h
     )
  => Proxy (k, v, blob)
  -> ModelVarContext (ModelState h)
  -> ModelState h
  -> Gen (Any (LockstepAction (ModelState h)))
arbitraryActionWithVars _ ctx (ModelState st _stats) =
    QC.frequency $
      concat
        [ genActionsSession
        , genActionsTables
        , genActionsCursor
        , genActionsBlobRef
        ]
  where
    _coveredAllCases :: LockstepAction (ModelState h) a -> ()
    _coveredAllCases = \case
        New{} -> ()
        Close{} -> ()
        Lookups{} -> ()
        RangeLookup{} -> ()
        NewCursor{} -> ()
        CloseCursor{} -> ()
        ReadCursor{} -> ()
        Updates{} -> ()
        Inserts{} -> ()
        Deletes{} -> ()
        RetrieveBlobs{} -> ()
        CreateSnapshot{} -> ()
        DeleteSnapshot{} -> ()
        ListSnapshots{} -> ()
        OpenSnapshot{} -> ()
        Duplicate{} -> ()

    genTableVar = QC.elements tableVars

    tableVars :: [Var h (WrapTable h IO k v blob)]
    tableVars =
      [ fromRight v
      | v <- findVars ctx Proxy
      , case lookupVar ctx v of
          MEither (Left _)                  -> False
          MEither (Right (MTable t)) ->
            Map.member (Model.tableID t) (Model.tables st)
      ]

    genCursorVar = QC.elements cursorVars

    cursorVars :: [Var h (WrapCursor h IO k v blob)]
    cursorVars =
      [ fromRight v
      | v <- findVars ctx Proxy
      , case lookupVar ctx v of
          MEither (Left _)            -> False
          MEither (Right (MCursor c)) ->
            Map.member (Model.cursorID c) (Model.cursors st)
      ]

    genBlobRefsVar = QC.elements blobRefsVars

    blobRefsVars :: [Var h (V.Vector (WrapBlobRef h IO blob))]
    blobRefsVars = fmap (mapGVar (OpComp OpLookupResults)) lookupResultVars
                ++ fmap (mapGVar (OpComp OpQueryResults))  queryResultVars
      where
        lookupResultVars :: [Var h (V.Vector (LookupResult  v (WrapBlobRef h IO blob)))]
        queryResultVars  :: [Var h (V.Vector (QueryResult k v (WrapBlobRef h IO blob)))]

        lookupResultVars = fromRight <$> findVars ctx Proxy
        queryResultVars  = fromRight <$> findVars ctx Proxy

    genUsedSnapshotName   = QC.elements usedSnapshotNames
    genUnusedSnapshotName = QC.elements unusedSnapshotNames

    usedSnapshotNames, unusedSnapshotNames :: [R.SnapshotName]
    (usedSnapshotNames, unusedSnapshotNames) =
      partitionEithers
        [ if Map.member snapshotname (Model.snapshots st)
            then Left  snapshotname -- used
            else Right snapshotname -- unused
        | name <- ["snap1", "snap2", "snap3" ]
        , let snapshotname = fromJust (R.mkSnapshotName name)
        ]

    genActionsSession :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsSession =
        [ (1, fmap Some $ New  @k @v @blob PrettyProxy <$> QC.arbitrary)
        | length tableVars <= 5 ] -- no more than 5 tables at once

     ++ [ (1, fmap Some $ OpenSnapshot @k @v @blob <$> genUsedSnapshotName)
        | not (null usedSnapshotNames) ]

     ++ [ (1, fmap Some $ DeleteSnapshot <$> genUsedSnapshotName)
        | not (null usedSnapshotNames) ]

     ++ [ (1, fmap Some $ pure ListSnapshots)
        | not (null tableVars) ] -- otherwise boring!

    genActionsTables :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsTables
      | null tableVars = []
      | otherwise      =
        [ (1,  fmap Some $ Close <$> genTableVar)
        , (10, fmap Some $ Lookups <$> genLookupKeys <*> genTableVar)
        , (5,  fmap Some $ RangeLookup <$> genRange <*> genTableVar)
        , (10, fmap Some $ Updates <$> genUpdates <*> genTableVar)
        , (10, fmap Some $ Inserts <$> genInserts <*> genTableVar)
        , (10, fmap Some $ Deletes <$> genDeletes <*> genTableVar)
        ]
     ++ [ (3,  fmap Some $ NewCursor <$> QC.arbitrary <*> genTableVar)
        | length cursorVars <= 5 -- no more than 5 cursors at once
        ]
     ++ [ (2,  fmap Some $ CreateSnapshot <$> genUnusedSnapshotName <*> genTableVar)
        | not (null unusedSnapshotNames)
        ]
     ++ [ (5,  fmap Some $ Duplicate <$> genTableVar)
        | length tableVars <= 5 -- no more than 5 tables at once
        ]

    genActionsCursor :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsCursor
      | null cursorVars = []
      | otherwise       =
        [ (2,  fmap Some $ CloseCursor <$> genCursorVar)
        , (10, fmap Some $ ReadCursor <$> (QC.getNonNegative <$> QC.arbitrary)
                                      <*> genCursorVar)
        ]

    genActionsBlobRef :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsBlobRef =
        [ (5, fmap Some $ RetrieveBlobs <$> genBlobRefsVar)
        | not (null blobRefsVars)
        ]

    fromRight ::
         Var h (Either Model.Err a)
      -> Var h a
    fromRight = mapGVar (\op -> OpFromRight `OpComp` op)

    genLookupKeys :: Gen (V.Vector k)
    genLookupKeys = QC.arbitrary

    genRange :: Gen (R.Range k)
    genRange = QC.arbitrary

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

    genBlob :: Gen (Maybe blob)
    genBlob = QC.arbitrary

shrinkActionWithVars ::
     forall h a. (
       Eq (Class.TableConfig h)
     , Arbitrary (Class.TableConfig h)
     , Typeable h
     )
  => ModelVarContext (ModelState h)
  -> ModelState h
  -> LockstepAction (ModelState h) a
  -> [Any (LockstepAction (ModelState h))]
shrinkActionWithVars _ctx _st = \case
    New p conf -> [ Some $ New p conf' | conf' <- QC.shrink conf ]

    -- Shrink inserts and deletes towards updates.
    Updates upds tableVar -> [
        Some $ Updates upds' tableVar
      | upds' <- QC.shrink upds
      ]
    Inserts kvbs tableVar -> [
        Some $ Inserts kvbs' tableVar
      | kvbs' <- QC.shrink kvbs
      ] <> [
        Some $ Updates (V.map f kvbs) tableVar
      | let f (k, v, mb) = (k, R.Insert v mb)
      ]
    Deletes ks tableVar -> [
        Some $ Deletes ks' tableVar
      | ks' <- QC.shrink ks
      ] <> [
        Some $ Updates (V.map f ks) tableVar
      | let f k = (k, R.Delete)
      ]

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
    OpQueryResults       -> Just . MVector
                          . V.mapMaybe (\case MQueryResult x -> getBlobRef x)
                          . \case MVector x -> x

{-------------------------------------------------------------------------------
  Statistics, labelling/tagging
-------------------------------------------------------------------------------}

data Stats = Stats {
    -- === Tags
    -- | Names for which snapshots exist
    snapshotted        :: Set R.SnapshotName
    -- === Final tags (per action sequence, across all tables)
    -- | Number of succesful lookups and their results
  , numLookupsResults  :: {-# UNPACK #-} !(Int, Int, Int)
                          -- (NotFound, Found, FoundWithBlob)
    -- | Number of succesful updates
  , numUpdates         :: {-# UNPACK #-} !(Int, Int, Int)
                          -- (Insert, InsertWithBlob, Delete)
    -- | Actions that succeeded
  , successActions     :: [String]
    -- | Actions that failed with an error
  , failActions        :: [(String, Model.Err)]
    -- === Final tags (per action sequence, per table)
    -- | Number of actions per table (succesful or failing)
  , numActionsPerTable :: !(Map Model.TableID Int)
    -- | The size of tables that were closed. This is used to augment the table
    -- sizes from the final model state (which of course has only tables still
    -- open in the final state).
  , closedTableSizes   :: !(Map Model.TableID Int)
    -- | The ultimate parent for each table. This is the 'TableId' of a table
    -- created using 'new' or 'open'.
  , parentTable        :: Map Model.TableID Model.TableID
    -- | Track the interleavings of operations via different but related tables.
    -- This is a map from the ultimate parent table to a summary log of which
    -- tables (derived from that parent table via duplicate) have had
    -- \"interesting\" actions performed on them. We record only the
    -- interleavings of different tables not multiple actions on the same table.
  , dupTableActionLog  :: Map Model.TableID [Model.TableID]
  }
  deriving stock Show

initStats :: Stats
initStats = Stats {
      -- === Tags
      snapshotted = Set.empty
      -- === Final tags
    , numLookupsResults = (0, 0, 0)
    , numUpdates = (0, 0, 0)
    , successActions = []
    , failActions = []
    , numActionsPerTable = Map.empty
    , closedTableSizes   = Map.empty
    , parentTable        = Map.empty
    , dupTableActionLog  = Map.empty
    }

updateStats ::
     forall h a. ( Show (Class.TableConfig h)
     , Eq (Class.TableConfig h)
     , Arbitrary (Class.TableConfig h)
     , Typeable h
     )
  => LockstepAction (ModelState h) a
  -> ModelLookUp (ModelState h)
  -> Model.Model
  -> Model.Model
  -> Val h a
  -> Stats
  -> Stats
updateStats action lookUp modelBefore _modelAfter result =
      -- === Tags
      updSnapshotted
      -- === Final tags
    . updNumLookupsResults
    . updNumUpdates
    . updSuccessActions
    . updFailActions
    . updNumActionsPerTable
    . updClosedTableSizes
    . updDupTableActionLog
    . updParentTable
  where
    -- === Tags

    updSnapshotted stats = case (action, result) of
      (CreateSnapshot name _, MEither (Right (MUnit ()))) -> stats {
          snapshotted = Set.insert name (snapshotted stats)
        }
      (DeleteSnapshot name, MEither (Right (MUnit ()))) -> stats {
          snapshotted = Set.delete name (snapshotted stats)
        }
      _ -> stats

    -- === Final tags

    updNumLookupsResults stats = case (action, result) of
      (Lookups _ _, MEither (Right (MVector lrs))) -> stats {
          numLookupsResults =
            let count :: (Int, Int, Int)
                      -> Val h (LookupResult v (WrapBlobRef h IO blob))
                      -> (Int, Int, Int)
                count (nf, f, fwb) (MLookupResult x) = case x of
                  NotFound        -> (nf+1, f  , fwb  )
                  Found{}         -> (nf  , f+1, fwb  )
                  FoundWithBlob{} -> (nf  , f  , fwb+1)
            in V.foldl' count (numLookupsResults stats) lrs
        }
      _ -> stats

    updNumUpdates stats = case (action, result) of
        (Updates upds _, MEither (Right (MUnit ()))) -> stats {
            numUpdates = countAll upds
          }
        (Inserts ins _, MEither (Right (MUnit ()))) -> stats {
            numUpdates = countAll $ V.map (\(k, v, b) -> (k, R.Insert v b)) ins
          }
        (Deletes ks _, MEither (Right (MUnit ()))) -> stats {
            numUpdates = countAll $ V.map (\k -> (k, R.Delete)) ks
          }
        _ -> stats
      where
        countAll :: forall k v blob. V.Vector (k, R.Update v blob) -> (Int, Int, Int)
        countAll upds =
          let count :: (Int, Int, Int)
                    -> (k, R.Update v blob)
                    -> (Int, Int, Int)
              count (i, iwb, d) (_, upd) = case upd  of
                R.Insert _ Nothing -> (i+1, iwb  , d  )
                R.Insert _ Just{}  -> (i  , iwb+1, d  )
                R.Delete{}         -> (i  , iwb  , d+1)
          in V.foldl' count (numUpdates stats) upds

    updSuccessActions stats = case result of
        MEither (Right _) -> stats {
            successActions = actionName action : successActions stats
          }
        _ -> stats

    updFailActions stats = case result of
        MEither (Left (MErr e)) -> stats {
            failActions = (actionName action, e) : failActions stats
          }
        _ -> stats

    updNumActionsPerTable :: Stats -> Stats
    updNumActionsPerTable stats = case action of
        New{}
          | MEither (Right (MTable table)) <- result -> initCount table
          | otherwise                                      -> stats
        OpenSnapshot{}
          | MEither (Right (MTable table)) <- result -> initCount table
          | otherwise                                      -> stats
        Duplicate{}
          | MEither (Right (MTable table)) <- result -> initCount table
          | otherwise                                      -> stats

        -- Note that for the other actions we don't count success vs failure.
        -- We don't need that level of detail. We just want to see the
        -- distribution. Success / failure is detailed elsewhere.
        Lookups _ tableVar     -> updateCount tableVar
        RangeLookup _ tableVar -> updateCount tableVar
        NewCursor _ tableVar   -> updateCount tableVar
        Updates _ tableVar     -> updateCount tableVar
        Inserts _ tableVar     -> updateCount tableVar
        Deletes _ tableVar     -> updateCount tableVar
        -- Note that we don't remove tracking map entries for tables that get
        -- closed. We want to know actions per table of all tables used, not
        -- just those that were still open at the end of the sequence of
        -- actions. We do also count Close itself as an action.
        Close tableVar        -> updateCount tableVar

        -- The others are not counted as table actions. We list them here
        -- explicitly so we don't miss any new ones we might add later.
        CloseCursor{}         -> stats
        ReadCursor{}          -> stats
        RetrieveBlobs{}       -> stats
        CreateSnapshot{}            -> stats
        DeleteSnapshot{}      -> stats
        ListSnapshots{}       -> stats
      where
        -- Init to 0 so we get an accurate count of tables with no actions.
        initCount :: forall k v blob. Model.Table k v blob -> Stats
        initCount table =
          let tid = Model.tableID table
           in stats {
                numActionsPerTable = Map.insert tid 0 (numActionsPerTable stats)
              }

        -- Note that batches (of inserts lookups etc) count as one action.
        updateCount :: forall k v blob.
                       Var h (WrapTable h IO k v blob)
                    -> Stats
        updateCount tableVar =
          let tid = getTableId (lookUp tableVar)
           in stats {
                numActionsPerTable = Map.insertWith (+) tid 1
                                                    (numActionsPerTable stats)
              }

    updClosedTableSizes stats = case action of
        Close tableVar
          | MTable t <- lookUp tableVar
          , let tid          = Model.tableID t
            -- This lookup can fail if the table was already closed:
          , Just (_, table) <- Map.lookup tid (Model.tables modelBefore)
          , let  tsize       = Model.withSomeTable Model.size table
          -> stats {
               closedTableSizes = Map.insert tid tsize (closedTableSizes stats)
             }
        _ -> stats

    updParentTable stats = case (action, result) of
        (New{}, MEither (Right (MTable tbl))) ->
          stats {
            parentTable = Map.insert (Model.tableID tbl)
                                     (Model.tableID tbl)
                                     (parentTable stats)
          }
        (OpenSnapshot{}, MEither (Right (MTable tbl))) ->
          stats {
            parentTable = Map.insert (Model.tableID tbl)
                                     (Model.tableID tbl)
                                     (parentTable stats)
          }
        (Duplicate ptblVar, MEither (Right (MTable tbl))) ->
          let -- immediate and ultimate parent table ids
              iptblId, uptblId :: Model.TableID
              iptblId = getTableId (lookUp ptblVar)
              uptblId = parentTable stats Map.! iptblId
           in stats {
                parentTable = Map.insert (Model.tableID tbl)
                                         uptblId
                                         (parentTable stats)
              }
        _ -> stats

    updDupTableActionLog stats | MEither (Right _) <- result =
      case action of
        Lookups     ks   tableVar
          | not (null ks)         -> updateLastActionLog tableVar
        RangeLookup r    tableVar
          | not (emptyRange r)    -> updateLastActionLog tableVar
        NewCursor   _    tableVar -> updateLastActionLog tableVar
        Updates     upds tableVar
          | not (null upds)       -> updateLastActionLog tableVar
        Inserts     ins  tableVar
          | not (null ins)        -> updateLastActionLog tableVar
        Deletes     ks   tableVar
          | not (null ks)         -> updateLastActionLog tableVar
        Close            tableVar -> updateLastActionLog tableVar
        _                         -> stats
      where
        -- add the current table to the front of the list of tables, if it's
        -- not the latest one already
        updateLastActionLog :: GVar Op (WrapTable h IO k v blob) -> Stats
        updateLastActionLog tableVar =
          case Map.lookup pthid (dupTableActionLog stats) of
            Just (thid' : _)
              | thid == thid' -> stats -- the most recent action was via this table
            malog ->
              let alog = thid : fromMaybe [] malog
               in stats {
                    dupTableActionLog = Map.insert pthid alog
                                                   (dupTableActionLog stats)
                  }
          where
            thid  = getTableId (lookUp tableVar)
            pthid = parentTable stats Map.! thid

        emptyRange (R.FromToExcluding l u) = l >= u
        emptyRange (R.FromToIncluding l u) = l >  u

    updDupTableActionLog stats = stats

    getTableId :: ModelValue (ModelState h) (WrapTable h IO k v blob)
                     -> Model.TableID
    getTableId (MTable t) = Model.tableID t

-- | Tags for every step
data Tag =
    -- | Snapshot with a name that already exists
    SnapshotTwice
    -- | Open an existing snapshot
  | OpenExistingSnapshot
    -- | Open a missing snapshot
  | OpenMissingSnapshot
    -- | Delete an existing snapshot
  | DeleteExistingSnapshot
    -- | Delete a missing snapshot
  | DeleteMissingSnapshot
    -- | Open a snapshot with the wrong label
  | OpenSnapshotWrongLabel -- TODO: implement
    -- | A merge happened on level @n@
  | MergeOnLevel Int -- TODO: implement
    -- | A table was closed twice
  | TableCloseTwice String -- TODO: implement
  deriving stock (Show, Eq, Ord)

-- | This is run for after every action
tagStep' ::
     (ModelState h, ModelState h)
  -> LockstepAction (ModelState h) a
  -> Val h a
  -> [Tag]
tagStep' (ModelState _stateBefore statsBefore,
          ModelState _stateAfter _statsAfter)
          action _result =
    catMaybes [
      tagSnapshotTwice
    , tagOpenExistingSnapshot
    , tagOpenExistingSnapshot
    , tagOpenMissingSnapshot
    , tagDeleteExistingSnapshot
    , tagDeleteMissingSnapshot
    ]
  where
    tagSnapshotTwice
      | CreateSnapshot name _ <- action
      , name `Set.member` snapshotted statsBefore
      = Just SnapshotTwice
      | otherwise
      = Nothing

    tagOpenExistingSnapshot
      | OpenSnapshot name <- action
      , name `Set.member` snapshotted statsBefore
      = Just OpenExistingSnapshot
      | otherwise
      = Nothing

    tagOpenMissingSnapshot
      | OpenSnapshot name <- action
      , not (name `Set.member` snapshotted statsBefore)
      = Just OpenMissingSnapshot
      | otherwise
      = Nothing

    tagDeleteExistingSnapshot
      | DeleteSnapshot name <- action
      , name `Set.member` snapshotted statsBefore
      = Just DeleteExistingSnapshot
      | otherwise
      = Nothing

    tagDeleteMissingSnapshot
      | DeleteSnapshot name <- action
      , not (name `Set.member` snapshotted statsBefore)
      = Just DeleteMissingSnapshot
      | otherwise
      = Nothing

-- | Tags for the final state
data FinalTag =
    -- | Total number of lookup results that were 'SUT.NotFound'
    NumLookupsNotFound String
    -- | Total number of lookup results that were 'SUT.Found'
  | NumLookupsFound String
    -- | Total number of lookup results that were 'SUT.FoundWithBlob'
  | NumLookupsFoundWithBlob String
    -- | Number of 'Class.Insert's succesfully submitted to a table
    -- (this includes submissions through both 'Class.updates' and
    -- 'Class.inserts')
  | NumInserts String
    -- | Number of 'Class.InsertWithBlob's succesfully submitted to a table
    -- (this includes submissions through both 'Class.updates' and
    -- 'Class.inserts')
  | NumInsertsWithBlobs String
    -- | Number of 'Class.Delete's succesfully submitted to a table
    -- (this includes submissions through both 'Class.updates' and
    -- 'Class.deletes')
  | NumDeletes String
    -- | Total number of actions (failing, succeeding, either)
  | NumActions String
    -- | Which actions succeded
  | ActionSuccess String
    -- | Which actions failed
  | ActionFail String Model.Err
    -- | Total number of flushes
  | NumFlushes String -- TODO: implement
    -- | Number of tables created (new, open or duplicate)
  | NumTables String
    -- | Number of actions on each table
  | NumTableActions String
    -- | Total /logical/ size of a table
  | TableSize String
    -- | Number of interleaved actions on duplicate tables
  | DupTableActionLog String
  deriving stock Show

-- | This is run only after completing every action
tagFinalState' :: Lockstep (ModelState h) -> [(String, [FinalTag])]
tagFinalState' (getModel -> ModelState finalState finalStats) = concat [
      tagNumLookupsResults
    , tagNumUpdates
    , tagNumActions
    , tagSuccessActions
    , tagFailActions
    , tagNumTables
    , tagNumTableActions
    , tagTableSizes
    , tagDupTableActionLog
    ]
  where
    tagNumLookupsResults = [
          ("Lookups not found"      , [NumLookupsNotFound      $ showPowersOf 10 nf])
        , ("Lookups found"          , [NumLookupsFound         $ showPowersOf 10 f])
        , ("Lookups found with blob", [NumLookupsFoundWithBlob $ showPowersOf 10 fwb])
        ]
      where (nf, f, fwb) = numLookupsResults finalStats

    tagNumUpdates = [
          ("Inserts"            , [NumInserts          $ showPowersOf 10 i])
        , ("Inserts with blobs" , [NumInsertsWithBlobs $ showPowersOf 10 iwb])
        , ("Deletes"            , [NumDeletes          $ showPowersOf 10 d])
        ]
      where (i, iwb, d) = numUpdates finalStats

    tagNumActions =
        [ let n = length (successActions finalStats) in
          ("Actions that succeeded total", [NumActions (showPowersOf 10 n)])
        , let n = length (failActions finalStats) in
          ("Actions that failed total", [NumActions (showPowersOf 10 n)])
        , let n = length (successActions finalStats)
                + length (failActions finalStats) in
          ("Actions total", [NumActions (showPowersOf 10 n)])
        ]

    tagSuccessActions =
        [ ("Actions that succeeded", [ActionSuccess c])
        | c <- successActions finalStats ]

    tagFailActions =
        [ ("Actions that failed", [ActionFail c e])
        | (c, e) <- failActions finalStats ]

    tagNumTables =
        [ ("Number of tables", [NumTables (showPowersOf 2 n)])
        | let n = Map.size (numActionsPerTable finalStats)
        ]

    tagNumTableActions =
        [ ("Number of actions per table", [ NumTableActions (showPowersOf 2 n) ])
        | n <- Map.elems (numActionsPerTable finalStats)
        ]

    tagTableSizes =
        [ ("Table sizes", [ TableSize (showPowersOf 2 size) ])
        | let openSizes, closedSizes :: Map Model.TableID Int
              openSizes   = Model.withSomeTable Model.size . snd <$>
                              Model.tables finalState
              closedSizes = closedTableSizes finalStats
        , size <- Map.elems (openSizes `Map.union` closedSizes)
        ]

    tagDupTableActionLog =
        [ ("Interleaved actions on table duplicates",
           [DupTableActionLog (showPowersOf 2 n)])
        | (_, alog) <- Map.toList (dupTableActionLog finalStats)
        , let n = length alog
        ]

{-------------------------------------------------------------------------------
  Utils
-------------------------------------------------------------------------------}

-- | Version of 'runActionsBracket' with tagging of the final state.
--
-- The 'tagStep' feature tags each step (i.e., 'Action'), but there are cases
-- where one wants to tag a /list of/ 'Action's. For example, if one wants to
-- count how often something happens over the course of running these actions,
-- then we would want to only tag the final state, not intermediate steps.
runActionsBracket' ::
     forall state st m e.  (
       RunLockstep state m
     , e ~ Error (Lockstep state)
     , forall a. IsPerformResult e a
     )
  => Proxy state
  -> IO st
  -> (st -> IO ())
  -> (m QC.Property -> st -> IO QC.Property)
  -> (Lockstep state -> [(String, [FinalTag])])
  -> Actions (Lockstep state) -> QC.Property
runActionsBracket' p init cleanup runner tagger actions =
    tagFinalState actions tagger
  $ Lockstep.Run.runActionsBracket p init cleanup' runner actions
  where
    cleanup' st = do
      cleanup st
      checkForgottenRefs

tagFinalState ::
     forall state. StateModel (Lockstep state)
  => Actions (Lockstep state)
  -> (Lockstep state -> [(String, [FinalTag])])
  -> Property
  -> Property
tagFinalState actions tagger =
    flip (foldr (\(key, values) -> QC.tabulate key (fmap show values))) finalTags
  where
    finalAnnState :: Annotated (Lockstep state)
    finalAnnState = stateAfter @(Lockstep state) actions

    finalTags = tagger $ underlyingState finalAnnState
