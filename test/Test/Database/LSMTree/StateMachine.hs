{-# LANGUAGE CPP                   #-}
{-# LANGUAGE OverloadedStrings     #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilies          #-}
{-# LANGUAGE UndecidableInstances  #-}

#if MIN_VERSION_GLASGOW_HASKELL(9,8,1,0)
{-# LANGUAGE TypeAbstractions      #-}
#endif

{-# OPTIONS_GHC -Wno-orphans #-}

{-
  TODO: improve generation and shrinking of dependencies. See
  https://github.com/IntersectMBO/lsm-tree/pull/4#discussion_r1334295154.

  TODO: The 'RetrieveBlobs' action will currently only retrieve blob references
  that come from a single batch lookup or range lookup. It would be interesting
  to also retrieve blob references from a mix of different batch lookups and/or
  range lookups. This would require some non-trivial changes, such as changes to
  'Op' to also include expressions for manipulating lists, such that we can map
  @'Var' ['R.BlobRef' b]@ to @'Var' ('R.BlobRef' b)@. 'RetrieveBlobs'
  would then hold a list of variables (e.g., @['Var' ('R.BlobRef b')]@)
  instead of a variable of a list (@'Var' ['R.BlobRef' b]@).

  TODO: it is currently not correctly modelled what happens if blob references
  are retrieved from an incorrect table.
-}
module Test.Database.LSMTree.StateMachine (
    tests
  , labelledExamples
    -- * Properties
  , propLockstep_ModelIOImpl
  , propLockstep_RealImpl_RealFS_IO
  , propLockstep_RealImpl_MockFS_IO
  , propLockstep_RealImpl_MockFS_IOSim
  , CheckCleanup (..)
  , CheckFS (..)
  , CheckRefs (..)
    -- * Lockstep
  , ModelState (..)
  , Key (..)
  , Value (..)
  , Blob (..)
  , StateModel (..)
  , Action (..)
  , Action' (..)
  ) where

import           Control.ActionRegistry (AbortActionRegistryError (..),
                     CommitActionRegistryError (..), getActionError,
                     getReasonExitCaseException)
import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Concurrent.Class.MonadSTM.Strict
import           Control.Exception (assert)
import           Control.Monad (forM_, void, (<=<))
import           Control.Monad.Class.MonadThrow (Exception (..), Handler (..),
                     MonadCatch (..), MonadThrow (..), SomeException, catches,
                     displayException, fromException)
import           Control.Monad.IOSim
import           Control.Monad.Primitive
import           Control.Monad.Reader (ReaderT (..))
import           Control.RefCount (RefException, checkForgottenRefs,
                     ignoreForgottenRefs)
import           Control.Tracer (Tracer, nullTracer)
import           Data.Bifunctor (Bifunctor (..))
import           Data.Constraint (Dict (..))
import           Data.Either (partitionEithers)
import           Data.Kind (Type)
#if MIN_VERSION_base(4,20,0)
import           Data.List (nub)
#else
import           Data.List (foldl', nub)
                 -- foldl' is included in the Prelude from base 4.20 onwards
#endif
import           Data.List.NonEmpty (NonEmpty (..))
import qualified Data.List.NonEmpty as NE
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (catMaybes, fromMaybe)
import           Data.Monoid (First (..))
import           Data.Primitive.MutVar
import           Data.Set (Set)
import qualified Data.Set as Set
import           Data.Typeable (Proxy (..), Typeable, cast)
import qualified Data.Vector as V
import           Database.LSMTree (BlobRefInvalidError (..),
                     CursorClosedError (..), SessionClosedError (..),
                     SessionDirCorruptedError (..),
                     SessionDirDoesNotExistError (..),
                     SessionDirLockedError (..), SnapshotCorruptedError (..),
                     SnapshotDoesNotExistError (..), SnapshotExistsError (..),
                     SnapshotNotCompatibleError (..), TableClosedError (..),
                     TableCorruptedError (..),
                     TableUnionNotCompatibleError (..))
import qualified Database.LSMTree as R
import           Database.LSMTree.Class (Entry (..), LookupResult (..))
import qualified Database.LSMTree.Class as Class
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact)
import           Database.LSMTree.Extras.NoThunks (propNoThunks)
import           Database.LSMTree.Internal.Serialise (SerialisedBlob,
                     SerialisedValue)
import qualified Database.LSMTree.Internal.Types as R.Types
import qualified Database.LSMTree.Internal.Unsafe as R.Unsafe
import qualified Database.LSMTree.Model.IO as ModelIO
import qualified Database.LSMTree.Model.Session as Model
import           NoThunks.Class
import           Prelude hiding (init)
import           System.Directory (removeDirectoryRecursive)
import           System.FS.API (FsError (..), HasFS, MountPoint (..), mkFsPath)
import           System.FS.BlockIO.API (HasBlockIO, defaultIOCtxParams)
import           System.FS.BlockIO.IO (ioHasBlockIO)
import           System.FS.IO (HandleIO, ioHasFS)
import qualified System.FS.Sim.Error as FSSim
import           System.FS.Sim.Error (Errors)
import qualified System.FS.Sim.MockFS as MockFS
import           System.FS.Sim.MockFS (MockFS)
import           System.FS.Sim.Stream (Stream)
import           System.IO.Temp (createTempDirectory,
                     getCanonicalTemporaryDirectory)
import           Test.Database.LSMTree.StateMachine.Op (HasBlobRef (getBlobRef),
                     Op (..))
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
import           Test.Util.FS (approximateEqStream, noRemoveDirectoryRecursiveE,
                     propNoOpenHandles, propNumOpenHandles)
import           Test.Util.FS.Error
import           Test.Util.PrettyProxy
import           Test.Util.QC (Choice)
import qualified Test.Util.QLS as QLS
import           Test.Util.TypeFamilyWrappers (WrapBlob (..), WrapBlobRef (..),
                     WrapCursor (..), WrapTable (..))

{-------------------------------------------------------------------------------
  Test tree
-------------------------------------------------------------------------------}

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.StateMachine" [
      testProperty "propLockstep_ModelIOImpl"
        propLockstep_ModelIOImpl

    , testProperty "propLockstep_RealImpl_RealFS_IO" $
        propLockstep_RealImpl_RealFS_IO nullTracer

    , testProperty "propLockstep_RealImpl_MockFS_IO" $
        propLockstep_RealImpl_MockFS_IO nullTracer CheckCleanup CheckFS CheckRefs

    , testProperty "propLockstep_RealImpl_MockFS_IOSim" $
        propLockstep_RealImpl_MockFS_IOSim nullTracer CheckCleanup CheckFS CheckRefs
    ]

labelledExamples :: IO ()
labelledExamples = QC.labelledExamples $ Lockstep.Run.tagActions (Proxy @(ModelState R.Table))

{-------------------------------------------------------------------------------
  propLockstep: reference implementation
-------------------------------------------------------------------------------}

instance Arbitrary Model.TableConfig where
  arbitrary :: Gen Model.TableConfig
  arbitrary = pure Model.TableConfig

deriving via AllowThunk (ModelIO.Session IO)
    instance NoThunks (ModelIO.Session IO)

propLockstep_ModelIOImpl ::
     Actions (Lockstep (ModelState ModelIO.Table))
  -> QC.Property
propLockstep_ModelIOImpl =
    runActionsBracket
      (Proxy @(ModelState ModelIO.Table))
      CheckCleanup
      NoCheckRefs -- there are no references to check for in the ModelIO implementation
      acquire
      release
      (\r (session, errsVar, logVar) -> do
            faultsVar <- newMutVar []
            let
              env :: RealEnv ModelIO.Table IO
              env = RealEnv {
                  envSession = session
                , envHandlers = [handler]
                , envErrors = errsVar
                , envErrorsLog = logVar
                , envInjectFaultResults = faultsVar
                }
            prop <- runReaderT r env
            faults <- readMutVar faultsVar
            pure $ QC.tabulate "Fault results" (fmap show faults) prop
        )
      tagFinalState'
  where
    acquire :: IO (Class.Session ModelIO.Table IO, StrictTVar IO Errors, StrictTVar IO ErrorsLog)
    acquire = do
      session <- Class.openSession ModelIO.NoSessionArgs
      errsVar <- newTVarIO FSSim.emptyErrors
      logVar <- newTVarIO emptyLog
      pure (session, errsVar, logVar)

    release :: (Class.Session ModelIO.Table IO, StrictTVar IO Errors, StrictTVar IO ErrorsLog) -> IO ()
    release (session, _, _) = Class.closeSession session

    handler :: Handler IO (Maybe Model.Err)
    handler = Handler $ pure . handler'
      where
        handler' :: ModelIO.Err -> Maybe Model.Err
        handler' (ModelIO.Err err) = Just err

{-------------------------------------------------------------------------------
  propLockstep: real implementation
-------------------------------------------------------------------------------}

instance Arbitrary R.TableConfig where
  arbitrary = do
    confMergeSchedule <- QC.frequency [
          (1, pure R.OneShot)
        , (4, pure R.Incremental)
        ]
    confWriteBufferAlloc <- QC.arbitrary
    confFencePointerIndex <- QC.arbitrary
    pure $ R.TableConfig {
        R.confMergePolicy       = R.LazyLevelling
      , R.confSizeRatio         = R.Four
      , confWriteBufferAlloc
      , R.confBloomFilterAlloc  = R.AllocFixed 10
      , confFencePointerIndex
      , R.confDiskCachePolicy   = R.DiskCacheNone
      , confMergeSchedule
      }

  shrink R.TableConfig{..} =
      [ R.TableConfig {
            confWriteBufferAlloc = confWriteBufferAlloc'
          , confFencePointerIndex = confFencePointerIndex'
          , ..
          }
      | ( confWriteBufferAlloc', confFencePointerIndex')
          <- QC.shrink (confWriteBufferAlloc, confFencePointerIndex)
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
      pure (R.AllocNumEntries x)

  shrink (R.AllocNumEntries x) =
      [ R.AllocNumEntries x'
      | QC.Positive x' <- QC.shrink (QC.Positive x)
      ]

deriving stock instance Enum R.FencePointerIndexType
deriving stock instance Bounded R.FencePointerIndexType
instance Arbitrary R.FencePointerIndexType where
  arbitrary = QC.arbitraryBoundedEnum
  shrink R.OrdinaryIndex = []
  shrink R.CompactIndex  = [R.OrdinaryIndex]

propLockstep_RealImpl_RealFS_IO ::
     Tracer IO R.LSMTreeTrace
  -> Actions (Lockstep (ModelState R.Table))
  -> QC.Property
propLockstep_RealImpl_RealFS_IO tr =
    runActionsBracket
      (Proxy @(ModelState R.Table))
      CheckCleanup
      CheckRefs
      acquire
      release
      (\r (_, session, errsVar, logVar) -> do
            faultsVar <- newMutVar []
            let
              env :: RealEnv R.Table IO
              env = RealEnv {
                  envSession = session
                , envHandlers = realErrorHandlers @IO
                , envErrors = errsVar
                , envErrorsLog = logVar
                , envInjectFaultResults = faultsVar
                }
            prop <- runReaderT r env
            faults <- readMutVar faultsVar
            pure $ QC.tabulate "Fault results" (fmap show faults) prop
        )
      tagFinalState'
  where
    acquire :: IO (FilePath, Class.Session R.Table IO, StrictTVar IO Errors, StrictTVar IO ErrorsLog)
    acquire = do
        (tmpDir, hasFS, hasBlockIO) <- createSystemTempDirectory "prop_lockstepIO_RealImpl_RealFS"
        session <- R.openSession tr hasFS hasBlockIO (mkFsPath [])
        errsVar <- newTVarIO FSSim.emptyErrors
        logVar <- newTVarIO emptyLog
        pure (tmpDir, session, errsVar, logVar)

    release :: (FilePath, Class.Session R.Table IO, StrictTVar IO Errors, StrictTVar IO ErrorsLog) -> IO Property
    release (tmpDir, !session, _, _) = do
        !prop <- propNoThunks session
        R.closeSession session
        removeDirectoryRecursive tmpDir
        pure prop

propLockstep_RealImpl_MockFS_IO ::
     Tracer IO R.LSMTreeTrace
  -> CheckCleanup
  -> CheckFS
  -> CheckRefs
  -> Actions (Lockstep (ModelState R.Table))
  -> QC.Property
propLockstep_RealImpl_MockFS_IO tr cleanupFlag fsFlag refsFlag =
    runActionsBracket
      (Proxy @(ModelState R.Table))
      cleanupFlag
      refsFlag
      (acquire_RealImpl_MockFS tr)
      (release_RealImpl_MockFS fsFlag)
      (\r (_, session, errsVar, logVar) -> do
            faultsVar <- newMutVar []
            let
              env :: RealEnv R.Table IO
              env = RealEnv {
                  envSession = session
                , envHandlers = realErrorHandlers @IO
                , envErrors = errsVar
                , envErrorsLog = logVar
                , envInjectFaultResults = faultsVar
                }
            prop <- runReaderT r env
            faults <- readMutVar faultsVar
            pure $ QC.tabulate "Fault results" (fmap show faults) prop
        )
      tagFinalState'

-- We can not use @bracket@ inside @PropertyM@, so @acquire_RealImpl_MockFS@ and
-- @release_RealImpl_MockFS@ are not run in a masked state and it is not
-- guaranteed that the latter runs if the former succeeded. Therefore, if
-- @runActions@ fails (with exceptions), then not having @bracket@ might lead to
-- more exceptions, which can obfuscate the orginal reason that the property
-- failed. Because of this, if @prop@ fails, it's probably best to also try
-- running the @IO@ version of this property with the failing seed, and compare
-- the counterexamples to see which one is more interesting.
propLockstep_RealImpl_MockFS_IOSim ::
     (forall s. Tracer (IOSim s) R.LSMTreeTrace)
  -> CheckCleanup
  -> CheckFS
  -> CheckRefs
  -> Actions (Lockstep (ModelState R.Table))
  -> QC.Property
propLockstep_RealImpl_MockFS_IOSim tr cleanupFlag fsFlag refsFlag actions =
    monadicIOSim_ prop
  where
    prop :: forall s. PropertyM (IOSim s) Property
    prop = do
        (fsVar, session, errsVar, logVar) <- QC.run (acquire_RealImpl_MockFS tr)
        faultsVar <- QC.run $ newMutVar []
        let
          env :: RealEnv R.Table (IOSim s)
          env = RealEnv {
              envSession = session
            , envHandlers = realErrorHandlers @(IOSim s)
            , envErrors = errsVar
            , envErrorsLog = logVar
            , envInjectFaultResults = faultsVar
            }
        void $ QD.runPropertyReaderT
                (QD.runActions @(Lockstep (ModelState R.Table)) actions)
                env
        faults <- QC.run $ readMutVar faultsVar
        p <- QC.run $ propCleanup cleanupFlag $
          release_RealImpl_MockFS fsFlag (fsVar, session, errsVar, logVar)
        p' <- QC.run $ propRefs refsFlag
        pure
          $ tagFinalState actions tagFinalState'
          $ QC.tabulate "Fault results" (fmap show faults)
          $ p QC..&&. p'

acquire_RealImpl_MockFS ::
     R.IOLike m
  => Tracer m R.LSMTreeTrace
  -> m (StrictTMVar m MockFS, Class.Session R.Table m, StrictTVar m Errors, StrictTVar m ErrorsLog)
acquire_RealImpl_MockFS tr = do
    fsVar <- newTMVarIO MockFS.empty
    errsVar <- newTVarIO FSSim.emptyErrors
    logVar <- newTVarIO emptyLog
    (hfs, hbio) <- simErrorHasBlockIOLogged fsVar errsVar logVar
    session <- R.openSession tr hfs hbio (mkFsPath [])
    pure (fsVar, session, errsVar, logVar)

-- | Flag that turns on\/off file system checks.
data CheckFS = CheckFS | NoCheckFS

release_RealImpl_MockFS ::
     (R.IOLike m)
  => CheckFS
  -> (StrictTMVar m MockFS, Class.Session R.Table m, StrictTVar m Errors, StrictTVar m ErrorsLog)
  -> m Property
release_RealImpl_MockFS fsFlag (fsVar, session, _, _) = do
    sts <- getAllSessionTables session
    forM_ sts $ \(SomeTable t) -> R.closeTable t
    scs <- getAllSessionCursors session
    forM_ scs $ \(SomeCursor c) -> R.closeCursor c
    mockfs1 <- atomically $ readTMVar fsVar
    R.closeSession session
    mockfs2 <- atomically $ readTMVar fsVar
    pure $ case fsFlag of
      CheckFS -> propNumOpenHandles 1 mockfs1 QC..&&. propNoOpenHandles mockfs2
      NoCheckFS -> QC.property ()

data SomeTable m
  = SomeTable (forall k v b. R.Table m k v b)

data SomeCursor m
  = SomeCursor (forall k v b. R.Cursor m k v b)

getAllSessionTables ::
     (MonadSTM m, MonadThrow m, MonadMVar m)
  => R.Session m
  -> m [SomeTable m]
getAllSessionTables (R.Types.Session s) = do
    R.Unsafe.withOpenSession s $ \seshEnv -> do
      ts <- readMVar (R.Unsafe.sessionOpenTables seshEnv)
      pure ((\x -> SomeTable (R.Types.Table x))  <$> Map.elems ts)

getAllSessionCursors ::
     (MonadSTM m, MonadThrow m, MonadMVar m)
  => R.Session m
  -> m [SomeCursor m]
getAllSessionCursors (R.Types.Session s) =
    R.Unsafe.withOpenSession s $ \seshEnv -> do
      cs <- readMVar (R.Unsafe.sessionOpenCursors seshEnv)
      pure ((\x -> SomeCursor (R.Types.Cursor x))  <$> Map.elems cs)

createSystemTempDirectory ::  [Char] -> IO (FilePath, HasFS IO HandleIO, HasBlockIO IO HandleIO)
createSystemTempDirectory prefix = do
    systemTempDir <- getCanonicalTemporaryDirectory
    tempDir <- createTempDirectory systemTempDir prefix
    let hasFS = ioHasFS (MountPoint tempDir)
    hasBlockIO <- ioHasBlockIO hasFS defaultIOCtxParams
    pure (tempDir, hasFS, hasBlockIO)

{-------------------------------------------------------------------------------
  Error handlers
-------------------------------------------------------------------------------}

realErrorHandlers :: Applicative f => [Handler f (Maybe Model.Err)]
realErrorHandlers = [Handler $ pure . Just . handleSomeException]

handleSomeException :: SomeException -> Model.Err
handleSomeException e =
  fromMaybe (Model.ErrOther $ displayException e) . getFirst . mconcat . fmap First $
    [ handleCommitActionRegistryError <$> fromException e
    , handleAbortActionRegistryError <$> fromException e
    , handleSessionDirDoesNotExistError <$> fromException e
    , handleSessionDirLockedError <$> fromException e
    , handleSessionDirCorruptedError <$> fromException e
    , handleSessionClosedError <$> fromException e
    , handleTableClosedError <$> fromException e
    , handleTableCorruptedError <$> fromException e
    , handleTableUnionNotCompatibleError <$> fromException e
    , handleSnapshotExistsError <$> fromException e
    , handleSnapshotDoesNotExistError <$> fromException e
    , handleSnapshotCorruptedError <$> fromException e
    , handleSnapshotNotCompatibleError <$> fromException e
    , handleBlobRefInvalidError <$> fromException e
    , handleCursorClosedError <$> fromException e
    , handleFsError <$> fromException e
    ]

handleCommitActionRegistryError :: CommitActionRegistryError -> Model.Err
handleCommitActionRegistryError = \case
  CommitActionRegistryError actionErrors ->
    Model.ErrCommitActionRegistry $
      handleSomeException <$> (getActionError <$> actionErrors)

handleAbortActionRegistryError :: AbortActionRegistryError -> Model.Err
handleAbortActionRegistryError = \case
  AbortActionRegistryError abortReason actionErrors ->
    Model.ErrAbortActionRegistry
      (handleSomeException <$> getReasonExitCaseException abortReason)
      (handleSomeException . getActionError <$> actionErrors)

handleSessionDirDoesNotExistError :: SessionDirDoesNotExistError -> Model.Err
handleSessionDirDoesNotExistError = \case
  ErrSessionDirDoesNotExist _dir -> Model.ErrSessionDirDoesNotExist

handleSessionDirLockedError :: SessionDirLockedError -> Model.Err
handleSessionDirLockedError = \case
  ErrSessionDirLocked _dir -> Model.ErrSessionDirLocked

handleSessionDirCorruptedError :: SessionDirCorruptedError -> Model.Err
handleSessionDirCorruptedError = \case
  ErrSessionDirCorrupted _dir -> Model.ErrSessionDirCorrupted

handleSessionClosedError :: SessionClosedError -> Model.Err
handleSessionClosedError = \case
  ErrSessionClosed -> Model.ErrSessionClosed

handleTableClosedError :: TableClosedError -> Model.Err
handleTableClosedError = \case
  ErrTableClosed -> Model.ErrTableClosed

handleTableCorruptedError :: TableCorruptedError -> Model.Err
handleTableCorruptedError = \case
  ErrLookupByteCountDiscrepancy{} -> Model.ErrTableCorrupted

handleTableUnionNotCompatibleError :: TableUnionNotCompatibleError -> Model.Err
handleTableUnionNotCompatibleError = \case
  ErrTableUnionHandleTypeMismatch{} -> Model.ErrTableUnionHandleTypeMismatch
  ErrTableUnionSessionMismatch{} -> Model.ErrTableUnionSessionMismatch

handleSnapshotExistsError :: SnapshotExistsError -> Model.Err
handleSnapshotExistsError = \case
  ErrSnapshotExists name -> Model.ErrSnapshotExists name

handleSnapshotDoesNotExistError :: SnapshotDoesNotExistError -> Model.Err
handleSnapshotDoesNotExistError = \case
  ErrSnapshotDoesNotExist name -> Model.ErrSnapshotDoesNotExist name

handleSnapshotCorruptedError :: SnapshotCorruptedError -> Model.Err
handleSnapshotCorruptedError = \case
  ErrSnapshotCorrupted name _ -> Model.ErrSnapshotCorrupted name

handleSnapshotNotCompatibleError :: SnapshotNotCompatibleError -> Model.Err
handleSnapshotNotCompatibleError = \case
  ErrSnapshotWrongTableType name _ _ -> Model.ErrSnapshotWrongTableType name
  ErrSnapshotWrongLabel name _ _ -> Model.ErrSnapshotWrongLabel name

handleBlobRefInvalidError :: BlobRefInvalidError -> Model.Err
handleBlobRefInvalidError = \case
  ErrBlobRefInvalid _ -> Model.ErrBlobRefInvalid

handleCursorClosedError :: CursorClosedError -> Model.Err
handleCursorClosedError = \case
  ErrCursorClosed -> Model.ErrCursorClosed

handleFsError :: FsError -> Model.Err
handleFsError = Model.ErrFsError . displayException

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

keyValueBlobLabel :: R.SnapshotLabel
keyValueBlobLabel = R.SnapshotLabel "Key Value Blob"

instance R.ResolveValue Value where
  resolveSerialised _ = (<>)

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

type Act h a = Action (Lockstep (ModelState h)) a
type Act' h a = Action' h (Either Model.Err a)
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
  , R.ResolveValue a
  , Arbitrary a
  )

type B a = (
    Class.C_ a
  , R.SerialiseValue a
  , Arbitrary a
  )

-- | Common constraints for keys, values and blobs
type C k v b = (K k, V v, B b)

{-------------------------------------------------------------------------------
  StateModel
-------------------------------------------------------------------------------}

newtype SilentCorruption = SilentCorruption {bitChoice :: Choice}
  deriving stock (Eq, Show)
  deriving newtype (Arbitrary)

instance ( Show (Class.TableConfig h)
         , Eq (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => StateModel (Lockstep (ModelState h)) where
  data instance Action (Lockstep (ModelState h)) a where
    Action :: Maybe Errors -> Action' h a -> Act h a

  initialState    = Lockstep.Defaults.initialState initModelState
  nextState       = Lockstep.Defaults.nextState
  precondition    = Lockstep.Defaults.precondition
  arbitraryAction = Lockstep.Defaults.arbitraryAction
  shrinkAction    = Lockstep.Defaults.shrinkAction

  -- Does not use the default implementation, because that would print "Action".
  -- We print the name of the inner 'Action'' instead.
  actionName (Action _ action') = head . words . show $ action'

deriving stock instance Show (Class.TableConfig h)
                     => Show (LockstepAction (ModelState h) a)

instance ( Eq (Class.TableConfig h)
         , Typeable h
         ) => Eq (LockstepAction (ModelState h) a) where
  (==) :: LockstepAction (ModelState h) a -> LockstepAction (ModelState h) a -> Bool
  Action merrs1 x == Action merrs2 y = merrs1 == merrs2 && x == y
    where
      _coveredAllCases :: Action (Lockstep (ModelState h)) a -> ()
      _coveredAllCases = \case
          Action{} -> ()

data Action' h a where
  -- Tables
  New ::
       C k v b
    => {-# UNPACK #-} !(PrettyProxy (k, v, b))
    -> Class.TableConfig h
    -> Act' h (WrapTable h IO k v b)
  Close ::
       C k v b
    => Var h (WrapTable h IO k v b)
    -> Act' h ()
  -- Queries
  Lookups ::
       C k v b
    => V.Vector k -> Var h (WrapTable h IO k v b)
    -> Act' h (V.Vector (LookupResult v (WrapBlobRef h IO b)))
  RangeLookup ::
       (C k v b, Ord k)
    => R.Range k -> Var h (WrapTable h IO k v b)
    -> Act' h (V.Vector (Entry k v (WrapBlobRef h IO b)))
  -- Cursor
  NewCursor ::
       C k v b
    => Maybe k
    -> Var h (WrapTable h IO k v b)
    -> Act' h (WrapCursor h IO k v b)
  CloseCursor ::
       C k v b
    => Var h (WrapCursor h IO k v b)
    -> Act' h ()
  ReadCursor ::
       C k v b
    => Int
    -> Var h (WrapCursor h IO k v b)
    -> Act' h (V.Vector (Entry k v (WrapBlobRef h IO b)))
  -- Updates
  Updates ::
       C k v b
    => V.Vector (k, R.Update v b) -> Var h (WrapTable h IO k v b)
    -> Act' h ()
  Inserts ::
       C k v b
    => V.Vector (k, v, Maybe b) -> Var h (WrapTable h IO k v b)
    -> Act' h ()
  Deletes ::
       C k v b
    => V.Vector k -> Var h (WrapTable h IO k v b)
    -> Act' h ()
  Mupserts ::
       C k v b
    => V.Vector (k, v) -> Var h (WrapTable h IO k v b)
    -> Act' h ()
  -- Blobs
  RetrieveBlobs ::
       B b
    => Var h (V.Vector (WrapBlobRef h IO b))
    -> Act' h (V.Vector (WrapBlob b))
  -- Snapshots
  SaveSnapshot ::
       C k v b
    => Maybe SilentCorruption
    -> R.SnapshotName
    -> R.SnapshotLabel
    -> Var h (WrapTable h IO k v b)
    -> Act' h ()
  OpenTableFromSnapshot ::
       C k v b
    => {-# UNPACK #-} !(PrettyProxy (k, v, b))
    -> R.SnapshotName
    -> R.SnapshotLabel
    -> Act' h (WrapTable h IO k v b)
  DeleteSnapshot :: R.SnapshotName -> Act' h ()
  ListSnapshots  :: Act' h [R.SnapshotName]
  -- Duplicate tables
  Duplicate ::
       C k v b
    => Var h (WrapTable h IO k v b)
    -> Act' h (WrapTable h IO k v b)
  -- Table union
  Union ::
       C k v b
    => Var h (WrapTable h IO k v b)
    -> Var h (WrapTable h IO k v b)
    -> Act' h (WrapTable h IO k v b)
  Unions ::
       C k v b
    => NonEmpty (Var h (WrapTable h IO k v b))
    -> Act' h (WrapTable h IO k v b)
  RemainingUnionDebt ::
       C k v b
    => Var h (WrapTable h IO k v b)
    -> Act' h R.UnionDebt
  SupplyUnionCredits ::
       C k v b
    => Var h (WrapTable h IO k v b)
    -> R.UnionCredits
    -> Act' h R.UnionCredits
  -- | Alternative version of 'SupplyUnionCredits' that supplies a portion of
  -- the table's current union debt as union credits.
  --
  -- 'SupplyUnionCredits' gets no information about union debt, so the union
  -- credits we generate are arbitrary, and it would require precarious, manual
  -- tuning to make sure the debt is ever paid off by an action sequence.
  -- 'SupplyUnionCredits' supplies a portion (if not all) of the current debt,
  -- so that unions are more likely to finish during a sequence of actions.
  SupplyPortionOfDebt ::
       C k v b
    => Var h (WrapTable h IO k v b)
    -> Portion
    -> Act' h R.UnionCredits

portionOf :: Portion -> R.UnionDebt -> R.UnionCredits
portionOf (Portion denominator) (R.UnionDebt debt)
  | denominator <= 0 = error "portion: denominator should be positive"
  | debt < 0 = error "portion: debt should be non-negative"
  | otherwise = R.UnionCredits (debt `div` denominator)

newtype Portion = Portion Int -- ^ Denominator: should be non-negative
  deriving stock (Show, Eq)

deriving stock instance Show (Class.TableConfig h)
                     => Show (Action' h a)

instance ( Eq (Class.TableConfig h)
         , Typeable h
         ) => Eq (Action' h a) where
  x == y = go x y
    where
      go :: Action' h a -> Action' h a -> Bool
      go
        (New (PrettyProxy :: PrettyProxy kvb) conf1)
        (New (PrettyProxy :: PrettyProxy kvb) conf2) =
          conf1 == conf2
      go (Close var1) (Close var2) =
        Just var1 == cast var2
      go (Lookups ks1 var1) (Lookups ks2 var2) =
        Just ks1 == cast ks2 && Just var1 == cast var2
      go (RangeLookup range1 var1) (RangeLookup range2 var2) =
        range1 == range2 && var1 == var2
      go (NewCursor k1 var1) (NewCursor k2 var2) =
        k1 == k2 && var1 == var2
      go (CloseCursor var1) (CloseCursor var2) =
        Just var1 == cast var2
      go (ReadCursor n1 var1) (ReadCursor n2 var2) =
        n1 == n2 && var1 == var2
      go (Updates ups1 var1) (Updates ups2 var2) =
        Just ups1 == cast ups2 && Just var1 == cast var2
      go (Inserts inss1 var1) (Inserts inss2 var2) =
        Just inss1 == cast inss2 && Just var1 == cast var2
      go (Deletes ks1 var1) (Deletes ks2 var2) =
        Just ks1 == cast ks2 && Just var1 == cast var2
      go (Mupserts mups1 var1) (Mupserts mups2 var2) =
        Just mups1 == cast mups2 && Just var1 == cast var2
      go (RetrieveBlobs vars1) (RetrieveBlobs vars2) =
        Just vars1 == cast vars2
      go (SaveSnapshot mcorr1 name1 label1 var1) (SaveSnapshot mcorr2 name2 label2 var2) =
        mcorr1 == mcorr2 && name1 == name2 && label1 == label2 && Just var1 == cast var2
      go
        (OpenTableFromSnapshot (PrettyProxy :: PrettyProxy kvb) name1 label1)
        (OpenTableFromSnapshot (PrettyProxy :: PrettyProxy kvb) name2 label2) =
          label1 == label2 && name1 == name2
      go (DeleteSnapshot name1) (DeleteSnapshot name2) =
        name1 == name2
      go ListSnapshots ListSnapshots =
        True
      go (Duplicate var1) (Duplicate var2) =
        Just var1 == cast var2
      go (Union var1_1 var1_2) (Union var2_1 var2_2) =
        Just var1_1 == cast var2_1 && Just var1_2 == cast var2_2
      go (Unions vars1) (Unions vars2) =
        Just vars1 == cast vars2
      go (RemainingUnionDebt var1) (RemainingUnionDebt var2) =
        Just var1 == cast var2
      go (SupplyUnionCredits var1 credits1) (SupplyUnionCredits var2 credits2) =
        Just var1 == cast var2 && credits1 == credits2
      go (SupplyPortionOfDebt var1 portion1) (SupplyPortionOfDebt var2 portion2) =
        Just var1 == cast var2 && portion1 == portion2
      go _ _ = False

      _coveredAllCases :: Action' h a -> ()
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
          Mupserts{} -> ()
          RetrieveBlobs{} -> ()
          SaveSnapshot{} -> ()
          OpenTableFromSnapshot{} -> ()
          DeleteSnapshot{} -> ()
          ListSnapshots{} -> ()
          Duplicate{} -> ()
          Union{} -> ()
          Unions{} -> ()
          RemainingUnionDebt{} -> ()
          SupplyUnionCredits{} -> ()
          SupplyPortionOfDebt{} -> ()

-- | This is not a fully lawful instance, because it uses 'approximateEqStream'.
instance Eq a => Eq (Stream a) where
  (==) = approximateEqStream

-- | This is not a fully lawful instance, because it uses 'approximateEqStream'.
deriving stock instance Eq Errors
deriving stock instance Eq FSSim.Partial
deriving stock instance Eq FSSim.PutCorruption
deriving stock instance Eq FSSim.Blob

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
    -- handle-like
    MTable ::
      Model.Table k v b ->
      Val h (WrapTable h IO k v b)
    MCursor :: Model.Cursor k v b -> Val h (WrapCursor h IO k v b)
    MBlobRef ::
      (Class.C_ b) =>
      Model.BlobRef b ->
      Val h (WrapBlobRef h IO b)
    -- values
    MLookupResult ::
      (Class.C_ v, Class.C_ b) =>
      LookupResult v (Val h (WrapBlobRef h IO b)) ->
      Val h (LookupResult v (WrapBlobRef h IO b))
    MEntry ::
      (Class.C k v b) =>
      Entry k v (Val h (WrapBlobRef h IO b)) ->
      Val h (Entry k v (WrapBlobRef h IO b))
    MBlob ::
      (Show b, Typeable b, Eq b) =>
      WrapBlob b ->
      Val h (WrapBlob b)
    MSnapshotName :: R.SnapshotName -> Val h R.SnapshotName
    MUnionDebt :: R.UnionDebt -> Val h R.UnionDebt
    MUnionCredits :: R.UnionCredits -> Val h R.UnionCredits
    MUnionCreditsPortion :: R.UnionCredits -> Val h R.UnionCredits
    MErr :: Model.Err -> Val h Model.Err
    -- combinators
    MUnit :: () -> Val h ()
    MPair :: (Val h a, Val h b) -> Val h (a, b)
    MEither :: Either (Val h a) (Val h b) -> Val h (Either a b)
    MList :: [Val h a] -> Val h [a]
    MVector :: V.Vector (Val h a) -> Val h (V.Vector a)

  data instance Observable (ModelState h) a where
    -- handle-like (opaque)
    OTable :: Obs h (WrapTable h IO k v b)
    OCursor :: Obs h (WrapCursor h IO k v b)
    OBlobRef :: Obs h (WrapBlobRef h IO b)
    -- values
    OLookupResult ::
      (Class.C_ v, Class.C_ b) =>
      LookupResult v (Obs h (WrapBlobRef h IO b)) ->
      Obs h (LookupResult v (WrapBlobRef h IO b))
    OEntry ::
      (Class.C k v b) =>
      Entry k v (Obs h (WrapBlobRef h IO b)) ->
      Obs h (Entry k v (WrapBlobRef h IO b))
    OBlob ::
      (Show b, Typeable b, Eq b) =>
      WrapBlob b ->
      Obs h (WrapBlob b)
    OUnionCredits :: R.UnionCredits -> Obs h R.UnionCredits
    OUnionCreditsPortion :: R.UnionCredits -> Obs h R.UnionCredits
    OId :: (Show a, Typeable a, Eq a) => a -> Obs h a
    -- combinators
    OPair :: (Obs h a, Obs h b) -> Obs h (a, b)
    OEither :: Either (Obs h a) (Obs h b) -> Obs h (Either a b)
    OList :: [Obs h a] -> Obs h [a]
    OVector :: V.Vector (Obs h a) -> Obs h (V.Vector a)

  observeModel :: Val h a -> Obs h a
  observeModel = \case
      MTable _               -> OTable
      MCursor _              -> OCursor
      MBlobRef _             -> OBlobRef
      MLookupResult x        -> OLookupResult $ fmap observeModel x
      MEntry x               -> OEntry $ fmap observeModel x
      MSnapshotName x        -> OId x
      MBlob x                -> OBlob x
      MUnionDebt x           -> OId x
      MUnionCredits x        -> OUnionCredits x
      MUnionCreditsPortion x -> OUnionCreditsPortion x
      MErr x                 -> OId x
      MUnit x                -> OId x
      MPair x                -> OPair $ bimap observeModel observeModel x
      MEither x              -> OEither $ bimap observeModel observeModel x
      MList x                -> OList $ map observeModel x
      MVector x              -> OVector $ V.map observeModel x

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
  usedVars (Action _ action') = case action' of
      New _ _                        -> []
      Close tableVar                 -> [SomeGVar tableVar]
      Lookups _ tableVar             -> [SomeGVar tableVar]
      RangeLookup _ tableVar         -> [SomeGVar tableVar]
      NewCursor _ tableVar           -> [SomeGVar tableVar]
      CloseCursor cursorVar          -> [SomeGVar cursorVar]
      ReadCursor _ cursorVar         -> [SomeGVar cursorVar]
      Updates _ tableVar             -> [SomeGVar tableVar]
      Inserts _ tableVar             -> [SomeGVar tableVar]
      Deletes _ tableVar             -> [SomeGVar tableVar]
      Mupserts _ tableVar            -> [SomeGVar tableVar]
      RetrieveBlobs blobsVar         -> [SomeGVar blobsVar]
      SaveSnapshot   _ _ _ tableVar  -> [SomeGVar tableVar]
      OpenTableFromSnapshot{}        -> []
      DeleteSnapshot _               -> []
      ListSnapshots                  -> []
      Duplicate tableVar             -> [SomeGVar tableVar]
      Union table1Var table2Var      -> [SomeGVar table1Var, SomeGVar table2Var]
      Unions tableVars               -> [SomeGVar tableVar | tableVar <- NE.toList tableVars]
      RemainingUnionDebt tableVar    -> [SomeGVar tableVar]
      SupplyUnionCredits tableVar _  -> [SomeGVar tableVar]
      SupplyPortionOfDebt tableVar _ -> [SomeGVar tableVar]

  arbitraryWithVars ::
       ModelVarContext (ModelState h)
    -> ModelState h
    -> Gen (Any (LockstepAction (ModelState h)))
  arbitraryWithVars ctx st =
    QC.scale (max 100) $
    arbitraryActionWithVars (Proxy @(Key, Value, Blob)) keyValueBlobLabel ctx st

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
        | Just Model.ErrBlobRefInvalid <- cast y ->
            flip all vec $ \case
              OBlob (WrapBlob _) -> True
              _ -> False

      -- When disk faults are injected, the model only knows /something/ went
      -- wrong, but the SUT can throw much more specific errors. We allow this.
      --
      -- See also 'Model.runModelMWithInjectedErrors' and
      -- 'runRealWithInjectedErrors'.
      (OEither (Left (OId lhs)), OEither (Left (OId rhs)))
        | Just (e :: Model.Err) <- cast lhs
        , not $ Model.isOther e
        , Just (Model.ErrDiskFault _) <- cast rhs
        -> True

      -- When snapshots are corrupted, the model only knows that the snapshot
      -- was corrupted, but not how, but the SUT can throw much more specific
      -- errors. We allow this.
      (OEither (Left (OId lhs)), OEither (Left (OId rhs)))
        | Just (e :: Model.Err) <- cast lhs
        , not $ Model.isOther e
        , Just (Model.ErrSnapshotCorrupted _) <- cast rhs
        -> True

      -- RemainingUnionDebt
      --
      -- Debt in the model is always 0, while debt in the real implementation
      -- may be larger than 0.
      (OEither (Right (OId lhs)), OEither (Right (OId rhs)))
        | Just (lhsDebt :: R.UnionDebt) <- cast lhs
        , Just (rhsDebt :: R.UnionDebt) <- cast rhs
        -> lhsDebt >= R.UnionDebt 0 && rhsDebt == R.UnionDebt 0

      -- SupplyUnionCredits
      --
      -- In the model, all supplied union credits are returned as leftovers,
      -- whereas the real implementation may use up some union credits.
      (OEither (Right (OUnionCredits lhsLeftovers)), OEither (Right (OUnionCredits rhsLeftovers)))
        -> lhsLeftovers <= rhsLeftovers

      -- SupplyPortionOfDebt
      --
      -- In the model, a portion of the debt is always 0, whereas in the real
      -- implementation the portion of the debt is non-negative.
      (OEither (Right (OUnionCreditsPortion lhsLeftovers)), OEither (Right (OUnionCreditsPortion rhsLeftovers)))
        -> lhsLeftovers >= rhsLeftovers

      -- default equalities
      (OTable, OTable) -> True
      (OCursor, OCursor) -> True
      (OBlobRef, OBlobRef) -> True
      (OLookupResult x, OLookupResult y) -> x == y
      (OEntry x, OEntry y) -> x == y
      (OBlob x, OBlob y) -> x == y
      (OUnionCredits x, OUnionCredits y) -> x == y
      (OUnionCreditsPortion x, OUnionCreditsPortion y) -> x == y
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
          OEntry{} -> ()
          OBlob{} -> ()
          OUnionCredits{} -> ()
          OUnionCreditsPortion{} -> ()
          OId{} -> ()
          OPair{} -> ()
          OEither{} -> ()
          OList{} -> ()
          OVector{} -> ()

{-------------------------------------------------------------------------------
  Real monad
-------------------------------------------------------------------------------}

type RealMonad h m = ReaderT (RealEnv h m) m

-- | An environment for implementations @h@ of the public API to run actions in
-- (see 'perform', 'runIO', 'runIOSim').
data RealEnv h m = RealEnv {
    -- | The session to run actions in.
    envSession            :: !(Class.Session h m)
    -- | Error handlers to convert thrown exceptions into pure error values.
    --
    -- Uncaught exceptions make the tests fail, so some handlers should be
    -- provided that catch the exceptions and turn them into pure error values
    -- if possible. The state machine infrastructure can then also compare the
    -- error values obtained from running @h@ to the error values obtained by
    -- running the model.
  , envHandlers           :: [Handler m (Maybe Model.Err)]
    -- | A variable holding simulated disk faults,
    --
    -- This variable shared with the simulated file system (if in use). This
    -- variable can be used to enable/disable errors locally, for example on a
    -- per-action basis.
  , envErrors             :: !(StrictTVar m Errors)
    -- | A variable holding a log of simulated disk faults.
    --
    -- Errors that are injected into the simulated file system using 'envErrors'
    -- are logged here.
  , envErrorsLog          :: !(StrictTVar m ErrorsLog)
    -- | The results of fault injection
  , envInjectFaultResults :: !(MutVar (PrimState m) [InjectFaultResult])
  }

data InjectFaultResult =
    -- | No faults were injected.
    InjectFaultNone
      String -- ^ Action name
    -- | Faults were injected, but the action accidentally succeeded, so the
    -- action had to be rolled back
  | InjectFaultAccidentalSuccess
      String -- ^ Action name
    -- | Faults were injected, which made the action fail with an error.
  | InjectFaultInducedError
      String -- ^ Action name
  deriving stock Show

{-------------------------------------------------------------------------------
  RunLockstep
-------------------------------------------------------------------------------}

instance ( Eq (Class.TableConfig h)
         , Class.IsTable h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
         ) => RunLockstep (ModelState h) (RealMonad h IO) where
  observeReal ::
       Proxy (RealMonad h IO)
    -> LockstepAction (ModelState h) a
    -> Realized (RealMonad h IO) a
    -> Obs h a
  observeReal _proxy (Action _ action') result = case action' of
      New{}                   -> OEither $ bimap OId (const OTable) result
      Close{}                 -> OEither $ bimap OId OId result
      Lookups{}               -> OEither $
          bimap OId (OVector . fmap (OLookupResult . fmap (const OBlobRef))) result
      RangeLookup{}           -> OEither $
          bimap OId (OVector . fmap (OEntry . fmap (const OBlobRef))) result
      NewCursor{}             -> OEither $ bimap OId (const OCursor) result
      CloseCursor{}           -> OEither $ bimap OId OId result
      ReadCursor{}            -> OEither $
          bimap OId (OVector . fmap (OEntry . fmap (const OBlobRef))) result
      Updates{}               -> OEither $ bimap OId OId result
      Inserts{}               -> OEither $ bimap OId OId result
      Deletes{}               -> OEither $ bimap OId OId result
      Mupserts{}              -> OEither $ bimap OId OId result
      RetrieveBlobs{}         -> OEither $ bimap OId (OVector . fmap OBlob) result
      SaveSnapshot{}          -> OEither $ bimap OId OId result
      OpenTableFromSnapshot{} -> OEither $ bimap OId (const OTable) result
      DeleteSnapshot{}        -> OEither $ bimap OId OId result
      ListSnapshots{}         -> OEither $ bimap OId (OList . fmap OId) result
      Duplicate{}             -> OEither $ bimap OId (const OTable) result
      Union{}                 -> OEither $ bimap OId (const OTable) result
      Unions{}                -> OEither $ bimap OId (const OTable) result
      RemainingUnionDebt{}    -> OEither $ bimap OId OId result
      SupplyUnionCredits{}    -> OEither $ bimap OId OUnionCredits result
      SupplyPortionOfDebt{}   -> OEither $ bimap OId OUnionCreditsPortion result

  showRealResponse ::
       Proxy (RealMonad h IO)
    -> LockstepAction (ModelState h) a
    -> Maybe (Dict (Show (Realized (RealMonad h IO) a)))
  showRealResponse _ (Action _ action') = case action' of
      New{}                   -> Nothing
      Close{}                 -> Just Dict
      Lookups{}               -> Nothing
      RangeLookup{}           -> Nothing
      NewCursor{}             -> Nothing
      CloseCursor{}           -> Just Dict
      ReadCursor{}            -> Nothing
      Updates{}               -> Just Dict
      Inserts{}               -> Just Dict
      Deletes{}               -> Just Dict
      Mupserts{}              -> Just Dict
      RetrieveBlobs{}         -> Just Dict
      SaveSnapshot{}          -> Just Dict
      OpenTableFromSnapshot{} -> Nothing
      DeleteSnapshot{}        -> Just Dict
      ListSnapshots{}         -> Just Dict
      Duplicate{}             -> Nothing
      Union{}                 -> Nothing
      Unions{}                -> Nothing
      RemainingUnionDebt{}    -> Just Dict
      SupplyUnionCredits{}    -> Just Dict
      SupplyPortionOfDebt{}   -> Just Dict

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
  observeReal _proxy (Action _ action') result = case action' of
      New{}                   -> OEither $ bimap OId (const OTable) result
      Close{}                 -> OEither $ bimap OId OId result
      Lookups{}               -> OEither $
          bimap OId (OVector . fmap (OLookupResult . fmap (const OBlobRef))) result
      RangeLookup{}           -> OEither $
          bimap OId (OVector . fmap (OEntry . fmap (const OBlobRef))) result
      NewCursor{}             -> OEither $ bimap OId (const OCursor) result
      CloseCursor{}           -> OEither $ bimap OId OId result
      ReadCursor{}            -> OEither $
          bimap OId (OVector . fmap (OEntry . fmap (const OBlobRef))) result
      Updates{}               -> OEither $ bimap OId OId result
      Inserts{}               -> OEither $ bimap OId OId result
      Deletes{}               -> OEither $ bimap OId OId result
      Mupserts{}              -> OEither $ bimap OId OId result
      RetrieveBlobs{}         -> OEither $ bimap OId (OVector . fmap OBlob) result
      SaveSnapshot{}          -> OEither $ bimap OId OId result
      OpenTableFromSnapshot{} -> OEither $ bimap OId (const OTable) result
      DeleteSnapshot{}        -> OEither $ bimap OId OId result
      ListSnapshots{}         -> OEither $ bimap OId (OList . fmap OId) result
      Duplicate{}             -> OEither $ bimap OId (const OTable) result
      Union{}                 -> OEither $ bimap OId (const OTable) result
      Unions{}                -> OEither $ bimap OId (const OTable) result
      RemainingUnionDebt{}    -> OEither $ bimap OId OId result
      SupplyUnionCredits{}    -> OEither $ bimap OId OUnionCredits result
      SupplyPortionOfDebt{}   -> OEither $ bimap OId OUnionCreditsPortion result

  showRealResponse ::
       Proxy (RealMonad h (IOSim s))
    -> LockstepAction (ModelState h) a
    -> Maybe (Dict (Show (Realized (RealMonad h (IOSim s)) a)))
  showRealResponse _ (Action _ action') = case action' of
      New{}                   -> Nothing
      Close{}                 -> Just Dict
      Lookups{}               -> Nothing
      RangeLookup{}           -> Nothing
      NewCursor{}             -> Nothing
      CloseCursor{}           -> Just Dict
      ReadCursor{}            -> Nothing
      Updates{}               -> Just Dict
      Inserts{}               -> Just Dict
      Deletes{}               -> Just Dict
      Mupserts{}              -> Just Dict
      RetrieveBlobs{}         -> Just Dict
      SaveSnapshot{}          -> Just Dict
      OpenTableFromSnapshot{} -> Nothing
      DeleteSnapshot{}        -> Just Dict
      ListSnapshots{}         -> Just Dict
      Duplicate{}             -> Nothing
      Union{}                 -> Nothing
      Unions{}                -> Nothing
      RemainingUnionDebt{}    -> Just Dict
      SupplyUnionCredits{}    -> Just Dict
      SupplyPortionOfDebt{}   -> Just Dict

{-------------------------------------------------------------------------------
  RunModel
-------------------------------------------------------------------------------}

instance ( Eq (Class.TableConfig h)
         , Class.IsTable h
         , Show (Class.TableConfig h)
         , Arbitrary (Class.TableConfig h)
         , Typeable h
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

-- TODO: there are a bunch of TODO(err) in 'runModel' on the last argument to
-- 'Model.runModelMWithInjectedErrors'. This last argument defines how the model
-- should respond to injected errors. Since we don't generate injected errors
-- for most of these actions yet, they are left open. We will fill these in as
-- we start generating injected errors for these actions and testing with them.

runModel ::
     ModelLookUp (ModelState h)
  -> LockstepAction (ModelState h) a
  -> Model.Model -> (Val h a, Model.Model)
runModel lookUp (Action merrs action') = case action' of
    New _ _cfg ->
      wrap MTable
      . Model.runModelMWithInjectedErrors merrs
          (Model.new Model.TableConfig)
          (pure ()) -- TODO(err)
    Close tableVar ->
      wrap MUnit
      . Model.runModelMWithInjectedErrors merrs
          (Model.close (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    Lookups ks tableVar ->
      wrap (MVector . fmap (MLookupResult . fmap MBlobRef))
      . Model.runModelMWithInjectedErrors merrs
          (Model.lookups ks (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    RangeLookup range tableVar ->
      wrap (MVector . fmap (MEntry . fmap MBlobRef))
      . Model.runModelMWithInjectedErrors merrs
          (Model.rangeLookup range (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    NewCursor offset tableVar ->
      wrap MCursor
      . Model.runModelMWithInjectedErrors merrs
          (Model.newCursor offset (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    CloseCursor cursorVar ->
      wrap MUnit
      . Model.runModelMWithInjectedErrors merrs
          (Model.closeCursor (getCursor $ lookUp cursorVar))
          (pure ()) -- TODO(err)
    ReadCursor n cursorVar ->
      wrap (MVector . fmap (MEntry . fmap MBlobRef))
      . Model.runModelMWithInjectedErrors merrs
          (Model.readCursor n (getCursor $ lookUp cursorVar))
          (pure ()) -- TODO(err)
    Updates kups tableVar ->
      wrap MUnit
      . Model.runModelMWithInjectedErrors merrs
          (Model.updates Model.getResolve kups (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    Inserts kins tableVar ->
      wrap MUnit
      . Model.runModelMWithInjectedErrors merrs
          (Model.inserts Model.getResolve kins (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    Deletes kdels tableVar ->
      wrap MUnit
      . Model.runModelMWithInjectedErrors merrs
          (Model.deletes Model.getResolve kdels (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    Mupserts kmups tableVar ->
      wrap MUnit
      . Model.runModelMWithInjectedErrors merrs
          (Model.mupserts Model.getResolve kmups (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    RetrieveBlobs blobsVar ->
      wrap (MVector . fmap (MBlob . WrapBlob))
      . Model.runModelMWithInjectedErrors merrs
          (Model.retrieveBlobs (getBlobRefs . lookUp $ blobsVar))
          (pure ()) -- TODO(err)
    SaveSnapshot mcorr name label tableVar ->
      wrap MUnit
        . Model.runModelMWithInjectedErrors merrs
            (do Model.saveSnapshot name label (getTable $ lookUp tableVar)
                forM_ mcorr $ \_ -> Model.corruptSnapshot name)
            (pure ()) -- TODO(err)
    OpenTableFromSnapshot _ name label ->
      wrap MTable
      . Model.runModelMWithInjectedErrors merrs
          (Model.openTableFromSnapshot name label)
          (pure ())
    DeleteSnapshot name ->
      wrap MUnit
      . Model.runModelMWithInjectedErrors merrs
          (Model.deleteSnapshot name)
          (pure ()) -- TODO(err)
    ListSnapshots ->
      wrap (MList . fmap MSnapshotName)
      . Model.runModelMWithInjectedErrors merrs
          Model.listSnapshots
          (pure ()) -- TODO(err)
    Duplicate tableVar ->
      wrap MTable
      . Model.runModelMWithInjectedErrors merrs
          (Model.duplicate (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    Union table1Var table2Var ->
      wrap MTable
      . Model.runModelMWithInjectedErrors merrs
          (Model.union Model.getResolve (getTable $ lookUp table1Var) (getTable $ lookUp table2Var))
          (pure ()) -- TODO(err)
    Unions tableVars ->
      wrap MTable
      . Model.runModelMWithInjectedErrors merrs
          (Model.unions Model.getResolve (fmap (getTable . lookUp) tableVars))
          (pure ()) -- TODO(err)
    RemainingUnionDebt tableVar ->
      wrap MUnionDebt
      . Model.runModelMWithInjectedErrors merrs
          (Model.remainingUnionDebt (getTable $ lookUp tableVar))
          (pure ()) -- TODO(err)
    SupplyUnionCredits tableVar credits ->
      wrap MUnionCredits
      . Model.runModelMWithInjectedErrors merrs
          (Model.supplyUnionCredits (getTable $ lookUp tableVar) credits)
          (pure ()) -- TODO(err)
    SupplyPortionOfDebt tableVar portion ->
      wrap MUnionCreditsPortion
      . Model.runModelMWithInjectedErrors merrs
          (do let table = getTable $ lookUp tableVar
              Model.supplyPortionOfDebt table portion
          )
          (pure ()) -- TODO(err)
  where
    getTable ::
         ModelValue (ModelState h) (WrapTable h IO k v b)
      -> Model.Table k v b
    getTable (MTable t) = t

    getCursor ::
         ModelValue (ModelState h) (WrapCursor h IO k v b)
      -> Model.Cursor k v b
    getCursor (MCursor t) = t

    getBlobRefs :: ModelValue (ModelState h) (V.Vector (WrapBlobRef h IO b)) -> V.Vector (Model.BlobRef b)
    getBlobRefs (MVector brs) = fmap (\(MBlobRef br) -> br) brs

wrap ::
     (a -> Val h b)
  -> (Either Model.Err a, Model.Model)
  -> (Val h (Either Model.Err b), Model.Model)
wrap f = first (MEither . bimap MErr f)

{-------------------------------------------------------------------------------
  Interpreters for @'IOLike' m@
-------------------------------------------------------------------------------}

-- TODO: there are a bunch of TODO(err) in 'runIO' and 'runIOSim' below on the
-- last argument to 'Model.runModelMWithInjectedErrors'. This last argument
-- defines how the SUT should recover from actions that accidentally succeeded
-- in the presence of disk faults. Since we don't generate injected errors for
-- most of these actions yet, they are left open. We will fill these in as we
-- start generating injected errors for these actions and testing with them.

runIO ::
     forall a h. Class.IsTable h
  => LockstepAction (ModelState h) a
  -> LookUp (RealMonad h IO)
  -> RealMonad h IO (Realized (RealMonad h IO) a)
runIO action lookUp = ReaderT $ \ !env -> do
    aux env action
  where
    aux ::
         RealEnv h IO
      -> LockstepAction (ModelState h) a
      -> IO (Realized IO a)
    aux env (Action merrs action') = case action' of
        New _ cfg ->
          runRealWithInjectedErrors "New" env merrs
            (WrapTable <$> Class.new session cfg)
            (\_ -> pure ()) -- TODO(err)
        Close tableVar ->
          runRealWithInjectedErrors "Close" env merrs
            (Class.close (unwrapTable $ lookUp' tableVar))
            (\_ -> pure ()) -- TODO(err)
        Lookups ks tableVar ->
          runRealWithInjectedErrors "Lookups" env merrs
            (fmap (fmap WrapBlobRef) <$> Class.lookups (unwrapTable $ lookUp' tableVar) ks)
            (\_ -> pure ()) -- TODO(err)
        RangeLookup range tableVar ->
          runRealWithInjectedErrors "RangeLookup" env merrs
            (fmap (fmap WrapBlobRef) <$> Class.rangeLookup (unwrapTable $ lookUp' tableVar) range)
            (\_ -> pure ()) -- TODO(err)
        NewCursor offset tableVar ->
          runRealWithInjectedErrors "NewCursor" env merrs
            (WrapCursor <$> Class.newCursor offset (unwrapTable $ lookUp' tableVar))
            (\_ -> pure ()) -- TODO(err)
        CloseCursor cursorVar ->
          runRealWithInjectedErrors "CloseCursor" env merrs
            (Class.closeCursor (Proxy @h) (unwrapCursor $ lookUp' cursorVar))
            (\_ -> pure ()) -- TODO(err)
        ReadCursor n cursorVar ->
          runRealWithInjectedErrors "ReadCursor" env merrs
            (fmap (fmap WrapBlobRef) <$> Class.readCursor (Proxy @h) n (unwrapCursor $ lookUp' cursorVar))
            (\_ -> pure ()) -- TODO(err)
        Updates kups tableVar ->
          runRealWithInjectedErrors "Updates" env merrs
            (Class.updates (unwrapTable $ lookUp' tableVar) kups)
            (\_ -> pure ()) -- TODO(err)
        Inserts kins tableVar ->
          runRealWithInjectedErrors "Inserts" env merrs
            (Class.inserts (unwrapTable $ lookUp' tableVar) kins)
            (\_ -> pure ()) -- TODO(err)
        Deletes kdels tableVar ->
          runRealWithInjectedErrors "Deletes" env merrs
            (Class.deletes (unwrapTable $ lookUp' tableVar) kdels)
            (\_ -> pure ()) -- TODO(err)
        Mupserts kmups tableVar ->
          runRealWithInjectedErrors "Mupserts" env merrs
            (Class.mupserts (unwrapTable $ lookUp' tableVar) kmups)
            (\_ -> pure ()) -- TODO(err)
        RetrieveBlobs blobRefsVar ->
          runRealWithInjectedErrors "RetrieveBlobs" env merrs
            (fmap WrapBlob <$> Class.retrieveBlobs (Proxy @h) session (unwrapBlobRef <$> lookUp' blobRefsVar))
            (\_ -> pure ()) -- TODO(err)
        SaveSnapshot mcorr name label tableVar ->
          let table = unwrapTable $ lookUp' tableVar in
          runRealWithInjectedErrors "SaveSnapshot" env merrs
            (do Class.saveSnapshot name label table
                forM_ mcorr $ \corr -> Class.corruptSnapshot (bitChoice corr) name table)
            (\() -> Class.deleteSnapshot session name) -- TODO(err)
        OpenTableFromSnapshot _ name label ->
          runRealWithInjectedErrors "OpenTableFromSnapshot" env merrs
            (WrapTable <$> Class.openTableFromSnapshot session name label)
            (\(WrapTable t) -> Class.close t)
        DeleteSnapshot name ->
          runRealWithInjectedErrors "DeleteSnapshot" env merrs
            (Class.deleteSnapshot session name)
            (\_ -> pure ()) -- TODO(err)
        ListSnapshots ->
          runRealWithInjectedErrors "ListSnapshots" env merrs
            (Class.listSnapshots session)
            (\_ -> pure ()) -- TODO(err)
        Duplicate tableVar ->
          runRealWithInjectedErrors "Duplicate" env merrs
            (WrapTable <$> Class.duplicate (unwrapTable $ lookUp' tableVar))
            (\_ -> pure ()) -- TODO(err)
        Union table1Var table2Var ->
          runRealWithInjectedErrors "Union" env merrs
            (WrapTable <$> Class.union (unwrapTable $ lookUp' table1Var) (unwrapTable $ lookUp' table2Var))
            (\_ -> pure ()) -- TODO(err)
        Unions tableVars ->
          runRealWithInjectedErrors "Unions" env merrs
            (WrapTable <$> Class.unions (fmap (unwrapTable . lookUp') tableVars))
            (\_ -> pure ()) -- TODO(err)
        RemainingUnionDebt tableVar ->
          runRealWithInjectedErrors "RemainingUnionDebt" env merrs
            (Class.remainingUnionDebt (unwrapTable $ lookUp' tableVar))
            (\_ -> pure ()) -- TODO(err)
        SupplyUnionCredits tableVar credits ->
          runRealWithInjectedErrors "SupplyUnionCredits" env merrs
            (Class.supplyUnionCredits (unwrapTable $ lookUp' tableVar) credits)
            (\_ -> pure ()) -- TODO(err)
        SupplyPortionOfDebt tableVar portion ->
          runRealWithInjectedErrors "SupplyPortionOfDebt" env merrs
              (do let table = unwrapTable $ lookUp' tableVar
                  debt <- Class.remainingUnionDebt table
                  Class.supplyUnionCredits table (portion `portionOf` debt))
              (\_ -> pure ()) -- TODO(err)
      where
        session = envSession env

    lookUp' :: Var h x -> Realized IO x
    lookUp' = lookUpGVar (Proxy @(RealMonad h IO)) lookUp

runIOSim ::
     forall s a h. Class.IsTable h
  => LockstepAction (ModelState h) a
  -> LookUp (RealMonad h (IOSim s))
  -> RealMonad h (IOSim s) (Realized (RealMonad h (IOSim s)) a)
runIOSim action lookUp = ReaderT $ \ !env -> do
    aux env action
  where
    aux ::
         RealEnv h (IOSim s)
      -> LockstepAction (ModelState h) a
      -> IOSim s (Realized (IOSim s) a)
    aux env (Action merrs action') = case action' of
        New _ cfg ->
          runRealWithInjectedErrors "New" env merrs
            (WrapTable <$> Class.new session cfg)
            (\_ -> pure ()) -- TODO(err)
        Close tableVar ->
          runRealWithInjectedErrors "Close" env merrs
            (Class.close (unwrapTable $ lookUp' tableVar))
            (\_ -> pure ()) -- TODO(err)
        Lookups ks tableVar ->
          runRealWithInjectedErrors "Lookups" env merrs
            (fmap (fmap WrapBlobRef) <$> Class.lookups (unwrapTable $ lookUp' tableVar) ks)
            (\_ -> pure ()) -- TODO(err)
        RangeLookup range tableVar ->
          runRealWithInjectedErrors "RangeLookup" env merrs
            (fmap (fmap WrapBlobRef) <$> Class.rangeLookup (unwrapTable $ lookUp' tableVar) range)
            (\_ -> pure ()) -- TODO(err)
        NewCursor offset tableVar ->
          runRealWithInjectedErrors "NewCursor" env merrs
            (WrapCursor <$> Class.newCursor offset (unwrapTable $ lookUp' tableVar))
            (\_ -> pure ()) -- TODO(err)
        CloseCursor cursorVar ->
          runRealWithInjectedErrors "CloseCursor" env merrs
            (Class.closeCursor (Proxy @h) (unwrapCursor $ lookUp' cursorVar))
            (\_ -> pure ()) -- TODO(err)
        ReadCursor n cursorVar ->
          runRealWithInjectedErrors "ReadCursor" env merrs
            (fmap (fmap WrapBlobRef) <$> Class.readCursor (Proxy @h) n (unwrapCursor $ lookUp' cursorVar))
            (\_ -> pure ()) -- TODO(err)
        Updates kups tableVar ->
          runRealWithInjectedErrors "Updates" env merrs
            (Class.updates (unwrapTable $ lookUp' tableVar) kups)
            (\_ -> pure ()) -- TODO(err)
        Inserts kins tableVar ->
          runRealWithInjectedErrors "Inserts" env merrs
            (Class.inserts (unwrapTable $ lookUp' tableVar) kins)
            (\_ -> pure ()) -- TODO(err)
        Deletes kdels tableVar ->
          runRealWithInjectedErrors "Deletes" env merrs
            (Class.deletes (unwrapTable $ lookUp' tableVar) kdels)
            (\_ -> pure ()) -- TODO(err)
        Mupserts kmups tableVar ->
          runRealWithInjectedErrors "Mupserts" env merrs
            (Class.mupserts (unwrapTable $ lookUp' tableVar) kmups)
            (\_ -> pure ()) -- TODO(err)
        RetrieveBlobs blobRefsVar ->
          runRealWithInjectedErrors "RetrieveBlobs" env merrs
            (fmap WrapBlob <$> Class.retrieveBlobs (Proxy @h) session (unwrapBlobRef <$> lookUp' blobRefsVar))
            (\_ -> pure ()) -- TODO(err)
        SaveSnapshot mcorr name label tableVar ->
          let table = unwrapTable $ lookUp' tableVar in
          runRealWithInjectedErrors "SaveSnapshot" env merrs
            (do Class.saveSnapshot name label table
                forM_ mcorr $ \corr -> Class.corruptSnapshot (bitChoice corr) name table)
            (\() -> Class.deleteSnapshot session name) -- TODO(err)
        OpenTableFromSnapshot _ name label ->
          runRealWithInjectedErrors "OpenTableFromSnapshot" env merrs
            (WrapTable <$> Class.openTableFromSnapshot session name label)
            (\(WrapTable t) -> Class.close t)
        DeleteSnapshot name ->
          runRealWithInjectedErrors "DeleteSnapshot" env merrs
            (Class.deleteSnapshot session name)
            (\_ -> pure ()) -- TODO(err)
        ListSnapshots ->
          runRealWithInjectedErrors "ListSnapshots" env merrs
            (Class.listSnapshots session)
            (\_ -> pure ()) -- TODO(err)
        Duplicate tableVar ->
          runRealWithInjectedErrors "Duplicate" env merrs
            (WrapTable <$> Class.duplicate (unwrapTable $ lookUp' tableVar))
            (\_ -> pure ()) -- TODO(err)
        Union table1Var table2Var ->
          runRealWithInjectedErrors "Union" env merrs
            (WrapTable <$> Class.union (unwrapTable $ lookUp' table1Var) (unwrapTable $ lookUp' table2Var))
            (\_ -> pure ()) -- TODO(err)
        Unions tableVars ->
          runRealWithInjectedErrors "Unions" env merrs
            (WrapTable <$> Class.unions (fmap (unwrapTable . lookUp') tableVars))
            (\_ -> pure ()) -- TODO(err)
        RemainingUnionDebt tableVar ->
          runRealWithInjectedErrors "RemainingUnionDebt" env merrs
            (Class.remainingUnionDebt (unwrapTable $ lookUp' tableVar))
            (\_ -> pure ()) -- TODO(err)
        SupplyUnionCredits tableVar credits ->
          runRealWithInjectedErrors "SupplyUnionCredits" env merrs
            (Class.supplyUnionCredits (unwrapTable $ lookUp' tableVar) credits)
            (\_ -> pure ()) -- TODO(err)
        SupplyPortionOfDebt tableVar portion ->
          runRealWithInjectedErrors "SupplyPortionOfDebt" env merrs
              (do let table = unwrapTable $ lookUp' tableVar
                  debt <- Class.remainingUnionDebt table
                  Class.supplyUnionCredits table (portion `portionOf` debt))
              (\_ -> pure ()) -- TODO(err)
      where
        session = envSession env

    lookUp' :: Var h x -> Realized (IOSim s) x
    lookUp' = lookUpGVar (Proxy @(RealMonad h (IOSim s))) lookUp

-- | @'runRealWithInjectedErrors' _ errsVar merrs action rollback@ runs @action@
-- with injected errors if available in @merrs@.
--
-- See 'Model.runModelMWithInjectedErrors': the real system is not guaranteed to
-- fail with an error if there are injected disk faults, but the model *always*
-- fails. In case the real system accidentally succeeded in running @action@
-- when there were errors to inject, then we roll back the success using
-- @rollback@. This ensures that the model and system stay in sync. For example:
-- if creating a snapshot accidentally succeeded, then the rollback action is to
-- delete that snapshot.
runRealWithInjectedErrors ::
     (MonadCatch m, MonadSTM m, PrimMonad m)
  => String -- ^ Name of the action
  -> RealEnv h m
  -> Maybe Errors
  -> m t-- ^ Action to run
  -> (t -> m ()) -- ^ Rollback if the action *accidentally* succeeded
  -> m (Either Model.Err t)
runRealWithInjectedErrors s env merrs k rollback =
  case merrs of
    Nothing -> do
      modifyMutVar faultsVar (InjectFaultNone s :)
      catchErr handlers k
    Just errs -> do
      atomically $ writeTVar logVar emptyLog
      eith <- catchErr handlers $ FSSim.withErrors errsVar errs k
      errsLog <- readTVarIO logVar
      case eith of
        Left e | Model.isDiskFault e -> do
          modifyMutVar faultsVar (InjectFaultInducedError s :)
          if countNoisyErrors errsLog == 0 then
            pure $ Left $ Model.ErrOther $
              -- If we injected 0 disk faults, but we still found an
              -- ErrDiskFault, then there is a bug in our code. ErrDiskFaults
              -- should not occur on the happy path.
              "Found a disk fault error, but no disk faults were injected: " <> show e
          else
            pure eith
        Left e -> do
          if countNoisyErrors errsLog > 0 then
            pure $ Left $ Model.ErrOther $
              -- If we injected 1 or more disk faults, but we did not find an
              -- ErrDiskFault, then there is a bug in our code. An injected disk
              -- fault should always lead to an ErrDiskFault.
              "Found an error that isn't a disk fault error, but disk faults were injected: " <> show e
          else
            pure eith
        Right x -> do
          modifyMutVar faultsVar (InjectFaultAccidentalSuccess s :)
          rollback x
          if (countNoisyErrors errsLog > 0) then
            pure $ Left $ Model.ErrOther $
              -- If we injected 1 or more disk faults, but the action
              -- accidentally succeeded, then 1 or more errors were swallowed
              -- that should have been found as ErrDiskFault.
              "Action succeeded, but disk faults were injected. Errors were swallowed!"
          else
            pure $ Left $ Model.ErrDiskFault ("dummy: " <> s)
  where
    errsVar = envErrors env
    logVar = envErrorsLog env
    faultsVar = envInjectFaultResults env
    handlers = envHandlers env

catchErr ::
     forall m a e. MonadCatch m
  => [Handler m (Maybe e)] -> m a -> m (Either e a)
catchErr hs action = catches (Right <$> action) (fmap f hs)
  where
    f (Handler h) = Handler $ \e -> maybe (throwIO e) (pure . Left) =<< h e

{-------------------------------------------------------------------------------
  Generator and shrinking
-------------------------------------------------------------------------------}

arbitraryActionWithVars ::
     forall h k v b. (
       C k v b
     , Ord k
     , Eq (Class.TableConfig h)
     , Show (Class.TableConfig h)
     , Arbitrary (Class.TableConfig h)
     , Typeable h
     )
  => Proxy (k, v, b)
  -> R.SnapshotLabel
  -> ModelVarContext (ModelState h)
  -> ModelState h
  -> Gen (Any (LockstepAction (ModelState h)))
arbitraryActionWithVars _ label ctx (ModelState st _stats) =
    QC.frequency $
      concat
        [ genActionsSession
        , genActionsTables
        , genActionsUnion
        , genActionsCursor
        , genActionsBlobRef
        ]
  where
    _coveredAllCases :: LockstepAction (ModelState h) a -> ()
    _coveredAllCases (Action _ action') = _coveredAllCases' action'

    _coveredAllCases' :: Action' h a -> ()
    _coveredAllCases' = \case
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
        Mupserts{} -> ()
        RetrieveBlobs{} -> ()
        SaveSnapshot{} -> ()
        DeleteSnapshot{} -> ()
        ListSnapshots{} -> ()
        OpenTableFromSnapshot{} -> ()
        Duplicate{} -> ()
        Union{} -> ()
        Unions{} -> ()
        RemainingUnionDebt{} -> ()
        SupplyUnionCredits{} -> ()
        SupplyPortionOfDebt{} -> ()

    genTableVar = QC.elements tableVars

    tableVars :: [Var h (WrapTable h IO k v b)]
    tableVars =
      [ fromRight v
      | v <- findVars ctx Proxy
      , case lookupVar ctx v of
          MEither (Left _)                  -> False
          MEither (Right (MTable t)) ->
            Map.member (Model.tableID t) (Model.tables st)
      ]

    genUnionDescendantTableVar = QC.elements unionDescendantTableVars

    unionDescendantTableVars :: [Var h (WrapTable h IO k v b)]
    unionDescendantTableVars =
        [ v
        | v <- tableVars
        , let t = case lookupVar ctx v of
                MTable t' -> t'
        , Model.isUnionDescendant t == Model.IsUnionDescendant
        ]

    genCursorVar = QC.elements cursorVars

    cursorVars :: [Var h (WrapCursor h IO k v b)]
    cursorVars =
      [ fromRight v
      | v <- findVars ctx Proxy
      , case lookupVar ctx v of
          MEither (Left _)            -> False
          MEither (Right (MCursor c)) ->
            Map.member (Model.cursorID c) (Model.cursors st)
      ]

    genBlobRefsVar = QC.elements blobRefsVars

    blobRefsVars :: [Var h (V.Vector (WrapBlobRef h IO b))]
    blobRefsVars = fmap (mapGVar (OpComp OpLookupResults)) lookupResultVars
                ++ fmap (mapGVar (OpComp OpEntrys))  queryResultVars
      where
        lookupResultVars :: [Var h (V.Vector (LookupResult  v (WrapBlobRef h IO b)))]
        queryResultVars  :: [Var h (V.Vector (Entry k v (WrapBlobRef h IO b)))]

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
        , let snapshotname = R.toSnapshotName name
        ]

    genActionsSession :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsSession =
        [ (1, fmap Some $ (Action <$> genErrors <*>) $
            New @k @v @b <$> pure PrettyProxy <*> QC.arbitrary)
        | length tableVars <= 5 -- no more than 5 tables at once
        , let genErrors = pure Nothing -- TODO: generate errors
        ]

     ++ [ (2, fmap Some $ (Action <$> genErrors <*>) $
            OpenTableFromSnapshot @k @v @b PrettyProxy <$> genUsedSnapshotName <*> pure label)
        | length tableVars <= 5 -- no more than 5 tables at once
        , not (null usedSnapshotNames)
        , let genErrors = QC.frequency [
                  (3, pure Nothing)
                , (1, Just . noRemoveDirectoryRecursiveE <$> QC.arbitrary)
                ]
        ]

     ++ [ (1, fmap Some $ (Action <$> genErrors <*>) $
            DeleteSnapshot <$> genUsedSnapshotName)
        | not (null usedSnapshotNames)
        , let genErrors = pure Nothing -- TODO: generate errors
        ]

     ++ [ (1, fmap Some $ (Action <$> genErrors <*>) $
            pure ListSnapshots)
        | not (null tableVars) -- otherwise boring!
        , let genErrors = pure Nothing -- TODO: generate errors
        ]

    genActionsTables :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsTables
      | null tableVars = []
      | otherwise      =
        [ (1,  fmap Some $ (Action <$> genErrors <*>) $
            Close <$> genTableVar)
        | let genErrors = pure Nothing
        ]
     ++ [ (10, fmap Some $ (Action <$> genErrors <*>) $
            Lookups <$> genLookupKeys <*> genTableVar)
        | let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (5,  fmap Some $ (Action <$> genErrors <*>) $
            RangeLookup <$> genRange <*> genTableVar)
        | let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (10, fmap Some $ (Action <$> genErrors <*>) $
            Updates <$> genUpdates <*> genTableVar)
        | let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (10, fmap Some $ (Action <$> genErrors <*>) $
            Inserts <$> genInserts <*> genTableVar)
        | let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (10, fmap Some $ (Action <$> genErrors <*>) $
            Deletes <$> genDeletes <*> genTableVar)
        | let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (10, fmap Some $ (Action <$> genErrors <*>) $
            Mupserts <$> genMupserts <*> genTableVar)
        | let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (3,  fmap Some $ (Action <$> genErrors <*>) $
            NewCursor <$> QC.arbitrary <*> genTableVar)
        | length cursorVars <= 5 -- no more than 5 cursors at once
        , let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (2,  fmap Some $ (Action <$> genErrors <*>) $
            SaveSnapshot <$> genCorruption <*> genUnusedSnapshotName <*> pure label <*> genTableVar)
        | not (null unusedSnapshotNames)
          -- TODO: should errors and corruption be generated at the same time,
          -- or should they be mutually exclusive?
        , let genErrors = pure Nothing -- TODO: generate errors
        , let genCorruption = QC.frequency [
                  (3, pure Nothing)
                , (1, Just <$> QC.arbitrary)
                ]
        ]
     ++ [ (5,  fmap Some $ (Action <$> genErrors <*>) $
            Duplicate <$> genTableVar)
        | length tableVars <= 5 -- no more than 5 tables at once
        , let genErrors = pure Nothing -- TODO: generate errors
        ]

    -- | Generate table actions that have to do with unions.
    genActionsUnion :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsUnion
      | null tableVars = []
      | otherwise =
        [ (2,  fmap Some $ (Action <$> genErrors <*>) $
            Union <$> genTableVar <*> genTableVar)
        | length tableVars <= 5 -- no more than 5 tables at once
        , let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (2,  fmap Some $ (Action <$> genErrors <*>) $ do
            -- Generate at least a 2-way union, and at most a 3-way union.
            --
            -- Tests for 1-way unions are included in the UnitTests module.
            -- n-way unions for n>3 lead to large unions, which are less
            -- likely to be finished before the end of an action sequence.
            n <- QC.chooseInt (2, 3)
            Unions . NE.fromList <$> QC.vectorOf n genTableVar)
        | length tableVars <= 5 -- no more than 5 tables at once
        , let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (2,  fmap Some $ (Action <$> genErrors <*>) $
            RemainingUnionDebt <$> genUnionDescendantTableVar)
            -- Tables not derived from unions are covered in UnitTests.
        | not (null unionDescendantTableVars)
        , let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (8, fmap Some $ (Action <$> genErrors <*>) $
            SupplyUnionCredits <$> genUnionDescendantTableVar <*> genUnionCredits)
            -- Tables not derived from unions are covered in UnitTests.
        | not (null unionDescendantTableVars)
        , let genErrors = pure Nothing -- TODO: generate errors
        ]
      ++ [ (2, fmap Some $ (Action <$> genErrors <*>) $
            SupplyPortionOfDebt <$> genUnionDescendantTableVar <*> genPortion)
            -- Tables not derived from unions are covered in UnitTests.
        | not (null unionDescendantTableVars)
        , let genErrors  = pure Nothing -- TODO: generate errors
        ]
      where
        -- The typical, interesting case is to supply a positive number of
        -- union credits. Supplying 0 or less credits is a no-op. We cover
        -- it in UnitTests so we don't have to cover it here.
        genUnionCredits = R.UnionCredits . QC.getPositive <$> QC.arbitrary

        -- TODO: tweak distribution once table unions are implemented
        genPortion = Portion <$> QC.elements [1, 2, 3]

    genActionsCursor :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsCursor
      | null cursorVars = []
      | otherwise       =
        [ (2,  fmap Some $ (Action <$> genErrors <*>) $
            CloseCursor <$> genCursorVar)
        | let genErrors = pure Nothing -- TODO: generate errors
        ]
     ++ [ (10, fmap Some $ (Action <$> genErrors <*>) $
            ReadCursor <$> (QC.getNonNegative <$> QC.arbitrary) <*> genCursorVar)
        | let genErrors = pure Nothing -- TODO: generate errors
        ]

    genActionsBlobRef :: [(Int, Gen (Any (LockstepAction (ModelState h))))]
    genActionsBlobRef =
        [ (5, fmap Some $ (Action <$> genErrors <*>) $
            RetrieveBlobs <$> genBlobRefsVar)
        | not (null blobRefsVars)
        , let genErrors = pure Nothing -- TODO: generate errors
        ]

    fromRight ::
         Var h (Either Model.Err a)
      -> Var h a
    fromRight = mapGVar (\op -> OpFromRight `OpComp` op)

    genLookupKeys :: Gen (V.Vector k)
    genLookupKeys = QC.arbitrary

    genRange :: Gen (R.Range k)
    genRange = QC.arbitrary

    genUpdates :: Gen (V.Vector (k, R.Update v b))
    genUpdates = QC.liftArbitrary ((,) <$> QC.arbitrary <*> QC.oneof [
          R.Insert <$> QC.arbitrary <*> genBlob
        , R.Upsert <$> QC.arbitrary
        , pure R.Delete
        ])
      where
        _coveredAllCases :: R.Update v b -> ()
        _coveredAllCases = \case
            R.Insert{} -> ()
            R.Upsert{} -> ()
            R.Delete{} -> ()

    genInserts :: Gen (V.Vector (k, v, Maybe b))
    genInserts = QC.liftArbitrary ((,,) <$> QC.arbitrary <*> QC.arbitrary <*> genBlob)

    genDeletes :: Gen (V.Vector k)
    genDeletes = QC.arbitrary

    genMupserts :: Gen (V.Vector (k, v))
    genMupserts = QC.liftArbitrary ((,) <$> QC.arbitrary <*> QC.arbitrary)

    genBlob :: Gen (Maybe b)
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
shrinkActionWithVars _ctx _st (Action merrs action') =
        [ -- TODO: it's somewhat unfortunate, but we have to dynamically
          -- construct evidence that @a@ is typeable, which is a requirement
          -- coming from the @Some@ existential. Could we find a different way
          -- to solve this using just regular constraints?
          case dictIsTypeable action' of
            Dict -> Some $ Action merrs' action'
        | merrs' <- QC.shrink merrs ]
     ++ [ Some $ Action merrs action''
        | Some action'' <- shrinkAction'WithVars _ctx _st action'
        ]

-- | Dynamically construct evidence that the result type @a@ of an action is
-- typeable.
dictIsTypeable :: Typeable h => Action' h a -> Dict (Typeable a)
dictIsTypeable = \case
      New{}                   -> Dict
      Close{}                 -> Dict
      Lookups{}               -> Dict
      RangeLookup{}           -> Dict
      NewCursor{}             -> Dict
      CloseCursor{}           -> Dict
      ReadCursor{}            -> Dict
      Updates{}               -> Dict
      Inserts{}               -> Dict
      Deletes{}               -> Dict
      Mupserts{}              -> Dict
      RetrieveBlobs{}         -> Dict
      SaveSnapshot{}          -> Dict
      OpenTableFromSnapshot{} -> Dict
      DeleteSnapshot{}        -> Dict
      ListSnapshots{}         -> Dict
      Duplicate{}             -> Dict
      Union{}                 -> Dict
      Unions{}                -> Dict
      RemainingUnionDebt{}    -> Dict
      SupplyUnionCredits{}    -> Dict
      SupplyPortionOfDebt{}   -> Dict

shrinkAction'WithVars ::
     forall h a. (
       Eq (Class.TableConfig h)
     , Arbitrary (Class.TableConfig h)
     , Typeable h
     )
  => ModelVarContext (ModelState h)
  -> ModelState h
  -> Action' h a
  -> [Any (Action' h)]
shrinkAction'WithVars _ctx _st a = case a of
    New p conf -> [
        Some $ New p conf'
      | conf' <- QC.shrink conf
      ]

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

    Mupserts kvs tableVar -> [
        Some $ Mupserts kvs' tableVar
      | kvs' <- QC.shrink kvs
      ] <> [
        Some $ Updates (V.map f kvs) tableVar
      | let f (k, v) = (k, R.Upsert v)
      ]

    Lookups ks tableVar -> [
        Some $ Lookups ks' tableVar
      | ks' <- QC.shrink ks
      ]

    -- * Unions

    Unions tableVars -> [
        Some $ Unions tableVars'
      | tableVars' <- QC.liftShrink (const []) tableVars
      ]

    SupplyUnionCredits tableVar (R.UnionCredits x) -> [
        Some $ SupplyUnionCredits tableVar (R.UnionCredits x')
      | x' <- QC.shrink x
      ]

    -- Shrink portions to absolute union credits. The credits are all /positive/
    -- powers of 2 that fit into a 64-bit Int. This choice is arbitrary, but the
    -- model has no accurate debt information, so the best we can do is provide
    -- a sensible distribution of union credits. This particular distribution is
    -- skewed to smaller numbers, because the union debt is also likely to be on
    -- the smaller side. Still, all larger powers of 2 are also included in case
    -- the union debt is larger.
    SupplyPortionOfDebt tableVar (Portion x) -> [
        Some $ SupplyUnionCredits tableVar (R.UnionCredits x')
      | x' <- [2 ^ i - 1 | (i :: Int) <- [0..20]]
      , assert (x' >= 0) True
      ] ++ [
        Some $ SupplyPortionOfDebt tableVar (Portion x')
      | x' <- QC.shrink x
      , x' > 0
      ]

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
    OpEntrys             -> Just . MVector
                          . V.mapMaybe (\case MEntry x -> getBlobRef x)
                          . \case MVector x -> x

{-------------------------------------------------------------------------------
  Statistics, labelling/tagging
-------------------------------------------------------------------------------}

data Stats = Stats {
    -- === Tags
    -- | Names for which snapshots exist
    snapshotted        :: !(Set R.SnapshotName)
    -- === Final tags (per action sequence, across all tables)
    -- | Number of succesful lookups and their results
  , numLookupsResults  :: {-# UNPACK #-} !(Int, Int, Int)
                          -- (NotFound, Found, FoundWithBlob)
    -- | Number of succesful updates
  , numUpdates         :: {-# UNPACK #-} !(Int, Int, Int, Int)
                          -- (Insert, InsertWithBlob, Delete, Mupsert)
    -- | Actions that succeeded
  , successActions     :: [String]
    -- | Actions that failed with an error
  , failActions        :: [(String, Model.Err)]
    -- === Final tags (per action sequence, per table)
    -- | Number of actions per table (succesful or failing)
  , numActionsPerTable :: !(Map Model.TableID Int)
    -- | The state of model tables at the point they were closed. This is used
    -- to augment the tables from the final model state (which of course has
    -- only tables still open in the final state).
  , closedTables       :: !(Map Model.TableID Model.SomeTable)
    -- | The ultimate parents for each table. These are the 'TableId's of tables
    -- created using 'new' or 'open'.
  , parentTable        :: !(Map Model.TableID [Model.TableID])
    -- | Track the interleavings of operations via different but related tables.
    -- This is a map from each ultimate parent table to a summary log of which
    -- tables (derived from that parent table via duplicate or union) have had
    -- \"interesting\" actions performed on them. We record only the
    -- interleavings of different tables not multiple actions on the same table.
  , dupTableActionLog  :: !(Map Model.TableID [Model.TableID])
    -- | The subset of tables (open or closed) that were created as a result
    -- of a union operation. This can be used for example to select subsets of
    -- the other per-table tracking maps above, or the state from the model.
    -- The map value is the size of the union table at the point it was created,
    -- so we can distinguish trivial empty unions from non-trivial.
  , unionTables        :: !(Map Model.TableID Int)
  }
  deriving stock Show

initStats :: Stats
initStats = Stats {
      -- === Tags
      snapshotted = Set.empty
      -- === Final tags
    , numLookupsResults = (0, 0, 0)
    , numUpdates = (0, 0, 0, 0)
    , successActions = []
    , failActions = []
    , numActionsPerTable = Map.empty
    , closedTables       = Map.empty
    , parentTable        = Map.empty
    , dupTableActionLog  = Map.empty
    , unionTables        = Map.empty
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
updateStats action@(Action _merrs action') lookUp modelBefore modelAfter result =
      -- === Tags
      updSnapshotted
      -- === Final tags
    . updNumLookupsResults
    . updNumUpdates
    . updSuccessActions
    . updFailActions
    . updNumActionsPerTable
    . updClosedTables
    . updDupTableActionLog
    . updParentTable
    . updUnionTables
  where
    -- === Tags

    updSnapshotted stats = case (action', result) of
      (SaveSnapshot _ name _ _, MEither (Right (MUnit ()))) -> stats {
          snapshotted = Set.insert name (snapshotted stats)
        }
      (DeleteSnapshot name, MEither (Right (MUnit ()))) -> stats {
          snapshotted = Set.delete name (snapshotted stats)
        }
      _ -> stats

    -- === Final tags

    updNumLookupsResults stats = case (action', result) of
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

    updNumUpdates stats = case (action', result) of
        (Updates upds _, MEither (Right (MUnit ()))) -> stats {
            numUpdates = countAll upds
          }
        (Inserts ins _, MEither (Right (MUnit ()))) -> stats {
            numUpdates = countAll $ V.map (\(k, v, b) -> (k, R.Insert v b)) ins
          }
        (Deletes ks _, MEither (Right (MUnit ()))) -> stats {
            numUpdates = countAll $ V.map (\k -> (k, R.Delete)) ks
          }
        (Mupserts mups _, MEither (Right (MUnit ()))) -> stats {
            numUpdates = countAll $ V.map (second R.Upsert) mups
          }
        _ -> stats
      where
        countAll :: forall k v b. V.Vector (k, R.Update v b) -> (Int, Int, Int, Int)
        countAll upds =
          let count :: (Int, Int, Int, Int)
                    -> (k, R.Update v blob)
                    -> (Int, Int, Int, Int)
              count (i, iwb, d, m) (_, upd) = case upd  of
                R.Insert _ Nothing -> (i+1, iwb  , d  , m  )
                R.Insert _ Just{}  -> (i  , iwb+1, d  , m  )
                R.Delete{}         -> (i  , iwb  , d+1, m  )
                R.Upsert{}         -> (i  , iwb  , d  , m+1)
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
    updNumActionsPerTable stats = case action' of
        New{}
          | MEither (Right (MTable table)) <- result -> initCount table
          | otherwise                                      -> stats
        OpenTableFromSnapshot{}
          | MEither (Right (MTable table)) <- result -> initCount table
          | otherwise                                      -> stats
        Duplicate{}
          | MEither (Right (MTable table)) <- result -> initCount table
          | otherwise                                      -> stats
        Union{}
          | MEither (Right (MTable table)) <- result -> initCount table
          | otherwise                                      -> stats
        Unions{}
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
        Mupserts _ tableVar    -> updateCount tableVar
        RemainingUnionDebt tableVar -> updateCount tableVar
        SupplyUnionCredits tableVar _ -> updateCount tableVar
        SupplyPortionOfDebt tableVar _ -> updateCount tableVar
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
        SaveSnapshot{}        -> stats
        DeleteSnapshot{}      -> stats
        ListSnapshots{}       -> stats
      where
        -- Init to 0 so we get an accurate count of tables with no actions.
        initCount :: forall k v b. Model.Table k v b -> Stats
        initCount table =
          let tid = Model.tableID table
           in stats {
                numActionsPerTable = Map.insert tid 0 (numActionsPerTable stats)
              }

        -- Note that batches (of inserts lookups etc) count as one action.
        updateCount :: forall k v b.
                       Var h (WrapTable h IO k v b)
                    -> Stats
        updateCount tableVar =
          let tid = getTableId (lookUp tableVar)
           in stats {
                numActionsPerTable = Map.insertWith (+) tid 1
                                                    (numActionsPerTable stats)
              }

    updClosedTables stats = case (action', result) of
        (Close tableVar, MEither (Right (MUnit ())))
          | MTable t <- lookUp tableVar
          , let tid          = Model.tableID t
            -- This lookup can fail if the table was already closed:
          , Just (_, table) <- Map.lookup tid (Model.tables modelBefore)
          -> stats {
               closedTables = Map.insert tid table (closedTables stats)
             }
        _ -> stats

    updParentTable stats = case (action', result) of
        (New{}, MEither (Right (MTable tbl))) ->
          insertParentTableNew tbl stats
        (OpenTableFromSnapshot{}, MEither (Right (MTable tbl))) ->
          insertParentTableNew tbl stats
        (Duplicate ptblVar, MEither (Right (MTable tbl))) ->
          insertParentTableDerived [ptblVar] tbl stats
        (Union ptblVar1 ptblVar2, MEither (Right (MTable tbl))) ->
          insertParentTableDerived [ptblVar1, ptblVar2]  tbl stats
        (Unions ptblVars, MEither (Right (MTable tbl))) ->
          insertParentTableDerived (NE.toList ptblVars) tbl stats
        _ -> stats

    -- insert an entry into the parentTable for a completely new table
    insertParentTableNew :: forall k v b. Model.Table k v b -> Stats -> Stats
    insertParentTableNew tbl stats =
      stats {
        parentTable = Map.insert (Model.tableID tbl)
                                 [Model.tableID tbl]
                                 (parentTable stats)
      }

    -- insert an entry into the parentTable for a table derived from a parent
    insertParentTableDerived :: forall k v b.
                                [GVar Op (WrapTable h IO k v b)]
                             -> Model.Table k v b -> Stats -> Stats
    insertParentTableDerived ptblVars tbl stats =
      let uptblIds :: [Model.TableID] -- the set of ultimate parent table ids
          uptblIds = nub [ uptblId
                         | ptblVar <- ptblVars
                           -- immediate and ultimate parent table id:
                         , let iptblId = getTableId (lookUp ptblVar)
                         , uptblId <- parentTable stats Map.! iptblId
                         ]
       in stats {
            parentTable = Map.insert (Model.tableID tbl)
                                     uptblIds
                                     (parentTable stats)
          }

    updDupTableActionLog stats | MEither (Right _) <- result =
      case action' of
        Lookups     ks   tableVar
          | not (null ks)         -> updateLastActionLog tableVar
          | otherwise             -> stats
        RangeLookup r    tableVar
          | not (emptyRange r)    -> updateLastActionLog tableVar
          | otherwise             -> stats
        NewCursor   _    tableVar -> updateLastActionLog tableVar
        Updates     upds tableVar
          | not (null upds)       -> updateLastActionLog tableVar
          | otherwise             -> stats
        Inserts     ins  tableVar
          | not (null ins)        -> updateLastActionLog tableVar
          | otherwise             -> stats
        Deletes     ks   tableVar
          | not (null ks)         -> updateLastActionLog tableVar
          | otherwise             -> stats
        Mupserts    mups tableVar
          | not (null mups)       -> updateLastActionLog tableVar
          | otherwise             -> stats
        Close            tableVar -> updateLastActionLog tableVar
        -- Uninteresting actions
        New{} -> stats
        CloseCursor{} -> stats
        ReadCursor{} -> stats
        RetrieveBlobs{} -> stats
        SaveSnapshot{} -> stats
        OpenTableFromSnapshot{} -> stats
        DeleteSnapshot{} -> stats
        ListSnapshots{} -> stats
        Duplicate{} -> stats
        Union{} -> stats
        Unions{} -> stats
        RemainingUnionDebt{} -> stats
        SupplyUnionCredits{} -> stats
        SupplyPortionOfDebt{} -> stats
      where
        -- add the current table to the front of the list of tables, if it's
        -- not the latest one already
        updateLastActionLog :: GVar Op (WrapTable h IO k v b) -> Stats
        updateLastActionLog tableVar =
          stats {
            dupTableActionLog = foldl' (flip (Map.alter extendLog))
                                       (dupTableActionLog stats)
                                       (parentTable stats Map.! thid)
          }
          where
            thid = getTableId (lookUp tableVar)

            extendLog :: Maybe [Model.TableID] -> Maybe [Model.TableID]
            extendLog (Just alog@(thid' : _)) | thid == thid'
                                  = Just alog          -- action via same table
            extendLog (Just alog) = Just (thid : alog) -- action via different table
            extendLog Nothing     = Just (thid : [])   -- first action on table

        emptyRange (R.FromToExcluding l u) = l >= u
        emptyRange (R.FromToIncluding l u) = l >  u

    updDupTableActionLog stats = stats

    updUnionTables stats = case action' of
        Union{}  -> insertUnionTable
        Unions{} -> insertUnionTable
        _        -> stats
      where
        insertUnionTable
          | MEither (Right (MTable t)) <- result
          , let tid = Model.tableID t
          , Just (_,tbl) <- Map.lookup tid (Model.tables modelAfter)
          , let sz = Model.withSomeTable Model.size tbl
          = stats {
              unionTables = Map.insert tid sz (unionTables stats)
            }
          | otherwise
          = stats

    getTableId :: ModelValue (ModelState h) (WrapTable h IO k v b)
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
    -- | A corrupted snapshot was created successfully
  | SaveSnapshotCorrupted R.SnapshotName
    -- | An /un/corrupted snapshot was created successfully
  | SaveSnapshotUncorrupted R.SnapshotName
    -- | A snapshot failed to open because we detected that the snapshot was
    -- corrupt
  | OpenTableFromSnapshotDetectsCorruption R.SnapshotName
    -- | Opened a snapshot (successfully) for a table involving a union
  | OpenTableFromSnapshotUnion
  deriving stock (Show, Eq, Ord)

-- | This is run for after every action
tagStep' ::
     (ModelState h, ModelState h)
  -> LockstepAction (ModelState h) a
  -> Val h a
  -> [Tag]
tagStep' (ModelState _stateBefore statsBefore,
          ModelState _stateAfter _statsAfter)
          (Action _ action') result =
    catMaybes [
      tagSnapshotTwice
    , tagOpenExistingSnapshot
    , tagOpenExistingSnapshot
    , tagOpenMissingSnapshot
    , tagDeleteExistingSnapshot
    , tagDeleteMissingSnapshot
    , tagSaveSnapshotCorruptedOrUncorrupted
    , tagOpenTableFromSnapshotDetectsCorruption
    , tagOpenTableFromSnapshotUnion
    ]
  where
    tagSnapshotTwice
      | SaveSnapshot _ name _ _ <- action'
      , name `Set.member` snapshotted statsBefore
      = Just SnapshotTwice
      | otherwise
      = Nothing

    tagOpenExistingSnapshot
      | OpenTableFromSnapshot _ name _ <- action'
      , name `Set.member` snapshotted statsBefore
      = Just OpenExistingSnapshot
      | otherwise
      = Nothing

    tagOpenMissingSnapshot
      | OpenTableFromSnapshot _ name _ <- action'
      , not (name `Set.member` snapshotted statsBefore)
      = Just OpenMissingSnapshot
      | otherwise
      = Nothing

    tagDeleteExistingSnapshot
      | DeleteSnapshot name <- action'
      , name `Set.member` snapshotted statsBefore
      = Just DeleteExistingSnapshot
      | otherwise
      = Nothing

    tagDeleteMissingSnapshot
      | DeleteSnapshot name <- action'
      , not (name `Set.member` snapshotted statsBefore)
      = Just DeleteMissingSnapshot
      | otherwise
      = Nothing

    tagSaveSnapshotCorruptedOrUncorrupted
      | SaveSnapshot mcorr name _ _ <- action'
      , MEither (Right (MUnit ())) <- result
      = Just $ case mcorr of
          Just (_ :: SilentCorruption) -> SaveSnapshotCorrupted name
          _                            -> SaveSnapshotUncorrupted name
      | otherwise
      = Nothing

    tagOpenTableFromSnapshotDetectsCorruption
      | OpenTableFromSnapshot _ name _ <- action'
      , MEither (Left (MErr (Model.ErrSnapshotCorrupted _))) <- result
      = Just (OpenTableFromSnapshotDetectsCorruption name)
      | otherwise
      = Nothing

    tagOpenTableFromSnapshotUnion
      | OpenTableFromSnapshot{} <- action'
      , MEither (Right (MTable t)) <- result
      , Model.isUnionDescendant t == Model.IsUnionDescendant
      = Just OpenTableFromSnapshotUnion
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
    -- | Number of 'Class.Mupsert's succesfully submitted to a table
    -- (this includes submissions through both 'Class.updates' and
    -- 'Class.mupserts')
  | NumMupserts String
    -- | Total number of actions (failing, succeeding, either)
  | NumActions String
    -- | Which actions succeded
  | ActionSuccess String
    -- | Which actions failed
  | ActionFail String Model.Err
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
    , tagNumUnionTables
    , tagNumUnionTableActions
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
        , ("Mupserts"           , [NumMupserts         $ showPowersOf 10 m])
        ]
      where (i, iwb, d, m) = numUpdates finalStats

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
              closedSizes = Model.withSomeTable Model.size <$>
                              closedTables finalStats
        , size <- Map.elems (openSizes `Map.union` closedSizes)
        ]

    tagDupTableActionLog =
        [ ("Interleaved actions on table duplicates or unions",
           [DupTableActionLog (showPowersOf 2 n)])
        | (_, alog) <- Map.toList (dupTableActionLog finalStats)
        , let n = length alog
        ]

    tagNumUnionTables =
        [ ("Number of union tables (empty)",
           [NumTables (showPowersOf 2 (Map.size trivial))])
        , ("Number of union tables (non-empty)",
           [NumTables (showPowersOf 2 (Map.size nonTrivial))])
        ]
      where
        (nonTrivial, trivial) = Map.partition (> 0) (unionTables finalStats)

    tagNumUnionTableActions =
        [ ("Number of actions per table with non-empty unions",
           [ NumTableActions (showPowersOf 2 n) ])
        | n <- Map.elems $ numActionsPerTable finalStats
                             `Map.intersection`
                           Map.filter (> 0) (unionTables finalStats)
        ]

{-------------------------------------------------------------------------------
  Utils
-------------------------------------------------------------------------------}

-- | Version of 'QLS.runActionsBracket' with tagging of the final state.
--
-- The 'tagStep' feature tags each step (i.e., 'Action'), but there are cases
-- where one wants to tag a /list of/ 'Action's. For example, if one wants to
-- count how often something happens over the course of running these actions,
-- then we would want to only tag the final state, not intermediate steps.
runActionsBracket ::
     forall state st m e prop.  (
       RunLockstep state m
     , e ~ Error (Lockstep state)
     , forall a. IsPerformResult e a
     , QC.Testable prop
     )
  => Proxy state
  -> CheckCleanup
  -> CheckRefs
  -> IO st
  -> (st -> IO prop)
  -> (m QC.Property -> st -> IO QC.Property)
  -> (Lockstep state -> [(String, [FinalTag])])
  -> Actions (Lockstep state) -> QC.Property
runActionsBracket p cleanupFlag refsFlag init cleanup runner tagger actions =
    tagFinalState actions tagger
  $ QLS.runActionsBracket p init cleanup' runner actions
  where
    cleanup' st = do
      -- We want to run forgotten reference checks after cleanup, since cleanup
      -- itself may lead to forgotten refs. The reference checks have the
      -- crucial side effect of reseting the forgotten refs state. If we don't
      -- do this then the next test run (e.g. during shrinking) will encounter a
      -- false/stale forgotten refs exception. But we also have to make sure
      -- that if cleanup itself fails, that we still run the reference checks.
      -- 'propCleanup' will make sure to catch any exceptions that are thrown by
      -- the 'cleanup' action. 'propRefs' will then definitely run afterwards so
      -- that the frogotten reference checks are definitely performed.
      x <- propCleanup cleanupFlag $ cleanup st
      y <- propRefs refsFlag
      pure (x QC..&&. y)

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

propException :: (Exception e, QC.Testable prop) => String -> Either e prop -> Property
propException s (Left e)     = QC.counterexample (s <> displayException e) False
propException _ (Right prop) = QC.property prop

{-------------------------------------------------------------------------------
  Cleanup exceptions
-------------------------------------------------------------------------------}

-- | Flag that turns on\/off cleanup checks.
--
-- If injected errors left the database in an inconsistent state, then property
-- cleanup might throw exceptions. If 'CheckCleanup' is used, this will lead to
-- failing properties, otherwise the exceptions are ignored.
data CheckCleanup = CheckCleanup | NoCheckCleanup

propCleanup :: (MonadCatch m, QC.Testable prop) => CheckCleanup -> m prop -> m Property
propCleanup flag cleanupAction =
    propException "Cleanup exception: " <$> checkCleanupM flag cleanupAction

checkCleanupM :: (MonadCatch m, QC.Testable prop) => CheckCleanup -> m prop -> m (Either SomeException Property)
checkCleanupM flag cleanupAction = do
    eith <- try @_ @SomeException cleanupAction
    case flag of
      CheckCleanup   -> pure $ QC.property <$> eith
      NoCheckCleanup -> pure (Right $ QC.property ())

{-------------------------------------------------------------------------------
  Reference checks
-------------------------------------------------------------------------------}

-- | Flag that turns on\/off reference checks.
--
-- If injected errors left the database in an inconsistent state, then some
-- references might be forgotten, which leads to reference exceptions. If
-- 'CheckRefs' is used, this will lead to failing properties, otherwise the
-- exceptions are ignored.
data CheckRefs = CheckRefs | NoCheckRefs

propRefs :: (PrimMonad m, MonadCatch m) => CheckRefs -> m Property
propRefs flag = propException "Reference exception: " <$> checkRefsM flag

checkRefsM :: (PrimMonad m, MonadCatch m) => CheckRefs -> m (Either RefException ())
checkRefsM flag = case flag of
    CheckRefs   -> try checkForgottenRefs
    NoCheckRefs -> Right <$> ignoreForgottenRefs
