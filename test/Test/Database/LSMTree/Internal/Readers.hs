{-# LANGUAGE TypeFamilies #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Readers (tests) where

import           Control.Exception (assert)
import           Control.Monad (forM)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Trans.Reader (ReaderT (..))
import           Control.Monad.Trans.State (StateT (..), get, put)
import           Control.RefCount
import           Data.Bifunctor (bimap, first)
import           Data.Coerce (coerce)
import           Data.Foldable (traverse_)
import qualified Data.Map.Strict as Map
import           Data.Proxy (Proxy (..))
import           Data.Tree (Tree)
import qualified Data.Tree as Tree
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators (BiasedKey (..))
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Internal.BlobRef
import qualified Database.LSMTree.Internal.BloomFilter as Bloom
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Index as Index (IndexType (Ordinary))
import           Database.LSMTree.Internal.Readers (HasMore (Drained, HasMore),
                     Readers)
import qualified Database.LSMTree.Internal.Readers as Readers
import qualified Database.LSMTree.Internal.Run as Run
import qualified Database.LSMTree.Internal.RunAcc as RunAcc
import qualified Database.LSMTree.Internal.RunBuilder as RunBuilder
import           Database.LSMTree.Internal.RunNumber
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.UniqCounter
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.Sim as FsSim
import qualified System.FS.Sim.MockFS as MockFS
import qualified Test.QuickCheck as QC
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck hiding (Some)
import           Test.Util.Orphans ()

import           Test.QuickCheck.StateModel
import           Test.QuickCheck.StateModel.Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Defaults as Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Run as Lockstep

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.Readers"
    [ testProperty "prop_lockstep" $
        Lockstep.runActionsBracket (Proxy @ReadersState)
          mempty mempty $ \act () -> do
          (prop, mockFS) <- FsSim.runSimHasBlockIO MockFS.empty $ \hfs hbio -> do
            (prop, RealState _ mCtx) <- runRealMonad hfs hbio
                                                     (RealState 0 Nothing) act
            traverse_ closeReadersCtx mCtx  -- close current readers
            pure prop

          -- ensure that all handles have been closed
          pure $ prop
              .&&. counterexample "file handles" (MockFS.numOpenHandles mockFS === 0)
    ]

runParams :: RunBuilder.RunParams
runParams =
    RunBuilder.RunParams {
      runParamCaching = RunBuilder.CacheRunData,
      runParamAlloc   = RunAcc.RunAllocFixed 10,
      runParamIndex   = Index.Ordinary
    }

testSalt :: Bloom.Salt
testSalt = 4

--------------------------------------------------------------------------------

type SerialisedEntry = Entry SerialisedValue SerialisedBlob

type Handle = MockFS.HandleMock

data ReaderSourceData =
    FromWriteBufferData
      !(RunData BiasedKey SerialisedValue SerialisedBlob)
  | FromRunData
      !(RunData BiasedKey SerialisedValue SerialisedBlob)
  | FromReadersData
      !Readers.ReadersMergeType ![ReaderSourceData]
  deriving stock (Eq, Show)

depth :: ReaderSourceData -> Int
depth = \case
    FromWriteBufferData _ -> 1
    FromRunData         _ -> 1
    FromReadersData _ rds -> 1 + maximum (0 : map depth rds)

instance Arbitrary ReaderSourceData where
  arbitrary = QC.oneof [genLeaf, genTree]
    where
      genLeaf = QC.oneof [
            FromWriteBufferData <$> arbitrary
          , FromRunData <$> arbitrary
          ]

      genTree =
          -- first generate a tree shape of up to 10 nodes, then the actual data
          QC.scale (`div` 10) (arbitrary @(Tree ())) >>= enrich

      enrich :: Tree () -> Gen ReaderSourceData
      enrich (Tree.Node () []) =
          genLeaf
      enrich (Tree.Node () children) =
          FromReadersData <$> arbitrary <*> traverse enrich children

  shrink (FromWriteBufferData rd) =
      [ FromWriteBufferData rd' | rd' <- shrink rd
      ]
  shrink (FromRunData rd) =
      [ FromWriteBufferData rd
      ] <>
      [ FromRunData rd' | rd' <- shrink rd
      ]
  shrink s@(FromReadersData ty rds) =
      rds
        <>
      [ FromRunData (RunData (sourceKOps s))
      ] <>
      [ FromReadersData ty' rds' | (ty', rds') <- shrink (ty, rds)
      ]

instance Arbitrary Readers.ReadersMergeType where
  arbitrary = QC.elements [Readers.MergeLevel, Readers.MergeUnion]
    where
      _coveredAllCases :: Readers.ReadersMergeType -> ()
      _coveredAllCases = \case
          Readers.MergeLevel -> ()
          Readers.MergeUnion -> ()

sourceKOps :: ReaderSourceData -> Map.Map BiasedKey SerialisedEntry
sourceKOps (FromWriteBufferData rd) = unRunData rd
sourceKOps (FromRunData         rd) = unRunData rd
sourceKOps (FromReadersData ty rds) = Map.unionsWith f (map sourceKOps rds)
  where
    f = case ty of
      Readers.MergeLevel -> combine resolve
      Readers.MergeUnion -> combineUnion resolve

resolve :: ResolveSerialisedValue
resolve (SerialisedValue x) (SerialisedValue y) = SerialisedValue (x <> y)

--------------------------------------------------------------------------------
-- Mock

newtype MockReaders = MockReaders
    { mockEntries :: [((SerialisedKey, RunNumber), SerialisedEntry)] }
  deriving stock Show

isEmpty :: MockReaders -> Bool
isEmpty (MockReaders xs) = null xs

size :: MockReaders -> Int
size (MockReaders xs) = length xs

newMock :: Maybe SerialisedKey
        -> [ReaderSourceData]
        -> MockReaders
newMock offset =
      MockReaders . Map.toList . Map.unions
    . zipWith (\i -> Map.mapKeysMonotonic (\k -> (k, RunNumber i))) [0..]
    . map (skip . Map.mapKeysMonotonic serialiseKey . sourceKOps)
  where
    skip = maybe id (\k -> Map.dropWhileAntitone (< k)) offset

peekKeyMock :: MockReaders -> Either () SerialisedKey
peekKeyMock (MockReaders xs) = case xs of
    []                -> Left ()
    (((k, _), _) : _) -> Right k

-- | Drops the first @n@ entries, returning the last of them.
popMock :: Int -> MockReaders -> (Either () (SerialisedKey, SerialisedEntry, HasMore), MockReaders)
popMock n (MockReaders xs) = assert (n >= 1) $
    case drop (n - 1) xs of
      [] ->
        (Left (), MockReaders []) --popping too many still modifies state
      (((k, _), e) : rest) ->
        (Right (k, e, toHasMore rest), MockReaders rest)

dropWhileKeyMock :: SerialisedKey -> MockReaders -> (Either () (Int, HasMore), MockReaders)
dropWhileKeyMock k m@(MockReaders xs)
  | null xs = (Left (), m)
  | otherwise =
      let (dropped, xs') = span ((<= k) . fst . fst) xs
      in (Right (length dropped, toHasMore xs'), MockReaders xs')

toHasMore :: [a] -> HasMore
toHasMore xs = if null xs then Drained else HasMore


data ReadersState = ReadersState MockReaders
  deriving stock Show

initReadersState :: ReadersState
initReadersState = ReadersState (newMock Nothing [])

--------------------------------------------------------------------------------

type ReadersAct a = Action (Lockstep ReadersState) (Either () a)

deriving stock instance Show (Action (Lockstep ReadersState) a)
deriving stock instance Eq   (Action (Lockstep ReadersState) a)

instance StateModel (Lockstep ReadersState) where
  data Action (Lockstep ReadersState) a where
    New          :: Maybe BiasedKey  -- ^ optional offset
                 -> [ReaderSourceData]
                 -> ReadersAct ()
    PeekKey      :: ReadersAct SerialisedKey
    Pop          :: Int  -- allow popping many at once to drain faster
                 -> ReadersAct (SerialisedKey, SerialisedEntry, HasMore)
    DropWhileKey :: SerialisedKey
                 -> ReadersAct (Int, HasMore)

  initialState    = Lockstep.initialState initReadersState
  nextState       = Lockstep.nextState
  precondition    = Lockstep.precondition
  arbitraryAction = Lockstep.arbitraryAction
  shrinkAction    = Lockstep.shrinkAction

type ReadersVal a = ModelValue ReadersState a
type ReadersObs a = Observable ReadersState a

deriving stock instance Show (ReadersVal a)
deriving stock instance Show (ReadersObs a)
deriving stock instance Eq   (ReadersObs a)

instance InLockstep ReadersState where
  data ModelValue ReadersState a where
    MEntry  :: SerialisedEntry -> ReadersVal SerialisedEntry
    MKey    :: SerialisedKey -> ReadersVal SerialisedKey
    MHasMore:: HasMore -> ReadersVal HasMore
    MInt    :: Int -> ReadersVal Int
    MUnit   :: () -> ReadersVal ()
    MEither :: Either (ReadersVal a) (ReadersVal b) -> ReadersVal (Either a b)
    MTuple2 :: (ReadersVal a, ReadersVal b)         -> ReadersVal (a, b)
    MTuple3 :: (ReadersVal a, ReadersVal b, ReadersVal c) -> ReadersVal (a, b, c)

  data Observable ReadersState a where
    OId     :: (Eq a, Show a) => a -> ReadersObs a
    OEither :: Either (ReadersObs a) (ReadersObs b) -> ReadersObs (Either a b)
    OTuple2 :: (ReadersObs a, ReadersObs b) -> ReadersObs (a, b)
    OTuple3 :: (ReadersObs a, ReadersObs b, ReadersObs c) -> ReadersObs (a, b, c)

  observeModel :: ReadersVal a -> ReadersObs a
  observeModel = \case
    MEntry e -> OId e
    MKey k -> OId k
    MHasMore h -> OId h
    MInt n -> OId n
    MUnit () -> OId ()
    MEither x -> OEither $ bimap observeModel observeModel x
    MTuple2 x -> OTuple2 $ bimap observeModel observeModel x
    MTuple3 x -> OTuple3 $ trimap observeModel observeModel observeModel x

  modelNextState :: forall a.
       LockstepAction ReadersState a
    -> ModelVarContext ReadersState
    -> ReadersState
    -> (ReadersVal a, ReadersState)
  modelNextState action _ctx (ReadersState mock) =
      ReadersState <$> runMock action mock

  usedVars :: LockstepAction ReadersState a -> [AnyGVar (ModelOp ReadersState)]
  usedVars = const []

  arbitraryWithVars ::
       ModelVarContext ReadersState
    -> ReadersState
    -> Gen (Any (LockstepAction ReadersState))
  arbitraryWithVars _ctx (ReadersState mock)
    | isEmpty mock = do
        -- It's not allowed to keep using a drained RunReaders,
        -- we can only create a new one.
        sources <- vector =<< chooseInt (1, 10)
        let keys = concatMap (Map.keys . sourceKOps) sources
        offset <-
          if null keys
          then pure Nothing
          else oneof
                 [ liftArbitrary (elements (coerce keys))  -- existing key
                 , arbitrary                               -- any key
                 ]
        pure $ Some $ New offset sources
    | otherwise =
        QC.frequency $
          [ (5, pure (Some PeekKey))
          , (8, pure (Some (Pop 1)))
          , (1, Some . Pop <$> chooseInt (1, size mock))  -- drain a significant amount
          , (1, pure (Some (Pop (max 1 (size mock - 3)))))  -- drain almost everything
          , (1, pure (Some (Pop (size mock + 1)))) -- drain /more/ than available
          , (1, Some . DropWhileKey <$> arbitrary)  -- might drop a lot
          ] <>
          [ (4, pure (Some (DropWhileKey k)))  -- drops at least one key
          | Right k <- [peekKeyMock mock]
          ]

  shrinkWithVars ::
       ModelVarContext ReadersState
    -> ReadersState
    -> LockstepAction ReadersState a
    -> [Any (LockstepAction ReadersState)]
  shrinkWithVars _ctx _st = \case
      New k sources -> [ Some (New k' sources')
                       | (k', sources') <- shrink (k, sources)
                       ]
      -- arbitraryWithVars does /not/ have an invariant that n is less than
      -- the number of elements available. The only invariant to preserve when
      -- shrinking is to keep n greater than 0.
      Pop n         -> Some . Pop <$> filter (> 0) (shrink n)
      _             -> []

  tagStep ::
       (ReadersState, ReadersState)
    -> LockstepAction ReadersState a
    -> ReadersVal a
    -> [String]
  tagStep (ReadersState before, ReadersState after) action _result = concat
      -- Directly using strings, since there is only a small number of tags.
      [ [ "NewEntries " <> showPowersOf 10 numEntries
        | New _ sources <- [action]
        , let numEntries = sum (map (Map.size . sourceKOps) sources)
        ]
      , [ "NewEntriesKeyDuplicates " <> showPowersOf 2 keyCount
        | New _ sources <- [action]
        , let keyCounts = Map.unionsWith (+) (map (Map.map (const 1) . sourceKOps) sources)
        , keyCount <- Map.elems keyCounts
        , keyCount > 1
        ]
      , [ "NewDepth " <> showPowersOf 2 (maximum (0 : map depth sources))
        | New _ sources <- [action]
        ]
      , [ "ReadersFullyDrained"
        | not (isEmpty before), isEmpty after
        ]
      , [ "DropWhileKeyDropped " <> showPowersOf 2 (length dropped)
        | DropWhileKey key <- [action]
        , let dropped = takeWhile ((== key) . fst . fst) (mockEntries before)
        ]
      ]

runMock ::
     Action (Lockstep ReadersState) a
  -> MockReaders
  -> (ReadersVal a, MockReaders)
runMock = \case
    New k sources  -> const $ wrap MUnit (Right (), newMock (coerce k) sources)
    PeekKey        -> \m -> wrap MKey (peekKeyMock m, m)
    Pop n          -> wrap wrapPop . popMock n
    DropWhileKey k -> wrap wrapDrop . dropWhileKeyMock k
  where
    wrap :: (a -> ReadersVal b) -> (Either () a, MockReaders) -> (ReadersVal (Either () b), MockReaders)
    wrap f = first (MEither . bimap MUnit f)

    wrapPop = MTuple3 . trimap MKey MEntry MHasMore

    wrapDrop = MTuple2 . bimap MInt MHasMore

trimap :: (a -> a') -> (b -> b') -> (c -> c') -> (a, b, c) -> (a', b', c')
trimap f g h (a, b, c) = (f a, g b, h c)

type RealMonad = ReaderT (FS.HasFS IO MockFS.HandleMock,
                          FS.HasBlockIO IO MockFS.HandleMock)
                         (StateT RealState IO)

runRealMonad :: FS.HasFS IO MockFS.HandleMock
             -> FS.HasBlockIO IO MockFS.HandleMock
             -> RealState
             -> RealMonad a
             -> IO (a, RealState)
runRealMonad hfs hbio st = (`runStateT` st) . (`runReaderT` (hfs, hbio))

data RealState =
    RealState
      !Int  -- ^ number of runs created so far (to generate fresh run numbers)
      !(Maybe ReadersCtx)

-- | Readers, together with their sources, so they can be cleaned up at the end
type ReadersCtx = ( [Readers.ReaderSource IO MockFS.HandleMock]
                  , Readers IO Handle
                  )

closeReadersCtx :: ReadersCtx -> IO ()
closeReadersCtx (sources, readers) = do
    Readers.close readers
    traverse_ closeReaderSource sources

closeReaderSource :: Readers.ReaderSource IO h -> IO ()
closeReaderSource = \case
    Readers.FromWriteBuffer _ wbblobs -> releaseRef wbblobs
    Readers.FromRun           run     -> releaseRef run
    Readers.FromReaders     _ sources -> traverse_ closeReaderSource sources

instance RunModel (Lockstep ReadersState) RealMonad where
  perform       = \_st -> runIO
  postcondition = Lockstep.postcondition
  monitoring    = Lockstep.monitoring (Proxy @RealMonad)

instance RunLockstep ReadersState RealMonad where
  observeReal _proxy = \case
      New {}          -> OEither . bimap OId OId
      PeekKey {}      -> OEither . bimap OId OId
      Pop {}          -> OEither . bimap OId (OTuple3 . trimap OId OId OId)
      DropWhileKey {} -> OEither . bimap OId (OTuple2 . bimap OId OId)

runIO :: LockstepAction ReadersState a -> LookUp -> RealMonad a
runIO act lu = case act of
    New offset srcDatas -> ReaderT $ \(hfs, hbio) -> do
      RealState numRuns mCtx <- get
      -- if runs are still being read, they need to be cleaned up
      traverse_ (liftIO . closeReadersCtx) mCtx
      counter <- liftIO $ newUniqCounter numRuns
      sources <- liftIO $ forM srcDatas (fromSourceData hfs hbio counter)
      newReaders <- liftIO $ do
        let offsetKey = maybe Readers.NoOffsetKey (Readers.OffsetKey . coerce) offset
        mreaders <- Readers.new resolve offsetKey sources
        -- TODO: tidy up cleanup code?
        case mreaders of
          Nothing -> do
            traverse_ closeReaderSource sources
            pure Nothing
          Just readers ->
            pure $ Just (sources, readers)
      -- TODO: unnecessarily convoluted, should we just drop the State monad?
      numRuns' <- liftIO $ uniqueToInt <$> incrUniqCounter counter
      put (RealState numRuns' newReaders)
      pure (Right ())
    PeekKey -> expectReaders $ \_ r -> do
      (,) HasMore <$> Readers.peekKey r
    Pop n | n <= 1 -> pop
    Pop n -> pop >>= \case
      Left  ()              -> pure (Left ())
      Right (_, _, Drained) -> pure (Left ()) -- n > 1, so n too big
      Right (_, _, HasMore) -> runIO (Pop (n-1)) lu
    DropWhileKey k -> expectReaders $ \_ r -> do
      (n, hasMore) <- Readers.dropWhileKey resolve r k
      pure (hasMore, (n, hasMore))
  where
    fromSourceData hfs hbio counter = \case
        FromWriteBufferData rd -> do
          n <- incrUniqCounter counter
          wbblobs <- WBB.new hfs (FS.mkFsPath [show (uniqueToInt n) <> ".wb.blobs"])
          let kops = unRunData (serialiseRunData rd)
          wb <- WB.fromMap <$> traverse (traverse (WBB.addBlob hfs wbblobs)) kops
          pure $ Readers.FromWriteBuffer wb wbblobs
        FromRunData rd -> do
          r <- unsafeCreateRun hfs hbio testSalt runParams (FS.mkFsPath []) counter $ serialiseRunData rd
          pure $ Readers.FromRun r
        FromReadersData ty rds -> do
          Readers.FromReaders ty <$> traverse (fromSourceData hfs hbio counter) rds

    pop = expectReaders $ \hfs r -> do
      (key, e, hasMore) <- Readers.pop resolve r
      fullEntry <- toMockEntry hfs e
      pure (hasMore, (key, fullEntry, hasMore))

    expectReaders ::
         (FS.HasFS IO MockFS.HandleMock -> Readers IO MockFS.HandleMock -> IO (HasMore, a))
      -> RealMonad (Either () a)
    expectReaders f =
        ReaderT $ \(hfs, _hbio) -> do
          get >>= \case
            RealState _ Nothing -> pure (Left ())
            RealState n (Just (sources, readers)) -> do
              (hasMore, x) <- liftIO $ f hfs readers
              case hasMore of
                HasMore ->
                  pure (Right x)
                Drained -> do
                  -- Readers is drained, clean up the sources
                  liftIO $ traverse_ closeReaderSource sources
                  put (RealState n Nothing)
                  pure (Right x)

    toMockEntry :: FS.HasFS IO MockFS.HandleMock -> Reader.Entry IO MockFS.HandleMock -> IO SerialisedEntry
    toMockEntry hfs = traverse (readRawBlobRef hfs) . Reader.toFullEntry
