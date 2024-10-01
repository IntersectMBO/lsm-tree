{-# LANGUAGE LambdaCase   #-}
{-# LANGUAGE TypeFamilies #-}

module Test.Database.LSMTree.Internal.RunReaders (tests) where

import           Control.Exception (assert)
import           Control.Monad (zipWithM)
import           Control.Monad.IO.Class (liftIO)
import           Control.Monad.Trans.Reader (ReaderT (..))
import           Control.Monad.Trans.State (StateT (..), get, put)
import           Data.Bifunctor (bimap, first)
import           Data.Coerce (coerce)
import           Data.Foldable (toList, traverse_)
import qualified Data.Map.Strict as Map
import           Data.Proxy (Proxy (..))
import qualified Data.Vector as V
import           Data.Word (Word64)
import           Database.LSMTree.Extras (showPowersOf)
import           Database.LSMTree.Extras.Generators (KeyForIndexCompact (..))
import           Database.LSMTree.Extras.RunData
import           Database.LSMTree.Internal.BlobRef (readBlob)
import           Database.LSMTree.Internal.Entry
import qualified Database.LSMTree.Internal.Paths as Paths
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunNumber
import qualified Database.LSMTree.Internal.RunReader as Reader
import           Database.LSMTree.Internal.RunReaders
                     (HasMore (Drained, HasMore), Readers)
import qualified Database.LSMTree.Internal.RunReaders as Readers
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import qualified System.FS.API as FS
import qualified System.FS.BlockIO.API as FS
import qualified System.FS.BlockIO.Sim as FsSim
import qualified System.FS.Sim.MockFS as MockFS
import qualified System.FS.Sim.STM as FsSim
import qualified Test.QuickCheck as QC
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.Orphans ()

import           Test.QuickCheck.StateModel
import           Test.QuickCheck.StateModel.Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Defaults as Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Run as Lockstep

tests :: TestTree
tests = testGroup "Database.LSMTree.Internal.RunReaders"
    [ testProperty "prop_lockstep" $
        Lockstep.runActionsBracket (Proxy @ReadersState)
          mempty mempty $ \act () -> do
          (prop, mockFS) <- FsSim.runSimFS MockFS.empty $ \hfs -> do
            hbio <- FsSim.fromHasFS hfs
            (prop, RealState _ mCtx) <- runRealMonad hfs hbio
                                                     (RealState 0 Nothing) act
            traverse_ (closeReadersCtx hfs hbio) mCtx  -- close current readers
            return prop

          -- ensure that all handles have been closed
          return $ prop
              .&&. counterexample "file handles" (MockFS.numOpenHandles mockFS === 0)
    ]

--------------------------------------------------------------------------------

type SerialisedEntry = Entry SerialisedValue SerialisedBlob

type Handle = FS.Handle MockFS.HandleMock

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
        -> [RunData KeyForIndexCompact SerialisedValue SerialisedBlob]
        -> MockReaders
newMock offset =
      MockReaders . Map.assocs . Map.unions
    . zipWith (\i -> Map.mapKeysMonotonic (\k -> (k, RunNumber i))) [0..]
    . map (skip . unRunData . serialiseRunData)
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
      let (dropped, xs') = span ((== k) . fst . fst) xs
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
    New          :: Maybe KeyForIndexCompact  -- ^ optional offset
                 -> Maybe (RunData KeyForIndexCompact SerialisedValue SerialisedBlob)
                 -> [RunData KeyForIndexCompact SerialisedValue SerialisedBlob]
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
    -> ModelLookUp ReadersState
    -> ReadersState
    -> (ReadersVal a, ReadersState)
  modelNextState action lookUp (ReadersState mock) =
      ReadersState <$> runMock lookUp action mock

  usedVars :: LockstepAction ReadersState a -> [AnyGVar (ModelOp ReadersState)]
  usedVars = const []

  arbitraryWithVars ::
       ModelFindVariables ReadersState
    -> ReadersState
    -> Gen (Any (LockstepAction ReadersState))
  arbitraryWithVars _ (ReadersState mock)
    | isEmpty mock = do
        -- It's not allowed to keep using a drained RunReaders,
        -- we can only create a new one.
        wb <- arbitrary
        wbs <- vector =<< chooseInt (1, 10)
        let keys = map fst $ concatMap (Map.toList . unRunData) $ toList wb <> wbs
        offset <-
          if null keys
          then pure Nothing
          else oneof
                 [ liftArbitrary (elements (coerce keys))  -- existing key
                 , arbitrary                               -- any key
                 ]
        return $ Some $ New offset wb wbs
    | otherwise =
        QC.frequency $
          [ (5, pure (Some PeekKey))
          , (8, pure (Some (Pop 1)))
          , (1, Some . Pop <$> chooseInt (1, size mock))  -- drain a significant amount
          , (1, pure (Some (Pop (max 1 (size mock - 3)))))  -- drain almost everything
          , (1, pure (Some (Pop (size mock + 1)))) -- drain /more/ than available
          , (1, Some . DropWhileKey <$> arbitrary)  -- most likely nothing to drop
          ] <>
          [ (4, pure (Some (DropWhileKey k)))  -- drops at least one key
          | Right k <- [peekKeyMock mock]
          ]

  shrinkWithVars ::
       ModelFindVariables ReadersState
    -> ReadersState
    -> LockstepAction ReadersState a
    -> [Any (LockstepAction ReadersState)]
  shrinkWithVars _ _ = \case
      New k wb wbs      -> [ Some (New k' wb' wbs')
                           | (k', wb', wbs') <- shrink (k, wb, wbs)
                           ]
      -- arbitraryWithVars does /not/ have an invariant that n is less than
      -- the number of elements available. The only invariant to preserve when
      -- shrinking is to keep n greater than 0.
      Pop n             -> Some . Pop <$> filter (> 0) (shrink n)
      _                 -> []

  tagStep ::
       (ReadersState, ReadersState)
    -> LockstepAction ReadersState a
    -> ReadersVal a
    -> [String]
  tagStep (ReadersState before, ReadersState after) action _result = concat
    -- Directly using strings, since there is only a small number of tags.
    [ [ "NewEntries " <> showPowersOf 10 numEntries
      | New _ wb wbs <- [action]
      , let numEntries = sum (map (Map.size . unRunData) (toList wb <> wbs))
      ]
    , [ "NewEntriesKeyDuplicates " <> showPowersOf 2 keyCount
      | New _ wb wbs <- [action]
      , let keyCounts = Map.unionsWith (+) (map (Map.map (const 1) . unRunData) (toList wb <> wbs))
      , keyCount <- Map.elems keyCounts
      , keyCount > 1
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
     lookUp
  -> Action (Lockstep ReadersState) a
  -> MockReaders
  -> (ReadersVal a, MockReaders)
runMock _ = \case
    New k wb wbs   -> const $ wrap MUnit (Right (), newMock (coerce k) (toList wb <> wbs))
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
      !Word64  -- ^ number of runs created so far (to generate fresh run numbers)
      !(Maybe ReadersCtx)

-- | Readers, together with the runs being read, so they can be cleaned up at the end
type ReadersCtx = (WBB.WriteBufferBlobs IO MockFS.HandleMock,
                   [Run.Run IO Handle],
                   Readers IO Handle)

closeReadersCtx :: FS.HasFS IO MockFS.HandleMock -> FS.HasBlockIO IO MockFS.HandleMock -> ReadersCtx -> IO ()
closeReadersCtx hfs hbio (wbblobs, runs, readers) = do
    Readers.close hfs hbio readers
    traverse_ Run.removeReference runs
    WBB.removeReference wbblobs

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

runIO :: LockstepAction ReadersState a -> LookUp RealMonad -> RealMonad (Realized RealMonad a)
runIO act lu = case act of
    New offset wb wbs -> ReaderT $ \(hfs, hbio) -> do
      RealState numRuns mCtx <- get
      -- if runs are still being read, they need to be cleaned up
      traverse_ (liftIO . closeReadersCtx hfs hbio) mCtx
      let wb' = fmap serialiseRunData wb
          wbs' = fmap serialiseRunData wbs
      runs <-
        zipWithM
          (\p -> liftIO . unsafeFlushAsWriteBuffer hfs hbio p)
          (Paths.RunFsPaths (FS.mkFsPath []) . RunNumber <$> [numRuns ..])
          wbs'
      newReaders <- liftIO $ do
        wbblobs <- WBB.new hfs (FS.mkFsPath [show numRuns <> ".wb.blobs"])
        wb'' <- traverse (fmap (flip (,) wbblobs . WB.fromMap) .
                         traverse (traverse (WBB.addBlob hfs wbblobs)) .
                         unRunData )
                        wb'
        let offsetKey = maybe Readers.NoOffsetKey (Readers.OffsetKey . coerce) offset
        mreaders <- Readers.new hfs hbio offsetKey wb'' (V.fromList runs)
        case mreaders of
          Nothing -> do
            traverse_ Run.removeReference runs
            WBB.removeReference wbblobs
            return Nothing
          Just readers ->
            return $ Just (wbblobs, runs, readers)
      put (RealState (numRuns + fromIntegral (length wbs)) newReaders)
      return (Right ())
    PeekKey -> expectReaders $ \_ _ r -> do
      (,) HasMore <$> Readers.peekKey r
    Pop n | n <= 1 -> pop
    Pop n -> pop >>= \case
      Left  ()              -> return (Left ())
      Right (_, _, Drained) -> return (Left ()) -- n > 1, so n too big
      Right (_, _, HasMore) -> runIO (Pop (n-1)) lu
    DropWhileKey k -> expectReaders $ \hfs hbio r -> do
      (n, hasMore) <- Readers.dropWhileKey hfs hbio r k
      return (hasMore, (n, hasMore))
  where
    pop = expectReaders $ \hfs hbio r -> do
      (key, e, hasMore) <- Readers.pop hfs hbio r
      fullEntry <- toMockEntry hfs e
      return (hasMore, (key, fullEntry, hasMore))

    expectReaders ::
         (FS.HasFS IO MockFS.HandleMock -> FS.HasBlockIO IO MockFS.HandleMock -> Readers IO Handle -> IO (HasMore, a))
      -> RealMonad (Either () a)
    expectReaders f =
        ReaderT $ \(hfs, hbio) -> do
          get >>= \case
            RealState _ Nothing -> return (Left ())
            RealState n (Just (wbblobs, runs, readers)) -> do
              (hasMore, x) <- liftIO $ f hfs hbio readers
              case hasMore of
                HasMore ->
                  return (Right x)
                Drained -> do
                  -- Readers is drained, clean up the runs
                  liftIO $ traverse_ Run.removeReference runs
                  liftIO $ WBB.removeReference wbblobs
                  put (RealState n Nothing)
                  return (Right x)

    toMockEntry :: FS.HasFS IO MockFS.HandleMock -> Reader.Entry IO Handle -> IO SerialisedEntry
    toMockEntry hfs = traverse (readBlob hfs) . Reader.toFullEntry
