{-# LANGUAGE TypeFamilies #-}

{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Test.ScheduledMergesQLS (tests) where

import           Control.Monad.Reader (ReaderT (..))
import           Control.Monad.ST
import           Control.Tracer (Tracer, nullTracer)
import           Data.Constraint (Dict (..))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import           Data.Primitive.PrimVar
import           Data.Proxy
import           Data.Semigroup (First (..))
import           Prelude hiding (lookup)

import           ScheduledMerges as LSM

import           Test.QuickCheck
import           Test.QuickCheck.StateModel hiding (lookUpVar)
import           Test.QuickCheck.StateModel.Lockstep hiding (ModelOp)
import qualified Test.QuickCheck.StateModel.Lockstep.Defaults as Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Run as Lockstep
import           Test.Tasty
import           Test.Tasty.QuickCheck (testProperty)

tests :: TestTree
tests = testGroup "Test.ScheduledMergesQLS" [
      testProperty "ScheduledMerges vs model" $ mapSize (*10) prop_LSM  -- still <10s
    ]

-- TODO: add tagging, e.g. how often ASupplyUnion makes progress or completes a
-- union merge.
prop_LSM :: Actions (Lockstep Model) -> Property
prop_LSM =
    Lockstep.runActionsBracket
      (Proxy :: Proxy Model)
      (newPrimVar (TableId 0))
      (\_ -> pure ())
      runReaderT

-------------------------------------------------------------------------------
-- QLS infrastructure
--

type ModelLSM = Int

newtype Model = Model { mlsms :: Map ModelLSM Table }
  deriving stock (Show)

data Table = Table {
      tableContent  :: !(Map Key (Value, Maybe Blob))
    , tableHasUnion :: !Bool
    }
  deriving stock Show

emptyTable :: Table
emptyTable = Table Map.empty False

onContent ::
     (Map Key (Value, Maybe Blob) -> Map Key (Value, Maybe Blob))
  -> ModelLSM
  -> Model
  -> Model
onContent f k (Model m) = Model (Map.adjust f' k m)
  where
    f' t = t { tableContent = f (tableContent t) }

type ModelOp r = Model -> (r, Model)

initModel :: Model
initModel = Model { mlsms = Map.empty }

resolveValueAndBlob :: (Value, Maybe Blob) -> (Value, Maybe Blob) -> (Value, Maybe Blob)
resolveValueAndBlob (v, b) (v', b') = (resolveValue v v', getFirst (First b <> First b'))

modelNew         ::                                           ModelOp ModelLSM
modelInsert      :: ModelLSM -> Key -> Value -> Maybe Blob -> ModelOp ()
modelDelete      :: ModelLSM -> Key ->                        ModelOp ()
modelMupsert     :: ModelLSM -> Key -> Value ->               ModelOp ()
modelLookup      :: ModelLSM -> Key ->                        ModelOp (LookupResult Value Blob)
modelDuplicate   :: ModelLSM ->                               ModelOp ModelLSM
modelUnions      :: [ModelLSM] ->                             ModelOp ModelLSM
modelSupplyUnion :: ModelLSM -> NonNegative UnionCredits ->   ModelOp ()
modelDump        :: ModelLSM ->                               ModelOp (Map Key (Value, Maybe Blob))

modelNew Model {mlsms} =
    (mlsm, Model { mlsms = Map.insert mlsm emptyTable mlsms })
  where
    mlsm = Map.size mlsms

modelInsert mlsm k v b model =
    ((), onContent (Map.insert k (v, b)) mlsm model)

modelDelete mlsm k model =
    ((), onContent (Map.delete k) mlsm model)

modelMupsert mlsm k v model =
    ((), onContent (Map.insertWith resolveValueAndBlob k (v, Nothing)) mlsm model)

modelLookup mlsm k model@Model {mlsms} =
    (result, model)
  where
    Just mval = Map.lookup mlsm mlsms
    result    = case Map.lookup k (tableContent mval) of
                  Nothing      -> NotFound
                  Just (v, mb) -> Found v mb

modelDuplicate mlsm Model {mlsms} =
    (mlsm', Model { mlsms = Map.insert mlsm' mval mlsms })
  where
    Just mval = Map.lookup mlsm mlsms
    mlsm'     = Map.size mlsms

modelUnions inputs Model {mlsms} =
    (mlsm', Model { mlsms = Map.insert mlsm' mval' mlsms })
  where
    contents   = map (\i -> tableContent (fromJust (Map.lookup i mlsms))) inputs
    hasUnion   = True
    mval'      = Table (Map.unionsWith resolveValueAndBlob contents) hasUnion
    mlsm'      = Map.size mlsms

modelSupplyUnion _mlsm _credit model =
    ((), model)

modelDump mlsm model@Model {mlsms} =
    (mval, model)
  where
    Just (Table mval _) = Map.lookup mlsm mlsms

instance StateModel (Lockstep Model) where
  data Action (Lockstep Model) a where
    ANew    :: LSMConfig -> Action (Lockstep Model) (LSM RealWorld)

    AInsert :: ModelVar Model (LSM RealWorld)
            -> Either (ModelVar Model Key) Key -- to refer to a prior key
            -> Value
            -> Maybe Blob
            -> Action (Lockstep Model) (Key)

    ADelete :: ModelVar Model (LSM RealWorld)
            -> Either (ModelVar Model Key) Key
            -> Action (Lockstep Model) ()

    AMupsert :: ModelVar Model (LSM RealWorld)
             -> Either (ModelVar Model Key) Key
             -> Value
             -> Action (Lockstep Model) (Key)

    ALookup :: ModelVar Model (LSM RealWorld)
            -> Either (ModelVar Model Key) Key
            -> Action (Lockstep Model) (LookupResult Value Blob)

    ADuplicate :: ModelVar Model (LSM RealWorld)
               -> Action (Lockstep Model) (LSM RealWorld)

    AUnions :: [ModelVar Model (LSM RealWorld)]
            -> Action (Lockstep Model) (LSM RealWorld)

    ASupplyUnion :: ModelVar Model (LSM RealWorld)
                 -> NonNegative UnionCredits
                 -> Action (Lockstep Model) ()

    ADump   :: ModelVar Model (LSM RealWorld)
            -> Action (Lockstep Model) (Map Key (Value, Maybe Blob))

  initialState    = Lockstep.initialState initModel
  nextState       = Lockstep.nextState
  precondition    = Lockstep.precondition
  arbitraryAction = Lockstep.arbitraryAction
  shrinkAction    = Lockstep.shrinkAction

instance RunModel (Lockstep Model) (ReaderT (PrimVar RealWorld TableId) IO) where
  perform       = \_state -> runActionIO
  postcondition = Lockstep.postcondition
  monitoring    = Lockstep.monitoring (Proxy :: Proxy (ReaderT (PrimVar RealWorld TableId) IO))

instance InLockstep Model where
  data ModelValue Model a where
    MLSM    :: ModelLSM
            -> ModelValue Model (LSM RealWorld)
    MUnit   :: ()
            -> ModelValue Model ()
    MInsert :: Key
            -> ModelValue Model Key
    MLookup :: LookupResult Value Blob
            -> ModelValue Model (LookupResult Value Blob)
    MDump   :: Map Key (Value, Maybe Blob)
            -> ModelValue Model (Map Key (Value, Maybe Blob))

  data Observable Model a where
    ORef :: Observable Model (LSM RealWorld)
    OId  :: (Show a, Eq a) => a -> Observable Model a

  observeModel (MLSM    _) = ORef
  observeModel (MUnit   x) = OId x
  observeModel (MInsert x) = OId x
  observeModel (MLookup x) = OId x
  observeModel (MDump   x) = OId x

  usedVars  ANew{}             = []
  usedVars (AInsert v evk _ _) = SomeGVar v
                               : case evk of Left vk -> [SomeGVar vk]; _ -> []
  usedVars (ADelete v evk)     = SomeGVar v
                               : case evk of Left vk -> [SomeGVar vk]; _ -> []
  usedVars (AMupsert v evk _)  = SomeGVar v
                               : case evk of Left vk -> [SomeGVar vk]; _ -> []
  usedVars (ALookup v evk)     = SomeGVar v
                               : case evk of Left vk -> [SomeGVar vk]; _ -> []
  usedVars (ADuplicate v)      = [SomeGVar v]
  usedVars (AUnions vs)        = [SomeGVar v | v <- vs]
  usedVars (ASupplyUnion v _)  = [SomeGVar v]
  usedVars (ADump v)           = [SomeGVar v]

  modelNextState = runModel

  arbitraryWithVars ctx model =
    case findVars ctx (Proxy :: Proxy (LSM RealWorld)) of
      []   ->
        -- Generate a write buffer size and size ratio in the range [3,5] most
        -- of the time, sometimes in the range [1,10] to hit edge cases. 4 was
        -- the hard-coded default for both before it was made configurable.
        fmap Some $ ANew <$> (
            LSMConfig <$> frequency [(10, choose (1,10)), (90, choose (3,5))]
                      <*> frequency [(10, choose (2,10)), (90, choose (3,5))]
          )
      vars ->
        let kvars = findVars ctx (Proxy :: Proxy Key)
            existingKey = Left <$> elements kvars
            freshKey = Right <$> arbitrary @Key
        in frequency $
          -- inserts of potentially fresh keys
          [ (3, fmap Some $
                  AInsert <$> elements vars
                          <*> freshKey
                          <*> arbitrary @Value
                          <*> arbitrary @(Maybe Blob))
          ]
          -- inserts of the same keys as used earlier
       ++ [ (1, fmap Some $
                  AInsert <$> elements vars
                          <*> existingKey
                          <*> arbitrary @Value
                          <*> arbitrary @(Maybe Blob))
          | not (null kvars)
          ]
          -- deletes of arbitrary keys:
       ++ [ (1, fmap Some $
                  ADelete <$> elements vars
                          <*> freshKey)
          ]
          -- deletes of the same key as inserted earlier:
       ++ [ (1, fmap Some $
                  ADelete <$> elements vars
                          <*> existingKey)
          | not (null kvars)
          ]
          -- mupserts of potentially fresh keys
       ++ [ (1, fmap Some $
                  AMupsert <$> elements vars
                           <*> freshKey
                           <*> arbitrary @Value)
          ]
          -- mupserts of the same keys as used earlier
       ++ [ (1, fmap Some $
                  AMupsert <$> elements vars
                           <*> existingKey
                           <*> arbitrary @Value)
          | not (null kvars)
          ]
          -- lookup of arbitrary keys:
       ++ [ (1, fmap Some $
                  ALookup <$> elements vars
                          <*> freshKey)
          ]
          -- lookup of the same key as inserted earlier:
       ++ [ (3, fmap Some $
                  ALookup <$> elements vars
                          <*> existingKey)
          | not (null kvars)
          ]
       ++ [ (1, fmap Some $
                  ADuplicate <$> elements vars)
          ]
       ++ [ (1, fmap Some $ do
                  -- bias towards binary, only go high when many tables exist
                  len <- elements ([2, 2] ++ take (length vars) [1..5])
                  AUnions <$> vectorOf len (elements vars))
          ]
          -- only supply to tables with unions
       ++ [ (2, fmap Some $
                  ASupplyUnion <$> elements varsWithUnion
                               <*> arbitrary)
          | let hasUnion v = let MLSM m = lookupVar ctx v in
                             case Map.lookup m (mlsms model) of
                               Nothing -> False
                               Just t  -> tableHasUnion t
          , let varsWithUnion = filter hasUnion vars
          , not (null varsWithUnion)
          ]
       ++ [ (1, fmap Some $
                  ADump <$> elements vars)
          ]

  shrinkWithVars _ctx _model (ANew conf) =
         [ Some $ ANew conf { configMaxWriteBufferSize = mwbs' }
         | mwbs' <- shrink mwbs
         , mwbs' >= 1, mwbs' <= 10
         ]
      ++ [ Some $ ANew conf { configSizeRatio = sr' }
         | sr' <- shrink sr
         , sr' >= 2, sr' <= 10
         ]
    where
      LSMConfig mwbs sr = conf

  shrinkWithVars _ctx _model (AInsert var (Right k) v b) =
    [ Some $ AInsert var (Right k') v' b' | (k', v', b') <- shrink (k, v, b) ]

  shrinkWithVars _ctx _model (AInsert var (Left _kv) v b) =
    [ Some $ AInsert var (Right k') v' b' | (k', v', b') <- shrink (K 100, v, b) ]

  shrinkWithVars _ctx _model (ADelete var (Right k)) =
    [ Some $ ADelete var (Right k') | k' <- shrink k ]

  shrinkWithVars _ctx _model (ADelete var (Left _kv)) =
    [ Some $ ADelete var (Right k) | k <- shrink (K 100) ]

  shrinkWithVars _ctx _model (AMupsert var (Right k) v) =
    [ Some $ AInsert var (Right k) v Nothing ] ++
    [ Some $ AMupsert var (Right k') v' | (k', v') <- shrink (k, v) ]

  shrinkWithVars _ctx _model (AMupsert var (Left kv) v) =
    [ Some $ AInsert var (Left kv) v Nothing ] ++
    [ Some $ AMupsert var (Right k') v' | (k', v') <- shrink (K 100, v) ]

  shrinkWithVars _ctx _model (AUnions [var]) =
    [ Some $ ADuplicate var ]

  shrinkWithVars _ctx _model (AUnions vars) =
    [ Some $ AUnions vs | vs <- shrinkList (const []) vars, not (null vs) ]

  shrinkWithVars _ctx _model (ASupplyUnion var c) =
    [ Some $ ASupplyUnion var c' | c' <- shrink c ]

  shrinkWithVars _ctx _model _action = []

deriving newtype instance Arbitrary UnionCredits

instance RunLockstep Model (ReaderT (PrimVar RealWorld TableId) IO) where
  observeReal _ action result =
    case (action, result) of
      (ANew{},         _) -> ORef
      (AInsert{},      x) -> OId x
      (ADelete{},      x) -> OId x
      (AMupsert{},     x) -> OId x
      (ALookup{},      x) -> OId x
      (ADuplicate{},   _) -> ORef
      (AUnions{},      _) -> ORef
      (ASupplyUnion{}, x) -> OId x
      (ADump{},        x) -> OId x

  showRealResponse _ ANew{}         = Nothing
  showRealResponse _ AInsert{}      = Just Dict
  showRealResponse _ ADelete{}      = Just Dict
  showRealResponse _ AMupsert{}     = Just Dict
  showRealResponse _ ALookup{}      = Just Dict
  showRealResponse _ ADuplicate{}   = Nothing
  showRealResponse _ AUnions{}      = Nothing
  showRealResponse _ ASupplyUnion{} = Nothing
  showRealResponse _ ADump{}        = Just Dict

deriving stock instance Show (Action (Lockstep Model) a)
deriving stock instance Show (Observable Model a)
deriving stock instance Show (ModelValue Model a)

deriving stock instance Eq (Action (Lockstep Model) a)
deriving stock instance Eq (Observable Model a)
deriving stock instance Eq (ModelValue Model a)


runActionIO :: Action (Lockstep Model) a
            -> LookUp
            -> ReaderT (PrimVar RealWorld TableId) IO a
runActionIO action lookUp = ReaderT $ \tidVar -> do
  case action of
    ANew conf           -> do
      tid <- incrTidVar tidVar
      stToIO $ newWith tr tid conf
    AInsert var evk v b -> stToIO $ insert tr (lookUpVar var) k v b >> pure k
      where k = either lookUpVar id evk
    ADelete var evk     -> stToIO$ delete tr (lookUpVar var) k >> pure ()
      where k = either lookUpVar id evk
    AMupsert var evk v  -> stToIO $ mupsert tr (lookUpVar var) k v >> pure k
      where k = either lookUpVar id evk
    ALookup var evk     -> stToIO $ lookup tr (lookUpVar var) k
      where k = either lookUpVar id evk
    ADuplicate var      -> do
      tid <- incrTidVar tidVar
      stToIO $ duplicate tr tid (lookUpVar var)
    AUnions vars        -> do
      tid <- incrTidVar tidVar
      stToIO $ unions tr tid (map lookUpVar vars)
    ASupplyUnion var c  -> stToIO $ supplyUnionCredits (lookUpVar var) (getNonNegative c) >> pure ()
    ADump      var      -> stToIO $ logicalValue (lookUpVar var)
  where
    lookUpVar :: ModelVar Model a -> a
    lookUpVar = realLookupVar lookUp

    tr :: Tracer (ST RealWorld) Event
    tr = nullTracer

    incrTidVar :: PrimVar RealWorld TableId -> IO TableId
    incrTidVar tidVar = do
        tid@(TableId x) <- readPrimVar tidVar
        writePrimVar tidVar (TableId (x + 1))
        pure tid

runModel :: Action (Lockstep Model) a
         -> ModelVarContext Model
         -> Model
         -> (ModelValue Model a, Model)
runModel action ctx m =
  case action of
    ANew{} -> (MLSM mlsm, m')
      where (mlsm, m') = modelNew m

    AInsert var evk v b -> (MInsert k, m')
      where ((), m') = modelInsert (lookUpLsMVar var) k v b m
            k = either lookUpKeyVar id evk

    ADelete var evk -> (MUnit (), m')
      where ((), m') = modelDelete (lookUpLsMVar var) k m
            k = either lookUpKeyVar id evk

    AMupsert var evk v -> (MInsert k, m')
      where ((), m') = modelMupsert (lookUpLsMVar var) k v m
            k = either lookUpKeyVar id evk

    ALookup var evk -> (MLookup mv, m')
      where (mv, m') = modelLookup (lookUpLsMVar var) k m
            k = either lookUpKeyVar id evk

    ADuplicate var -> (MLSM mlsm', m')
      where (mlsm', m') = modelDuplicate (lookUpLsMVar var) m

    AUnions vars -> (MLSM mlsm', m')
      where (mlsm', m') = modelUnions (map lookUpLsMVar vars) m

    ASupplyUnion var c -> (MUnit (), m')
      where ((), m') = modelSupplyUnion (lookUpLsMVar var) c m

    ADump var -> (MDump mapping, m)
      where (mapping, _) = modelDump (lookUpLsMVar var) m
  where
    lookUpLsMVar :: ModelVar Model (LSM RealWorld) -> ModelLSM
    lookUpLsMVar var = case lookupVar ctx var of MLSM r -> r

    lookUpKeyVar :: ModelVar Model Key -> Key
    lookUpKeyVar var = case lookupVar ctx var of MInsert k -> k
