{-# LANGUAGE TypeFamilies #-}

module ScheduledMergesTestQLS (tests) where

import           Control.Monad.ST
import           Control.Tracer (Tracer, nullTracer)
import           Data.Constraint (Dict (..))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import           Data.Monoid (First (..))
import           Data.Proxy
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
tests =
    testProperty "ScheduledMerges vs model" $ mapSize (*10) prop_LSM  -- still <10s

prop_LSM :: Actions (Lockstep Model) -> Property
prop_LSM = Lockstep.runActions (Proxy :: Proxy Model)

-------------------------------------------------------------------------------
-- QLS infrastructure
--

type ModelLSM = Int

newtype Model = Model { mlsms :: Map ModelLSM (Map Key (Value, Maybe Blob)) }
  deriving stock (Show)

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
modelSupplyUnion :: ModelLSM -> NonNegative Credit ->         ModelOp ()
modelDump        :: ModelLSM ->                               ModelOp (Map Key (Value, Maybe Blob))

modelNew Model {mlsms} =
    (mlsm, Model { mlsms = Map.insert mlsm Map.empty mlsms })
  where
    mlsm = Map.size mlsms

modelInsert mlsm k v b Model {mlsms} =
    ((), Model { mlsms = Map.adjust (Map.insert k (v, b)) mlsm mlsms })

modelDelete mlsm k Model {mlsms} =
    ((), Model { mlsms = Map.adjust (Map.delete k) mlsm mlsms })

modelMupsert mlsm k v Model {mlsms} =
    ((), Model { mlsms = Map.adjust (Map.insertWith resolveValueAndBlob k (v, Nothing)) mlsm mlsms })

modelLookup mlsm k model@Model {mlsms} =
    (result, model)
  where
    Just mval = Map.lookup mlsm mlsms
    result    = case Map.lookup k mval of
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
    mvals      = map (\i -> fromJust (Map.lookup i mlsms)) inputs
    mval'      = Map.unionsWith resolveValueAndBlob mvals
    mlsm'      = Map.size mlsms

modelSupplyUnion _mlsm _credit model =
    ((), model)

modelDump mlsm model@Model {mlsms} =
    (mval, model)
  where
    Just mval = Map.lookup mlsm mlsms

instance StateModel (Lockstep Model) where
  data Action (Lockstep Model) a where
    ANew    :: Action (Lockstep Model) (LSM RealWorld)

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
                 -> NonNegative Credit
                 -> Action (Lockstep Model) ()

    ADump   :: ModelVar Model (LSM RealWorld)
            -> Action (Lockstep Model) (Map Key (Value, Maybe Blob))

  initialState    = Lockstep.initialState initModel
  nextState       = Lockstep.nextState
  precondition    = Lockstep.precondition
  arbitraryAction = Lockstep.arbitraryAction
  shrinkAction    = Lockstep.shrinkAction

instance RunModel (Lockstep Model) IO where
  perform       = \_state -> runActionIO
  postcondition = Lockstep.postcondition
  monitoring    = Lockstep.monitoring (Proxy :: Proxy IO)

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

  usedVars  ANew               = []
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

  arbitraryWithVars ctx _model =
    case findVars ctx (Proxy :: Proxy (LSM RealWorld)) of
      []   -> return (Some ANew)
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
                  len <- elements [1..5]
                  AUnions <$> vectorOf len (elements vars))
          ]
          -- TODO: only supply to tables with unions?
       ++ [ (1, fmap Some $
                  ASupplyUnion <$> elements vars
                               <*> arbitrary)
          ]
       ++ [ (1, fmap Some $
                  ADump <$> elements vars)
          ]

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


instance RunLockstep Model IO where
  observeReal _ action result =
    case (action, result) of
      (ANew,           _) -> ORef
      (AInsert{},      x) -> OId x
      (ADelete{},      x) -> OId x
      (AMupsert{},     x) -> OId x
      (ALookup{},      x) -> OId x
      (ADuplicate{},   _) -> ORef
      (AUnions{},      _) -> ORef
      (ASupplyUnion{}, x) -> OId x
      (ADump{},        x) -> OId x

  showRealResponse _ ANew           = Nothing
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
            -> LookUp IO
            -> IO a
runActionIO action lookUp =
  stToIO $
  case action of
    ANew                -> new
    AInsert var evk v b -> insert tr (lookUpVar var) k v b >> return k
      where k = either lookUpVar id evk
    ADelete var evk     -> delete tr (lookUpVar var) k >> return ()
      where k = either lookUpVar id evk
    AMupsert var evk v  -> mupsert tr (lookUpVar var) k v >> return k
      where k = either lookUpVar id evk
    ALookup var evk     -> lookup (lookUpVar var) k
      where k = either lookUpVar id evk
    ADuplicate var      -> duplicate (lookUpVar var)
    AUnions vars        -> unions (map lookUpVar vars)
    ASupplyUnion var c  -> supplyUnionCredits (lookUpVar var) (getNonNegative c) >> return ()
    ADump      var      -> logicalValue (lookUpVar var)
  where
    lookUpVar :: ModelVar Model a -> a
    lookUpVar = lookUpGVar (Proxy :: Proxy IO) lookUp

    tr :: Tracer (ST RealWorld) Event
    tr = nullTracer

runModel :: Action (Lockstep Model) a
         -> ModelVarContext Model
         -> Model
         -> (ModelValue Model a, Model)
runModel action ctx m =
  case action of
    ANew -> (MLSM mlsm, m')
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
