{-# LANGUAGE PatternSynonyms #-}
{-# LANGUAGE TypeFamilies    #-}

{-# OPTIONS_GHC -Wno-orphans #-}
{-# OPTIONS_GHC -Wno-incomplete-uni-patterns #-}

module Test.ScheduledMergesQLS (tests) where

import           Control.Monad
import           Control.Monad.ST
import           Control.Tracer (Tracer, nullTracer)
import           Data.Bifunctor (Bifunctor (..))
import           Data.Constraint (Dict (..))
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe (fromJust)
import           Data.Proxy
import           Data.Semigroup (First (..))
import           Data.Void (Void)
import           Prelude hiding (lookup)

import           ScheduledMerges as LSM
import           Test.ScheduledMergesQLS.Op

import           Test.QuickCheck
import           Test.QuickCheck.StateModel hiding (lookUpVar, shrinkVar)
import           Test.QuickCheck.StateModel.Lockstep
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
prop_LSM = Lockstep.runActions (Proxy :: Proxy Model)

instance Show (LSM RealWorld) where
  show _ = "<< LSM RealWorld >>"

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

type ModelFunc r = Model -> (r, Model)

initModel :: Model
initModel = Model { mlsms = Map.empty }

resolveValueAndBlob :: (Value, Maybe Blob) -> (Value, Maybe Blob) -> (Value, Maybe Blob)
resolveValueAndBlob (v, b) (v', b') = (resolveValue v v', getFirst (First b <> First b'))

modelNew         ::                                           ModelFunc ModelLSM
modelInsert      :: ModelLSM -> Key -> Value -> Maybe Blob -> ModelFunc ()
modelDelete      :: ModelLSM -> Key ->                        ModelFunc ()
modelMupsert     :: ModelLSM -> Key -> Value ->               ModelFunc ()
modelLookup      :: ModelLSM -> Key ->                        ModelFunc (LookupResult Value Blob)
modelDuplicate   :: ModelLSM ->                               ModelFunc ModelLSM
modelUnions      :: [ModelLSM] ->                             ModelFunc ModelLSM
modelSupplyUnion :: ModelLSM -> NonNegative UnionCredits ->   ModelFunc ()
modelDump        :: ModelLSM ->                               ModelFunc (Map Key (Value, Maybe Blob))

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
    ANew    :: LSMConfig -> Action (Lockstep Model) (LSM RealWorld, Dep (LSM RealWorld))

    AInsert :: DepVar (LSM RealWorld)
            -> Either (ModelVar Model Key) Key -- to refer to a prior key
            -> Value
            -> Maybe Blob
            -> Action (Lockstep Model) (Key, Dep (LSM RealWorld))

    ADelete :: DepVar (LSM RealWorld)
            -> Either (ModelVar Model Key) Key
            -> Action (Lockstep Model) (Dep (LSM RealWorld))

    AMupsert :: ModelVar Model (LSM RealWorld)
             -> Either (ModelVar Model Key) Key
             -> Value
             -> Action (Lockstep Model) (Key)

    ALookup :: ModelVar Model (LSM RealWorld)
            -> Either (ModelVar Model Key) Key
            -> Action (Lockstep Model) (LookupResult Value Blob)

    ADuplicate :: DepVar (LSM RealWorld)
               -> Action (Lockstep Model) (LSM RealWorld, Dep (LSM RealWorld))

    AUnions :: [DepVar (LSM RealWorld)]
            -> Action (Lockstep Model) (LSM RealWorld, [Dep (LSM RealWorld)])

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

    MList :: [ModelValue Model a] -> ModelValue Model [a]
    MPair ::
         (ModelValue Model a, ModelValue Model b)
      -> ModelValue Model (a, b)
    MEither ::
         Either (ModelValue Model a) (ModelValue Model b)
      -> ModelValue Model (Either a b)

  data Observable Model a where
    ORef :: Observable Model (LSM RealWorld)
    OId  :: (Show a, Eq a) => a -> Observable Model a

    OList :: [Observable Model a] -> Observable Model [a]
    OPair ::
         (Observable Model a, Observable Model b)
      -> Observable Model (a, b)
    OEither ::
         Either (Observable Model a) (Observable Model b)
      -> Observable Model (Either a b)

  type ModelOp Model = Op

  observeModel (MLSM    _) = ORef
  observeModel (MUnit   x) = OId x
  observeModel (MInsert x) = OId x
  observeModel (MLookup x) = OId x
  observeModel (MDump   x) = OId x
  observeModel (MPair   x) = OPair $ bimap observeModel observeModel x
  observeModel (MEither x) = OEither $ bimap observeModel observeModel x
  observeModel (MList  xs) = OList $ map observeModel xs

  usedVars  ANew{}             = []
  usedVars (AInsert v evk _ _) = usedVarsDepVar v
                               : case evk of Left vk -> [SomeGVar vk]; _ -> []
  usedVars (ADelete v evk)     = usedVarsDepVar v
                               : case evk of Left vk -> [SomeGVar vk]; _ -> []
  usedVars (AMupsert v evk _)  = SomeGVar v
                               : case evk of Left vk -> [SomeGVar vk]; _ -> []
  usedVars (ALookup v evk)     = SomeGVar v
                               : case evk of Left vk -> [SomeGVar vk]; _ -> []
  usedVars (ADuplicate v)      = [usedVarsDepVar v]
  usedVars (AUnions vs)        = [usedVarsDepVar v | v <- vs]
  usedVars (ASupplyUnion v _)  = [SomeGVar v]
  usedVars (ADump v)           = [SomeGVar v]

  modelNextState = runModel

  arbitraryWithVars ctx model =
    case findLsmVars ctx of
      []   ->
        -- Generate a write buffer size and size ratio in the range [3,5] most
        -- of the time, sometimes in the range [1,10] to hit edge cases. 4 was
        -- the hard-coded default for both before it was made configurable.
        fmap Some $ ANew <$> (
            LSMConfig <$> frequency [(10, choose (1,10)), (90, choose (3,5))]
                      <*> frequency [(10, choose (2,10)), (90, choose (3,5))]
                      <*> elements [LazyLevelling, Levelling]
          )
      vars ->
        let kvars = findVars ctx (Proxy :: Proxy Key)
            existingKey = Left <$> elements kvars
            freshKey = Right <$> arbitrary @Key
        in frequency $
          -- inserts of potentially fresh keys
          [ (3, fmap Some $
                  AInsert <$> (Left <$> elements vars)
                          <*> freshKey
                          <*> arbitrary @Value
                          <*> arbitrary @(Maybe Blob))
          ]
          -- inserts of the same keys as used earlier
       ++ [ (1, fmap Some $
                  AInsert <$> (Left <$> elements vars)
                          <*> existingKey
                          <*> arbitrary @Value
                          <*> arbitrary @(Maybe Blob))
          | not (null kvars)
          ]
          -- deletes of arbitrary keys:
       ++ [ (1, fmap Some $
                  ADelete <$> (Left <$> elements vars)
                          <*> freshKey)
          ]
          -- deletes of the same key as inserted earlier:
       ++ [ (1, fmap Some $
                  ADelete <$> (Left <$> elements vars)
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
                  ADuplicate <$> (Left <$> elements vars))
          ]
       ++ [ (1, fmap Some $ do
                  -- bias towards binary, only go high when many tables exist
                  len <- elements ([2, 2] ++ take (length vars) [1..5])
                  AUnions <$> vectorOf len (Left <$> elements vars))
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
      ++ [ Some $ ANew conf { configMergePolicy = mp' }
         | mp' <- [ LazyLevelling | mp == Levelling ]
         ]
    where
      LSMConfig mwbs sr mp = conf

  shrinkWithVars ctx _model (AInsert var evk v b) =
       [ Some $ AInsert var' evk v b
       | var' <- shrinkDepVar ctx (findLsmVars ctx) (findLsmDepVars ctx) var
       ]
    ++ case evk of
        Right k ->
          [ Some $ AInsert var (Right k') v' b' | (k', v', b') <- shrink (k, v, b) ]
        Left _kv ->
          [ Some $ AInsert var (Right k') v' b' | (k', v', b') <- shrink (K 100, v, b) ]

  shrinkWithVars ctx _model (ADelete var evk) =
       [ Some $ ADelete var' evk
       | var' <- shrinkDepVar ctx (findLsmVars ctx) (findLsmDepVars ctx) var
       ]
    ++ case evk of
        Right k ->
          [ Some $ ADelete var (Right k') | k' <- shrink k ]
        Left _kv ->
          [ Some $ ADelete var (Right k) | k <- shrink (K 100) ]

  shrinkWithVars _ctx _model (AMupsert var (Right k) v) =
    [ Some $ AInsert (Left var) (Right k) v Nothing ] ++
    [ Some $ AMupsert var (Right k') v' | (k', v') <- shrink (k, v) ]

  shrinkWithVars _ctx _model (AMupsert var (Left kv) v) =
    [ Some $ AInsert (Left var) (Left kv) v Nothing ] ++
    [ Some $ AMupsert var (Right k') v' | (k', v') <- shrink (K 100, v) ]

  shrinkWithVars ctx _model (ADuplicate var) =
    [ Some $ ADuplicate var'
    | var' <- shrinkDepVar ctx (findLsmVars ctx) (findLsmDepVars ctx) var
    ]

  shrinkWithVars _ctx _model (AUnions [var]) =
    [ Some $ ADuplicate var ]

  shrinkWithVars ctx _model (AUnions vars) =
    [ Some $ AUnions vs | vs <- shrinkList (shrinkDepVar ctx (findLsmVars ctx) (findLsmDepVars ctx)) vars, not (null vs) ]

  shrinkWithVars _ctx _model (ASupplyUnion var c) =
    [ Some $ ASupplyUnion var c' | c' <- shrink c ]

  shrinkWithVars _ctx _model _action = []

findLsmVars :: ModelVarContext Model -> [GVar (ModelOp Model) (LSM RealWorld)]
findLsmVars ctx =
       -- Variables for results of 'ANew'
       findVars ctx (Proxy @(LSM RealWorld))
    ++ -- Variables for results of 'ADuplicate'
       [ mapGVar (OpFst `OpComp`) var
       | var <- findVars ctx (Proxy @(LSM RealWorld, Dep (LSM RealWorld)))
       ]
    ++ -- Variables for results of 'AUnions'
       [ mapGVar (OpFst `OpComp`) var
       | var <- findVars ctx (Proxy @(LSM RealWorld, [Dep (LSM RealWorld)]))
       ]

findLsmDepVars :: ModelVarContext Model -> [GVar (ModelOp Model) (Dep (LSM RealWorld))]
findLsmDepVars ctx =
       -- Variables for results of 'AInsert'
       [ mapGVar (OpSnd `OpComp`) var
       | var <- findVars ctx (Proxy @(Key, Dep (LSM RealWorld)))
       ]
    ++ -- Variables for results of 'Delete'
       [ var
       | var <- findVars ctx (Proxy @(Dep (LSM RealWorld)))
       ]
    ++ -- Variables for results of 'ADuplicate'
       [ mapGVar (OpSnd `OpComp`) var
       | var <- findVars ctx (Proxy @(LSM RealWorld, Dep (LSM RealWorld)))
       ]
    ++ -- Variables for results of 'AUnions'
       [ mapGVar (opIndex `OpComp` OpSnd `OpComp`) var
       | var <- findVars ctx (Proxy @(LSM RealWorld, [Dep (LSM RealWorld)]))
       , let MPair (_, MList deps) = lookupVar ctx var
       , opIndex <-
           [ OpIndex i
           | i <- [0..length deps - 1]
           ]
       ]

deriving newtype instance Arbitrary UnionCredits

instance RunLockstep Model IO where
  observeReal _ action result =
    case (action, result) of
      (ANew{},         x) -> OPair $ bimap (const ORef) (ODep . const ORef) x
      (AInsert{},      x) -> OPair $ bimap OId (ODep . const ORef) x
      (ADelete{},      _) -> ODep $ ORef
      (AMupsert{},     x) -> OId x
      (ALookup{},      x) -> OId x
      (ADuplicate{},   x) -> OPair $ bimap (const ORef) (ODep . const ORef) x
      (AUnions{},      x) -> OPair $ bimap (const ORef) (OList . fmap (ODep . const ORef)) x
      (ASupplyUnion{}, x) -> OId x
      (ADump{},        x) -> OId x

  showRealResponse _ ANew{}         = Just Dict
  showRealResponse _ AInsert{}      = Just Dict
  showRealResponse _ ADelete{}      = Just Dict
  showRealResponse _ AMupsert{}     = Just Dict
  showRealResponse _ ALookup{}      = Just Dict
  showRealResponse _ ADuplicate{}   = Just Dict
  showRealResponse _ AUnions{}      = Just Dict
  showRealResponse _ ASupplyUnion{} = Just Dict
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
    ANew conf           -> newWith conf >>= \table -> pure (table, Dep table)
    AInsert var evk v b -> insert tr table k v b >> pure (k, Dep table)
      where
        table = lookupDepTable var
        k = either lookUpVar id evk
    ADelete var evk     -> delete tr table k >> pure (Dep table)
      where
        table = lookupDepTable var
        k = either lookUpVar id evk
    AMupsert var evk v  -> mupsert tr (lookUpVar var) k v >> pure k
      where k = either lookUpVar id evk
    ALookup var evk     -> lookup (lookUpVar var) k
      where k = either lookUpVar id evk
    ADuplicate var      -> duplicate table >>= \table' -> pure (table', Dep table)
      where table = lookupDepTable var
    AUnions vars        -> unions tables >>= \table -> pure (table ,fmap Dep tables)
      where tables = map lookupDepTable vars
    ASupplyUnion var c  -> supplyUnionCredits (lookUpVar var) (getNonNegative c) >> pure ()
    ADump      var      -> logicalValue (lookUpVar var)
  where
    lookUpVar :: ModelVar Model a -> a
    lookUpVar = realLookupVar (Proxy :: Proxy IO) lookUp

    lookupDepTable :: DepVar a -> a
    lookupDepTable = realLookupDepVar lookUp

    tr :: Tracer (ST RealWorld) Event
    tr = nullTracer

runModel :: Action (Lockstep Model) a
         -> ModelVarContext Model
         -> Model
         -> (ModelValue Model a, Model)
runModel action ctx m =
  case action of
    ANew{} -> (MPair (MLSM mlsm, MDep (MLSM mlsm)), m')
      where (mlsm, m') = modelNew m

    AInsert var evk v b -> (MPair (MInsert k, MDep (MLSM table)), m')
      where ((), m') = modelInsert table k v b m
            table = lookUpLsmDepVar var
            k = either lookUpKeyVar id evk

    ADelete var evk -> (MDep (MLSM table), m')
      where ((), m') = modelDelete table k m
            table = lookUpLsmDepVar var
            k = either lookUpKeyVar id evk

    AMupsert var evk v -> (MInsert k, m')
      where ((), m') = modelMupsert (lookUpLsMVar var) k v m
            k = either lookUpKeyVar id evk

    ALookup var evk -> (MLookup mv, m')
      where (mv, m') = modelLookup (lookUpLsMVar var) k m
            k = either lookUpKeyVar id evk

    ADuplicate var -> (MPair (MLSM mlsm', MDep (MLSM mlsm)), m')
      where mlsm = lookUpLsmDepVar var
            (mlsm', m') = modelDuplicate mlsm m

    AUnions vars -> (MPair (MLSM mlsm', MList (fmap (MDep . MLSM) mlsms)), m')
      where mlsms = map lookUpLsmDepVar vars
            (mlsm', m') = modelUnions mlsms m

    ASupplyUnion var c -> (MUnit (), m')
      where ((), m') = modelSupplyUnion (lookUpLsMVar var) c m

    ADump var -> (MDump mapping, m)
      where (mapping, _) = modelDump (lookUpLsMVar var) m
  where
    lookUpLsMVar :: ModelVar Model (LSM RealWorld) -> ModelLSM
    lookUpLsMVar var = case lookupVar ctx var of MLSM r -> r

    lookUpLsmDepVar :: DepVar (LSM RealWorld) -> ModelLSM
    lookUpLsmDepVar var = case lookupDepVar ctx var of MLSM r -> r

    lookUpKeyVar :: ModelVar Model Key -> Key
    lookUpKeyVar var = case lookupVar ctx var of MInsert k -> k


instance InterpretOp Op (ModelValue Model) where
  intOp :: Op a b -> ModelValue Model a -> Maybe (ModelValue Model b)
  intOp = \case
    OpId -> \x -> Just x
    OpFst -> \case MPair (x, _) -> Just x
    OpSnd -> \case MPair (_, y) -> Just y
    OpFromLeft -> \case MEither e -> either Just (const Nothing) e
    OpFromRight -> \case MEither e -> either (const Nothing) Just e
    OpLeft -> Just . MEither . Left
    OpRight -> Just . MEither . Right
    OpComp f g -> intOp g >=> intOp f
    OpIndex i -> \case MList xs -> xs !? i

type Dep a = Either Void a

{-# COMPLETE Dep #-}

pattern Dep :: a -> Dep a
pattern Dep x = Right x

pattern OpUnDep :: Op (Dep a) a
pattern OpUnDep = OpFromRight

pattern MDep :: ModelValue Model a -> ModelValue Model (Dep a)
pattern MDep x = MEither (Right x)

pattern ODep :: Observable Model a -> Observable Model (Dep a)
pattern ODep x = OEither (Right x)

type DepVar a =
        Either
          (ModelVar Model a)
          (ModelVar Model (Dep a))

usedVarsDepVar :: DepVar a -> AnyGVar Op
usedVarsDepVar depVar = either SomeGVar SomeGVar depVar

getDepVar :: DepVar a -> ModelVar Model a
getDepVar dvar = either id (mapGVar (OpUnDep `OpComp`)) dvar

lookupDepVar :: ModelVarContext Model -> DepVar a -> ModelValue Model a
lookupDepVar ctx = lookupVar ctx . getDepVar

realLookupDepVar :: LookUp IO -> DepVar a -> a
realLookupDepVar lookUp = lookUp' . getDepVar
  where
    lookUp' = realLookupVar (Proxy @IO) lookUp

shrinkDepVar ::
     ModelVarContext Model
  -> [GVar Op a]
  -> [GVar Op (Dep a)]
  -> DepVar a
  -> [DepVar a]
shrinkDepVar ctx findNonDeps findDeps = \case
    Left z  ->
         (Left <$> shrinkVar ctx z)
      ++ (Left <$> [ x  | x <- findNonDeps, compareVarNumber x z == LT])
      ++ (Right <$> findDeps)
    Right z ->
         (Right <$> shrinkVar ctx z)
      ++ (Right <$> [ x | x <- findDeps, compareVarNumber x z == LT] )
