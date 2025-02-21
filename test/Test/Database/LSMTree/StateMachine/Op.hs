{-# LANGUAGE TypeFamilies #-}

{-# OPTIONS_GHC -Wno-orphans #-}

-- | SumProd Op extended with BlobRef extraction
module Test.Database.LSMTree.StateMachine.Op (
    -- * 'Op'
    Op (..)
  , intOpId
    -- * 'HasBlobRef' class
  , HasBlobRef (..)
    -- * Dependent shrinking
  , Dep (..)
  , DepVar
  , getDepVar
  , lookupDepVar
  , shrinkDepVar
  ) where

import           Control.Concurrent.Class.MonadMVar (MonadMVar (..))
import           Control.Concurrent.Class.MonadSTM (MonadSTM (..))
import qualified Control.Concurrent.MVar as Real
import qualified Control.Concurrent.STM as Real
import           Control.Monad ((<=<))
import           Control.Monad.IOSim (IOSim)
import           Control.Monad.Reader (ReaderT)
import           Control.Monad.State (StateT)
import           Data.Data (Proxy (Proxy))
import           Data.Kind (Type)
import qualified Data.Vector as V
import           Database.LSMTree (Cursor, LookupResult, QueryResult, Table)
import qualified Database.LSMTree.Class as Class
import           Database.LSMTree.Common (BlobRef)
import           GHC.Show (appPrec)
import           Test.QuickCheck.StateModel (LookUp, Realized)
import           Test.QuickCheck.StateModel.Lockstep
import qualified Test.QuickCheck.StateModel.Lockstep.Op as Op
import           Test.Util.TypeFamilyWrappers (WrapBlob (..), WrapBlobRef (..),
                     WrapCursor, WrapTable (..))

{-------------------------------------------------------------------------------
  'Op'
-------------------------------------------------------------------------------}

data Op a b where
  OpId                 :: Op a a
  OpFst                :: Op (a, b) a
  OpSnd                :: Op (b, a) a
  OpLeft               :: Op a (Either a b)
  OpRight              :: Op b (Either a b)
  OpFromLeft           :: Op (Either a b) a
  OpFromRight          :: Op (Either a b) b
  OpComp               :: Op b c -> Op a b -> Op a c
  OpUnDep              :: Op (Dep a) a
  OpLookupResults      :: Op (V.Vector (Class.LookupResult v (WrapBlobRef h IO b))) (V.Vector (WrapBlobRef h IO b))
  OpQueryResults       :: Op (V.Vector (Class.QueryResult k v (WrapBlobRef h IO b))) (V.Vector (WrapBlobRef h IO b))

intOpId :: Op a b -> a -> Maybe b
intOpId OpId            = Just
intOpId OpFst           = Just . fst
intOpId OpSnd           = Just . snd
intOpId OpLeft          = Just . Left
intOpId OpRight         = Just . Right
intOpId OpFromLeft      = either Just (const Nothing)
intOpId OpFromRight     = either (const Nothing) Just
intOpId (OpComp g f)    = intOpId g <=< intOpId f
intOpId OpUnDep         = Just . unDep
intOpId OpLookupResults = Just . V.mapMaybe getBlobRef
intOpId OpQueryResults  = Just . V.mapMaybe getBlobRef

{-------------------------------------------------------------------------------
  'InterpretOp' instances
-------------------------------------------------------------------------------}

instance Operation Op where
  opIdentity = OpId

instance InterpretOp Op (Op.WrapRealized IO) where
  intOp ::
       Op a b
    -> Op.WrapRealized IO a
    -> Maybe (Op.WrapRealized IO b)
  intOp = Op.intOpRealizedId intOpId

instance InterpretOp Op (Op.WrapRealized m)
      => InterpretOp Op (Op.WrapRealized (StateT s m)) where
  intOp = Op.intOpTransformer

instance InterpretOp Op (Op.WrapRealized m)
      => InterpretOp Op (Op.WrapRealized (ReaderT r m)) where
  intOp = Op.intOpTransformer

instance InterpretOp Op (Op.WrapRealized (IOSim s)) where
  intOp ::
       Op a b
    -> Op.WrapRealized (IOSim s) a
    -> Maybe (Op.WrapRealized (IOSim s) b)
  intOp = \case
      OpId                 -> Just
      OpFst                -> Just . Op.WrapRealized . fst . Op.unwrapRealized
      OpSnd                -> Just . Op.WrapRealized . snd . Op.unwrapRealized
      OpLeft               -> Just . Op.WrapRealized . Left . Op.unwrapRealized
      OpRight              -> Just . Op.WrapRealized . Right . Op.unwrapRealized
      OpFromLeft           -> either (Just . Op.WrapRealized) (const Nothing) . Op.unwrapRealized
      OpFromRight          -> either (const Nothing) (Just . Op.WrapRealized) . Op.unwrapRealized
      OpComp g f           -> Op.intOp g <=< Op.intOp f
      OpUnDep              -> Just . Op.WrapRealized . unDep . Op.unwrapRealized
      OpLookupResults      -> Just . Op.WrapRealized . V.mapMaybe getBlobRef . Op.unwrapRealized
      OpQueryResults       -> Just . Op.WrapRealized . V.mapMaybe getBlobRef . Op.unwrapRealized

{-------------------------------------------------------------------------------
  'Show' and 'Eq' instances
-------------------------------------------------------------------------------}

sameOp :: Op a b -> Op c d -> Bool
sameOp = go
  where
    go :: Op a b -> Op c d -> Bool
    go OpId                 OpId            = True
    go OpFst                OpFst           = True
    go OpSnd                OpSnd           = True
    go OpLeft               OpLeft          = True
    go OpRight              OpRight         = True
    go OpFromLeft           OpFromLeft      = True
    go OpFromRight          OpFromRight     = True
    go (OpComp g f)         (OpComp g' f')  = go g g' && go f f'
    go OpUnDep              OpUnDep         = True
    go OpLookupResults      OpLookupResults = True
    go OpQueryResults       OpQueryResults  = True
    go _                    _               = False

    _coveredAllCases :: Op a b -> ()
    _coveredAllCases = \case
        OpId                   -> ()
        OpFst                  -> ()
        OpSnd                  -> ()
        OpLeft                 -> ()
        OpRight                -> ()
        OpFromLeft             -> ()
        OpFromRight            -> ()
        OpComp{}               -> ()
        OpUnDep                -> ()
        OpLookupResults{}      -> ()
        OpQueryResults{}       -> ()


instance Eq (Op a b)  where
  (==) = sameOp

-- TODO: parentheses
instance Show (Op a b) where
  showsPrec :: Int -> Op a b -> ShowS
  showsPrec p = \op -> case op of
      OpComp{} -> showParen (p > appPrec) (go op)
      _        -> go op
    where
      go :: Op x y -> String -> String
      go OpId            = showString "OpId"
      go OpFst           = showString "OpFst"
      go OpSnd           = showString "OpSnd"
      go OpLeft          = showString "OpLeft"
      go OpRight         = showString "OpRight"
      go OpFromLeft      = showString "OpFromLeft"
      go OpFromRight     = showString "OpFromRight"
      go (OpComp g f)    = go g . showString " `OpComp` " . go f
      go OpUnDep         = showString "OpUnDep"
      go OpLookupResults = showString "OpLookupResults"
      go OpQueryResults  = showString "OpQueryResults"

{-------------------------------------------------------------------------------
  IOSim
-------------------------------------------------------------------------------}

type instance Realized (IOSim s) a = RealizeIOSim s a

type RealizeIOSim :: Type -> Type -> Type
type family RealizeIOSim s a where
  -- io-classes
  RealizeIOSim s (Real.TVar a)  = TVar (IOSim s) a
  RealizeIOSim s (Real.TMVar a) = TMVar (IOSim s) a
  RealizeIOSim s (Real.MVar a)  = MVar (IOSim s) a
  -- lsm-tree
  RealizeIOSim s (Table IO k v b)    = Table (IOSim s) k v b
  RealizeIOSim s (LookupResult v b)  = LookupResult v (RealizeIOSim s b)
  RealizeIOSim s (QueryResult k v b) = QueryResult k v (RealizeIOSim s b)
  RealizeIOSim s (Cursor IO k v b)   = Table (IOSim s) k v b
  RealizeIOSim s (BlobRef IO b)      = BlobRef (IOSim s) b
  -- Type family wrappers
  RealizeIOSim s (WrapTable h IO k v b)  = WrapTable h (IOSim s) k v b
  RealizeIOSim s (WrapCursor h IO k v b) = WrapCursor h (IOSim s) k v b
  RealizeIOSim s (WrapBlobRef h IO b)    = WrapBlobRef h (IOSim s) b
  RealizeIOSim s (WrapBlob b)            = WrapBlob b
  -- Congruence
  RealizeIOSim s (f a b) = f (RealizeIOSim s a) (RealizeIOSim s b)
  RealizeIOSim s (f a)   = f (RealizeIOSim s a)
  -- Default
  RealizeIOSim s a = a

{-------------------------------------------------------------------------------
  'HasBlobRef' class
-------------------------------------------------------------------------------}

class HasBlobRef f where
  getBlobRef :: f b  -> Maybe b

instance HasBlobRef (Class.LookupResult v) where
  getBlobRef Class.NotFound{}                = Nothing
  getBlobRef Class.Found{}                   = Nothing
  getBlobRef (Class.FoundWithBlob _ blobref) = Just blobref

instance HasBlobRef (Class.QueryResult k v) where
  getBlobRef Class.FoundInQuery{}                     = Nothing
  getBlobRef (Class.FoundInQueryWithBlob _ _ blobref) = Just blobref

{-------------------------------------------------------------------------------
  Dependent shrinking
-------------------------------------------------------------------------------}

newtype Dep a = Dep { unDep :: a }
  deriving stock (Show, Eq, Functor)

type DepVar state a =
        Either
          (ModelVar state a)
          (ModelVar state (Dep a))

getDepVar ::
     ModelOp state ~ Op
  => Proxy state
  -> DepVar state a
  -> ModelVar state a
getDepVar _ dvar = either id (mapGVar (OpUnDep `OpComp`)) dvar

lookupDepVar ::
     forall m state a. (ModelOp state ~ Op, InterpretOp Op (Op.WrapRealized m))
  => Proxy m -> Proxy state -> LookUp m -> DepVar state a -> Realized m a
lookupDepVar _ _ lookUp = lookUp' . getDepVar (Proxy @state)
  where
    lookUp' = lookUpGVar (Proxy @m) lookUp

shrinkDepVar ::
     ModelOp state ~ Op
  => ModelVarContext state
  -> [GVar Op (Dep a)]
  -> DepVar state a
  -> [DepVar state a]
shrinkDepVar ctx findDeps = \case
    Left z  -> (Left <$> shrinkVar ctx z) ++ (Right <$> findDeps)
    Right z -> Right <$> shrinkVar ctx z
