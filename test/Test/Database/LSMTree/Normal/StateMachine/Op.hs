{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- | SumProd Op extended with BlobRef extraction
module Test.Database.LSMTree.Normal.StateMachine.Op (
    -- * 'Op'
    Op (..)
  , intOpId
    -- * 'HasBlobRef' class
  , HasBlobRef (..)
  ) where

import           Control.Monad ((<=<))
import           Control.Monad.IOSim (IOSim)
import           Control.Monad.Reader (ReaderT)
import           Control.Monad.State (StateT)
import qualified Data.Vector as V
import qualified Database.LSMTree.Class as Class
import qualified Database.LSMTree.Model.Table as Model
import           GHC.Show (appPrec)
import           Test.QuickCheck.StateModel.Lockstep (InterpretOp, Operation)
import qualified Test.QuickCheck.StateModel.Lockstep.Op as Op
import           Test.Util.Orphans ()
import           Test.Util.TypeFamilyWrappers (WrapBlobRef)

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
  OpLookupResults      :: Op (V.Vector (Class.LookupResult v (WrapBlobRef h IO blobref))) (V.Vector (WrapBlobRef h IO blobref))
  OpQueryResults       :: Op (V.Vector (Class.QueryResult k v (WrapBlobRef h IO blobref))) (V.Vector (WrapBlobRef h IO blobref))

intOpId :: Op a b -> a -> Maybe b
intOpId OpId            = Just
intOpId OpFst           = Just . fst
intOpId OpSnd           = Just . snd
intOpId OpLeft          = Just . Left
intOpId OpRight         = Just . Right
intOpId OpFromLeft      = either Just (const Nothing)
intOpId OpFromRight     = either (const Nothing) Just
intOpId (OpComp g f)    = intOpId g <=< intOpId f
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
      OpLookupResults      -> Just  . Op.WrapRealized . V.mapMaybe getBlobRef . Op.unwrapRealized
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
      go OpLookupResults = showString "OpLookupResults"
      go OpQueryResults  = showString "OpQueryResults"

{-------------------------------------------------------------------------------
  'HasBlobRef' class
-------------------------------------------------------------------------------}

class HasBlobRef f where
  getBlobRef :: f blobref  -> Maybe blobref

instance HasBlobRef (Class.LookupResult v) where
  getBlobRef Class.NotFound{}                = Nothing
  getBlobRef Class.Found{}                   = Nothing
  getBlobRef (Class.FoundWithBlob _ blobref) = Just blobref

instance HasBlobRef (Class.QueryResult k v) where
  getBlobRef Class.FoundInQuery{}                     = Nothing
  getBlobRef (Class.FoundInQueryWithBlob _ _ blobref) = Just blobref

instance HasBlobRef (Model.LookupResult v) where
  getBlobRef Model.NotFound{}                = Nothing
  getBlobRef Model.Found{}                   = Nothing
  getBlobRef (Model.FoundWithBlob _ blobref) = Just blobref

instance HasBlobRef (Model.QueryResult k v) where
  getBlobRef Model.FoundInQuery{}                     = Nothing
  getBlobRef (Model.FoundInQueryWithBlob _ _ blobref) = Just blobref
