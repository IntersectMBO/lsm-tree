{-# LANGUAGE FlexibleContexts      #-}
{-# LANGUAGE FlexibleInstances     #-}
{-# LANGUAGE GADTs                 #-}
{-# LANGUAGE InstanceSigs          #-}
{-# LANGUAGE LambdaCase            #-}
{-# LANGUAGE MultiParamTypeClasses #-}

-- | SumProd Op extended with BlobRef extraction
module Test.Database.LSMTree.StateMachine.Op (
    -- * 'Op'
    Op (..)
  , intOpId
    -- * 'HasBlobRef' class
  , HasBlobRef (..)
  ) where

import           Control.Monad ((<=<))
import           Control.Monad.Identity (Identity (..))
import qualified Data.Vector as V
import qualified Database.LSMTree.Class as Class
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
  OpLookupResults      :: Op (V.Vector (Class.LookupResult v (WrapBlobRef h m b))) (V.Vector (WrapBlobRef h m b))
  OpEntrys             :: Op (V.Vector (Class.Entry k v (WrapBlobRef h m b))) (V.Vector (WrapBlobRef h m b))

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
intOpId OpEntrys        = Just . V.mapMaybe getBlobRef

{-------------------------------------------------------------------------------
  'InterpretOp' instances
-------------------------------------------------------------------------------}

instance Operation Op where
  opIdentity = OpId

instance InterpretOp Op Identity where
  intOp = Op.intOpIdentity intOpId

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
    go OpEntrys             OpEntrys        = True
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
        OpEntrys{}             -> ()


instance Eq (Op a b)  where
  (==) = sameOp

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
      go OpEntrys        = showString "OpEntrys"

{-------------------------------------------------------------------------------
  'HasBlobRef' class
-------------------------------------------------------------------------------}

class HasBlobRef f where
  getBlobRef :: f b  -> Maybe b

instance HasBlobRef (Class.LookupResult v) where
  getBlobRef Class.NotFound{}                = Nothing
  getBlobRef Class.Found{}                   = Nothing
  getBlobRef (Class.FoundWithBlob _ blobref) = Just blobref

instance HasBlobRef (Class.Entry k v) where
  getBlobRef Class.Entry{}                     = Nothing
  getBlobRef (Class.EntryWithBlob _ _ blobref) = Just blobref
