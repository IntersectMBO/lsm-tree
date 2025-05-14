-- | The code here is largely copied from
-- "Test.QuickCheck.StateModel.Lockstep.Op.SumProd", but extended with support
-- for lists.
module Test.ScheduledMergesQLS.Op (Op(..), (!?), intOpId) where

import           Control.Monad ((<=<))
import           Control.Monad.Reader (ReaderT)
import           Control.Monad.State (StateT)
import           GHC.Show (appPrec)
import           Test.QuickCheck.StateModel.Lockstep.Op

-- | Operations with support for products, sums, and lists
data Op a b where
  OpId        :: Op a a
  OpFst       :: Op (a, b) a
  OpSnd       :: Op (b, a) a
  OpFromLeft  :: Op (Either a b) a
  OpFromRight :: Op (Either a b) b
  OpLeft      :: Op a (Either a b)
  OpRight     :: Op b (Either a b)
  OpComp      :: Op b c -> Op a b -> Op a c
  OpIndex     :: Int -> Op [a] a

intOpId :: Op a b -> a -> Maybe b
intOpId OpId         = Just
intOpId OpFst        = Just . fst
intOpId OpSnd        = Just . snd
intOpId OpFromLeft   = either Just (const Nothing)
intOpId OpFromRight  = either (const Nothing) Just
intOpId OpLeft       = Just . Left
intOpId OpRight      = Just . Right
intOpId (OpComp g f) = intOpId g <=< intOpId f
intOpId (OpIndex i)  = \x -> x !? i

infixl 9  !?

-- | Only available in more recent versions of @base@, so we copied the code
-- here instead.
(!?) :: [a] -> Int -> Maybe a
{-# INLINABLE (!?) #-}
xs !? n
  | n < 0     = Nothing
  | otherwise = foldr (\x r k -> case k of
                                   0 -> Just x
                                   _ -> r (k-1)) (const Nothing) xs n

{-------------------------------------------------------------------------------
  'InterpretOp' instances
-------------------------------------------------------------------------------}

instance Operation Op where
  opIdentity = OpId

instance InterpretOp Op (WrapRealized IO) where
  intOp = intOpRealizedId intOpId

instance InterpretOp Op (WrapRealized m)
      => InterpretOp Op (WrapRealized (StateT s m)) where
  intOp = intOpTransformer

instance InterpretOp Op (WrapRealized m)
      => InterpretOp Op (WrapRealized (ReaderT r m)) where
  intOp = intOpTransformer

{-------------------------------------------------------------------------------
  'Show' and 'Eq' instances
-------------------------------------------------------------------------------}

sameOp :: Op a b -> Op c d -> Bool
sameOp = go
  where
    go :: Op a b -> Op c d -> Bool
    go OpId         OpId           = True
    go OpFst        OpFst          = True
    go OpSnd        OpSnd          = True
    go OpFromLeft OpFromLeft       = True
    go OpFromRight OpFromRight     = True
    go OpLeft       OpLeft         = True
    go OpRight      OpRight        = True
    go (OpComp g f) (OpComp g' f') = go g g' && go f f'
    go (OpIndex i)  (OpIndex i')   = i == i'
    go _            _              = False

    _coveredAllCases :: Op a b -> ()
    _coveredAllCases = \case
        OpId    -> ()
        OpFst   -> ()
        OpSnd   -> ()
        OpFromLeft -> ()
        OpFromRight -> ()
        OpLeft  -> ()
        OpRight -> ()
        OpComp{} -> ()
        OpIndex{} -> ()

instance Eq (Op a b)  where
  (==) = sameOp

instance Show (Op a b) where
  showsPrec p = \op -> case op of
      OpComp{} -> showParen (p > appPrec) (go op)
      _        -> go op
    where
      go :: Op x y -> String -> String
      go OpId         = showString "OpId"
      go OpFst        = showString "OpFst"
      go OpSnd        = showString "OpSnd"
      go OpFromLeft   = showString "OpFromLeft"
      go OpFromRight  = showString "OpFromRight"
      go OpLeft       = showString "OpLeft"
      go OpRight      = showString "OpRight"
      go (OpComp g f) = go g . showString " `OpComp` " . go f
      go (OpIndex i)  = showString "OpIndex " . shows i
