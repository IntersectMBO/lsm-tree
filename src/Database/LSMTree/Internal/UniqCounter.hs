{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.UniqCounter (
  UniqCounter (..),
  newUniqCounter,
  incrUniqCounter,
  Unique,
  uniqueToInt,
  uniqueToRunNumber,
  uniqueToTableId,
  uniqueToCursorId,
) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Data.Primitive.PrimVar as P
import           Database.LSMTree.Internal.RunNumber

-- | A unique value derived from a 'UniqCounter'.
newtype Unique = Unique Int

-- | Use specialised versions like 'uniqueToRunNumber' where possible.
uniqueToInt :: Unique -> Int
uniqueToInt (Unique n) = n

uniqueToRunNumber :: Unique -> RunNumber
uniqueToRunNumber (Unique n) = RunNumber n

uniqueToTableId :: Unique -> TableId
uniqueToTableId (Unique n) = TableId n

uniqueToCursorId :: Unique -> CursorId
uniqueToCursorId (Unique n) = CursorId n

-- | An atomic counter for producing 'Unique' values.
--
newtype UniqCounter m = UniqCounter (PrimVar (PrimState m) Int)

instance NFData (UniqCounter m) where
  rnf (UniqCounter (P.PrimVar mba)) = rnf mba

{-# INLINE newUniqCounter #-}
newUniqCounter :: PrimMonad m => Int -> m (UniqCounter m)
newUniqCounter = fmap UniqCounter . P.newPrimVar

{-# INLINE incrUniqCounter #-}
-- | Atomically, return the current state of the counter, and increment the
-- counter.
incrUniqCounter :: PrimMonad m => UniqCounter m -> m Unique
incrUniqCounter (UniqCounter uniqVar) =
  Unique <$> P.fetchAddInt uniqVar 1
