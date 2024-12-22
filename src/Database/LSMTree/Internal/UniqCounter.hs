module Database.LSMTree.Internal.UniqCounter (
  UniqCounter (..),
  newUniqCounter,
  incrUniqCounter,
  Unique,
  uniqueToWord64,
  uniqueToRunNumber,
) where

import           Control.Monad.Primitive (PrimMonad, PrimState)
import           Data.Primitive.PrimVar as P
import           Data.Word (Word64)
import           Database.LSMTree.Internal.RunNumber

-- | A unique value derived from a 'UniqCounter'.
newtype Unique = Unique Int

-- | Avoid this function, use specialised versions like 'uniqueToRunNumber' if possible.
uniqueToWord64 :: Unique -> Word64
uniqueToWord64 (Unique n) = fromIntegral n

uniqueToRunNumber :: Unique -> RunNumber
uniqueToRunNumber (Unique n) = RunNumber (fromIntegral n)

-- | An atomic counter for producing 'Unique' values.
--
newtype UniqCounter m = UniqCounter (PrimVar (PrimState m) Int)

{-# INLINE newUniqCounter #-}
newUniqCounter :: PrimMonad m => Int -> m (UniqCounter m)
newUniqCounter = fmap UniqCounter . P.newPrimVar

{-# INLINE incrUniqCounter #-}
-- | Atomically, return the current state of the counter, and increment the
-- counter.
incrUniqCounter :: PrimMonad m => UniqCounter m -> m Unique
incrUniqCounter (UniqCounter uniqVar) =
  Unique <$> P.fetchAddInt uniqVar 1
