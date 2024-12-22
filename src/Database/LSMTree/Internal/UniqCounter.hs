module Database.LSMTree.Internal.UniqCounter (
  UniqCounter (..),
  newUniqCounter,
  incrUniqCounter,
  Unique,
  uniqueToWord64,
  uniqueToRunNumber,
) where

import           Control.Monad.Primitive
import           Data.Coerce (coerce)
import           Data.Primitive.PrimVar as P
import           Data.Word (Word64)
import           Database.LSMTree.Internal.RunNumber
import           Unsafe.Coerce (unsafeCoerce)

-- | A newtype wrapper around 'Word64'.
newtype Unique = Unique Word64

-- | Avoid this function, use specialised versions like 'uniqueToRunNumber' if possible.
uniqueToWord64 :: Unique -> Word64
uniqueToWord64 = coerce

uniqueToRunNumber :: Unique -> RunNumber
uniqueToRunNumber = coerce

-- | An atomic counter for producing 'Unique' values.
newtype UniqCounter m = UniqCounter (PrimVar (PrimState m) Int)
-- Note that it is safe to store the 64-bit value as an `Int` type.
-- The README.md clear states that the use must provide a machine architecture
-- with a 64-bit Int type.
--
-- Additionally, it is safe to @unsafeCoerce@ between `Int` and `Word64` for
-- the same reason, the use must guarantee a 2's compliment representation for
-- signed integral types. See thethis module's @convert@ function for more info.
--
-- This is important because, by storing the underlying 64-bit value as an @Int@
-- type, we can utilize the atomic "fetch-and-add" instructions provided by the
-- @primitive@ package.

-- |
-- This call to @unsafeCoerce@ relies on the machine's binary representation of
-- the @Int@ type to be safe. The binary representation must be 64-bots wide, a
-- given guarantee by the user according to the @README.md@. Additionally, the
-- binary representation of @Int@ must form an additive ring which is homomorphic
-- with @Word64@ (such as 2's compliment); i.e.:
--
-- @
-- let limit = maxBound :: Int
-- in  forall (x :: Int).
--       (unsafeCoerce limit :: Word64) + (unsafeCoerce x :: Word64) === unsafeCoerce (limit + x)
-- @
{-# INLINE convert #-}
convert :: Int -> Word64
convert = unsafeCoerce

{-# INLINE newUniqCounter #-}
newUniqCounter :: PrimMonad m => Word64 -> m (UniqCounter m)
newUniqCounter = fmap UniqCounter . P.newPrimVar . unsafeCoerce

{-# INLINE incrUniqCounter #-}
-- | Return the current state of the atomic counter, and then increment the
-- counter.
incrUniqCounter :: PrimMonad m => UniqCounter m -> m Unique
incrUniqCounter (UniqCounter uniqVar) =
  Unique . convert <$> P.fetchAddInt uniqVar 1
