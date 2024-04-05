{-# LANGUAGE DeriveFunctor     #-}
{-# LANGUAGE FlexibleInstances #-}

module Database.LSMTree.Internal.Unsliced (
    -- * Unsliced raw bytes
    Unsliced
    -- * Unsliced keys
  , makeUnslicedKey
  , unsafeMakeUnslicedKey
  , fromUnslicedKey
  , unsafeNoAssertMakeUnslicedKey
  ) where

import           Control.Exception (assert)
import qualified Data.Vector.Primitive as PV
import           Data.Word (Word8)
import           Database.LSMTree.Internal.Serialise (SerialisedKey (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import           Database.LSMTree.Internal.Vector (noRetainedExtraMemory)

-- | A type @a@ represented as unsliced raw bytes.
newtype Unsliced a =
    UnsafeNoAssertUnsliced (PV.Vector Word8)
  deriving Functor

invariant :: Unsliced a -> Bool
invariant (UnsafeNoAssertUnsliced pvec) = noRetainedExtraMemory pvec

makeUnsliced :: RawBytes -> Unsliced RawBytes
makeUnsliced (RawBytes pvec)
    | invariant urb = urb
    | otherwise     = UnsafeNoAssertUnsliced (PV.force pvec)
    where urb = UnsafeNoAssertUnsliced pvec

unsafeMakeUnsliced :: RawBytes -> Unsliced RawBytes
unsafeMakeUnsliced (RawBytes pvec) = assert (invariant urb) urb
  where urb = UnsafeNoAssertUnsliced pvec

unsafeNoAssertMakeUnsliced :: RawBytes -> Unsliced RawBytes
unsafeNoAssertMakeUnsliced (RawBytes pvec) = UnsafeNoAssertUnsliced pvec

fromUnsliced :: Unsliced RawBytes -> RawBytes
fromUnsliced (UnsafeNoAssertUnsliced pvec) = RawBytes pvec

{-------------------------------------------------------------------------------
  Unsliced keys
-------------------------------------------------------------------------------}

makeUnslicedKey :: SerialisedKey -> Unsliced SerialisedKey
makeUnslicedKey (SerialisedKey rb) = SerialisedKey <$> makeUnsliced rb

unsafeMakeUnslicedKey :: SerialisedKey -> Unsliced SerialisedKey
unsafeMakeUnslicedKey (SerialisedKey rb) = SerialisedKey <$> unsafeMakeUnsliced rb

unsafeNoAssertMakeUnslicedKey :: SerialisedKey -> Unsliced SerialisedKey
unsafeNoAssertMakeUnslicedKey (SerialisedKey rb) =
    SerialisedKey <$> unsafeNoAssertMakeUnsliced rb

fromUnslicedKey :: Unsliced SerialisedKey -> SerialisedKey
fromUnslicedKey x = SerialisedKey (fromUnsliced ((\(SerialisedKey y) -> y) <$> x))

instance Show (Unsliced SerialisedKey) where
  show x = show (fromUnslicedKey x)

instance Eq (Unsliced SerialisedKey) where
  x == y = fromUnslicedKey x == fromUnslicedKey y

instance Ord (Unsliced SerialisedKey) where
  x <= y = fromUnslicedKey x <= fromUnslicedKey y
