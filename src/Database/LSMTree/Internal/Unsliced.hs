{-# LANGUAGE DeriveFunctor              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE RoleAnnotations            #-}

module Database.LSMTree.Internal.Unsliced (
    -- * Unsliced raw bytes
    Unsliced
    -- * Unsliced keys
  , makeUnslicedKey
  , unsafeMakeUnslicedKey
  , fromUnslicedKey
  ) where

import           Control.DeepSeq (NFData)
import           Control.Exception (assert)
import           Data.Primitive.ByteArray
import qualified Data.Vector.Primitive as VP
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise (SerialisedKey (..))
import           Database.LSMTree.Internal.Vector (mkPrimVector,
                     noRetainedExtraMemory)

-- | Unsliced string of bytes
type role Unsliced nominal
newtype Unsliced a = Unsliced ByteArray
  deriving newtype NFData

getByteArray :: RawBytes -> ByteArray
getByteArray (RawBytes (VP.Vector _ _ ba)) = ba

precondition :: RawBytes -> Bool
precondition (RawBytes pvec) = noRetainedExtraMemory pvec

makeUnsliced :: RawBytes -> Unsliced RawBytes
makeUnsliced bytes
    | precondition bytes = Unsliced (getByteArray bytes)
    | otherwise     = Unsliced (getByteArray $ RB.copy bytes)

unsafeMakeUnsliced :: RawBytes -> Unsliced RawBytes
unsafeMakeUnsliced bytes = assert (precondition bytes) (Unsliced (getByteArray bytes))

fromUnsliced :: Unsliced RawBytes -> RawBytes
fromUnsliced (Unsliced ba) = RawBytes (mkPrimVector 0 (sizeofByteArray ba) ba)

{-------------------------------------------------------------------------------
  Unsliced keys
-------------------------------------------------------------------------------}

from :: Unsliced RawBytes -> Unsliced SerialisedKey
from (Unsliced ba) = Unsliced ba

to :: Unsliced SerialisedKey -> Unsliced RawBytes
to (Unsliced ba) = Unsliced ba

makeUnslicedKey :: SerialisedKey -> Unsliced SerialisedKey
makeUnslicedKey (SerialisedKey rb) = from (makeUnsliced rb)

unsafeMakeUnslicedKey :: SerialisedKey -> Unsliced SerialisedKey
unsafeMakeUnslicedKey (SerialisedKey rb) = from (unsafeMakeUnsliced rb)

fromUnslicedKey :: Unsliced SerialisedKey -> SerialisedKey
fromUnslicedKey x = SerialisedKey (fromUnsliced (to x))

instance Show (Unsliced SerialisedKey) where
  show x = show (fromUnslicedKey x)

instance Eq (Unsliced SerialisedKey) where
  x == y = fromUnslicedKey x == fromUnslicedKey y

instance Ord (Unsliced SerialisedKey) where
  x <= y = fromUnslicedKey x <= fromUnslicedKey y
