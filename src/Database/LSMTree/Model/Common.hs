{-# LANGUAGE RoleAnnotations     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}
module Database.LSMTree.Model.Common (
    SomeSerialisationConstraint (..)
  , SomeUpdateConstraint (..)
  ) where

import           Data.Bits (shiftR, (.&.))
import qualified Data.ByteString as BS
import           Data.Word (Word64)

class SomeSerialisationConstraint a where
    serialise :: a -> BS.ByteString

    -- Note: cannot fail.
    deserialise :: BS.ByteString -> a

instance SomeSerialisationConstraint BS.ByteString where
    serialise = id
    deserialise = id

class SomeUpdateConstraint a where
    merge :: a -> a -> a

instance SomeUpdateConstraint BS.ByteString where
    merge = (<>)

-- | MSB, so order is preserved.
instance SomeSerialisationConstraint Word64 where
    serialise w = BS.pack [b1,b2,b3,b4,b5,b6,b7,b8] where
        b8 = fromIntegral $        w    .&. 0xff
        b7 = fromIntegral $ shiftR w  8 .&. 0xff
        b6 = fromIntegral $ shiftR w 16 .&. 0xff
        b5 = fromIntegral $ shiftR w 24 .&. 0xff
        b4 = fromIntegral $ shiftR w 32 .&. 0xff
        b3 = fromIntegral $ shiftR w 40 .&. 0xff
        b2 = fromIntegral $ shiftR w 48 .&. 0xff
        b1 = fromIntegral $ shiftR w 56 .&. 0xff

    deserialise = BS.foldl' (\acc d -> acc * 0x100 + fromIntegral d) 0
