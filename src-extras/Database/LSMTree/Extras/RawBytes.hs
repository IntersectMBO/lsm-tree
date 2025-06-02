{-# LANGUAGE TypeFamilies #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Extras.RawBytes () where

import           Data.Word
import           Database.LSMTree.Internal.RawBytes
import           GHC.Exts

{- |
@'fromList'@: \(O(n)\).

@'toList'@: \(O(n)\).
-}
instance IsList RawBytes where
  type Item RawBytes = Word8

  fromList :: [Item RawBytes] -> RawBytes
  fromList = pack

  toList :: RawBytes -> [Item RawBytes]
  toList = unpack

{- |
@'fromString'@: \(O(n)\).

__Warning:__ 'fromString' truncates multi-byte characters to octets. e.g. \"枯朶に烏のとまりけり秋の暮\" becomes \"�6k�nh~�Q��n�\".
-}
instance IsString RawBytes where
    fromString = fromByteString . fromString
