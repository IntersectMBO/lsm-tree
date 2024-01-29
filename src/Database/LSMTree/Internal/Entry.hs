{-# LANGUAGE DeriveTraversable #-}
module Database.LSMTree.Internal.Entry (
    Entry (..),
) where

import           Control.DeepSeq (NFData (..))

data Entry v blobref
    = Insert !v
    | InsertWithBlob !v !blobref
    | Mupdate !v
    | Delete
  deriving (Eq, Show, Functor, Foldable, Traversable)

instance (NFData v, NFData blobref) => NFData (Entry v blobref) where
    rnf (Insert v)            = rnf v
    rnf (InsertWithBlob v br) = rnf v `seq` rnf br
    rnf (Mupdate v)           = rnf v
    rnf Delete                = ()
