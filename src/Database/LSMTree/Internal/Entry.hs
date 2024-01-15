{-# LANGUAGE DeriveTraversable #-}
module Database.LSMTree.Internal.Entry (
    Entry (..),
) where

data Entry v blobref
    = Insert !v
    | InsertWithBlob !v !blobref
    | Mupdate !v
    | Delete
  deriving (Show, Functor, Foldable, Traversable)
