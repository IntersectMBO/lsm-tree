{-# LANGUAGE PolyKinds #-}

module Test.Util.PrettyProxy (
    PrettyProxy (..)
  ) where

import           Data.Typeable

-- | A version of 'Proxy' that also shows its type parameter.
data PrettyProxy a = PrettyProxy

-- | Shows the type parameter @a@ as an explicit type application.
instance Typeable a => Show (PrettyProxy a) where
  showsPrec d p =
        showParen (d > app_prec)
      $ showString "PrettyProxy @("
      . showString (show (typeRep p))
      . showString ")"
    where app_prec = 10
