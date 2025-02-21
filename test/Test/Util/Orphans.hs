{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Util.Orphans () where

import           Control.Monad.IOSim (IOSim)
import           Database.LSMTree.Common (IOLike, SerialiseValue)
import           Database.LSMTree.Internal.Serialise (SerialiseKey)
import           Test.QuickCheck.Modifiers (Small (..))

{-------------------------------------------------------------------------------
  IOSim
-------------------------------------------------------------------------------}

instance IOLike (IOSim s)
{-------------------------------------------------------------------------------
  QuickCheck
-------------------------------------------------------------------------------}

deriving newtype instance SerialiseKey a => SerialiseKey (Small a)
deriving newtype instance SerialiseValue a => SerialiseValue (Small a)
