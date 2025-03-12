{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE InstanceSigs               #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE StandaloneKindSignatures   #-}
{-# LANGUAGE TypeFamilies               #-}

{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Util.Orphans () where


import           Control.Monad.IOSim (IOSim)
import           Database.LSMTree.Common (IOLike, SerialiseKey, SerialiseValue)
import           Test.QuickCheck.Modifiers (Small (..))

instance IOLike (IOSim s)

{-------------------------------------------------------------------------------
  QuickCheck
-------------------------------------------------------------------------------}

deriving newtype instance SerialiseKey a => SerialiseKey (Small a)
deriving newtype instance SerialiseValue a => SerialiseValue (Small a)
