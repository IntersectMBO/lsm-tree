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

import           Database.LSMTree (SerialiseKey, SerialiseValue)
import           Test.QuickCheck.Modifiers (Small (..))

{-------------------------------------------------------------------------------
  QuickCheck
-------------------------------------------------------------------------------}

deriving newtype instance SerialiseKey a => SerialiseKey (Small a)
deriving newtype instance SerialiseValue a => SerialiseValue (Small a)
