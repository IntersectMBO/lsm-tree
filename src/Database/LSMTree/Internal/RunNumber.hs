module Database.LSMTree.Internal.RunNumber (
    RunNumber (..),
) where

import           Control.DeepSeq (NFData)
import           Data.Word (Word64)

newtype RunNumber = RunNumber Word64
  deriving newtype (Eq, Ord, Show, NFData)

-- read as Word64
-- the Read instance is used in Internal.open ?!?
deriving newtype instance Read RunNumber
