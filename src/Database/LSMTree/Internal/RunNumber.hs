module Database.LSMTree.Internal.RunNumber (
    RunNumber (..),
) where

import           Control.DeepSeq (NFData)
import           Data.Word (Word64)

newtype RunNumber = RunNumber Word64
  deriving newtype (Eq, Ord, Show, NFData)
