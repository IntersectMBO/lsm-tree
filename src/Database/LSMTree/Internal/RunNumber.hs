module Database.LSMTree.Internal.RunNumber (
    RunNumber (..),
    TableId (..),
    CursorId (..),
) where

import           Control.DeepSeq (NFData)

newtype RunNumber = RunNumber Int
  deriving newtype (Eq, Ord, Show, NFData)

newtype TableId = TableId Int
  deriving newtype (Eq, Ord, Show, NFData)

newtype CursorId = CursorId Int
  deriving newtype (Eq, Ord, Show, NFData)
