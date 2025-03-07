{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.RunNumber (
    RunNumber (..),
    TableId (..),
    CursorId (..),
) where

import           Control.DeepSeq (NFData)

newtype RunNumber = RunNumber Int
  deriving stock (Eq, Ord, Show)
  deriving newtype (NFData)

newtype TableId = TableId Int
  deriving stock (Eq, Ord, Show)
  deriving newtype (NFData)

newtype CursorId = CursorId Int
  deriving stock (Eq, Ord, Show)
  deriving newtype (NFData)
