module Database.LSMTree.Internal.UniqCounter (
  UniqCounter (..),
  newUniqCounter,
  incrUniqCounter,
  Unique,
  uniqueToWord64,
  uniqueToRunNumber,
) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Data.Coerce (coerce)
import           Data.Word (Word64)
import           Database.LSMTree.Internal.RunNumber

-- | A newtype wrapper around 'Word64'.
newtype Unique = Unique Word64

-- | Avoid this function, use specialised versions like 'uniqueToRunNumber' if possible.
uniqueToWord64 :: Unique -> Word64
uniqueToWord64 = coerce

uniqueToRunNumber :: Unique -> RunNumber
uniqueToRunNumber = coerce

-- | An atomic counter for producing 'Unique' values.
newtype UniqCounter m = UniqCounter (StrictMVar m Word64)

{-# INLINE newUniqCounter #-}
newUniqCounter :: MonadMVar m => Word64 -> m (UniqCounter m)
newUniqCounter x = UniqCounter <$> newMVar x

{-# INLINE incrUniqCounter #-}
-- | Return the current state of the atomic counter, and then increment the
-- counter.
incrUniqCounter :: MonadMVar m => UniqCounter m -> m Unique
incrUniqCounter (UniqCounter uniqVar) = modifyMVar uniqVar (\x -> pure ((x+1), Unique x))
