module Database.LSMTree.Internal.TempRegistry (
    TempRegistry
  , newTempRegistry
  , releaseTempRegistry
  , allocateTemp
  ) where

import           Control.Concurrent.Class.MonadMVar.Strict
import           Control.Monad.Class.MonadThrow
import qualified Data.Vector as V

-- | A temporary registry for resources that are bound to end up in some final
-- state, after which they /should/ be guaranteed to be released correctly.
--
-- It is the responsibility of the user to guarantee that this final state is
-- released correctly in the presence of (async) exceptions.
--
-- NOTE: we could use an even more proper abstraction for this /temporary
-- registry/ pattern, because it is a pattern that is bound to show up more
-- often. An example of such an abstraction is the @WithTempRegistry@ from
-- @ouroboros-consensus@:
-- https://github.com/IntersectMBO/ouroboros-consensus/blob/main/ouroboros-consensus/src/ouroboros-consensus/Ouroboros/Consensus/Util/ResourceRegistry.hs
newtype TempRegistry m a = TempRegistry (StrictMVar m (V.Vector a))

{-# SPECIALISE newTempRegistry :: IO (TempRegistry IO a) #-}
newTempRegistry :: MonadMVar m => m (TempRegistry m a)
newTempRegistry = TempRegistry <$> newMVar V.empty

{-# SPECIALISE releaseTempRegistry :: TempRegistry IO a -> (a -> IO ()) -> IO () #-}
releaseTempRegistry :: MonadMVar m => TempRegistry m a -> (a -> m ()) -> m ()
releaseTempRegistry (TempRegistry var) free = do
    xs <- takeMVar var
    V.mapM_ free xs

{-# SPECIALISE allocateTemp :: TempRegistry IO a -> IO a -> IO a #-}
allocateTemp :: (MonadMask m, MonadMVar m) => TempRegistry m a -> m a -> m a
allocateTemp (TempRegistry var) acquire =
    mask_ $ do
      x <- acquire
      modifyMVar_ var (pure . V.cons x)
      pure x
