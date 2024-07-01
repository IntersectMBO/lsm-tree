{-# LANGUAGE CPP #-}
module Data.Arena (
    ArenaManager,
    newArenaManager,
    Arena,
    Size,
    Offset,
    Alignment,
    withArena,

    allocateFromArena,
    -- * Test helpers
    withUnmanagedArena,
) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.Primitive
import           Data.Primitive.ByteArray
import           Data.Primitive.MutVar

#ifdef NO_IGNORE_ASSERTS
import           Control.Monad (forM_)
import           Data.Word (Word8)
#endif

data ArenaManager s = ArenaManager

newArenaManager :: PrimMonad m => m (ArenaManager (PrimState m))
newArenaManager = do
    _toUseTheConstraint <- newMutVar 'x'
    return ArenaManager

-- | For use in bencmark environments
instance NFData (ArenaManager s) where
    rnf _ = ()

-- TODO: this is debug implementation
-- we retain all the allocated arrays,
-- so we can scramble them at the end.
data Arena s = Arena (MutVar s [MutableByteArray s])

type Size      = Int
type Offset    = Int
type Alignment = Int

withArena :: PrimMonad m => ArenaManager (PrimState m) -> (Arena (PrimState m) -> m a) -> m a
withArena _ f = do
    mvar <- newMutVar []
    result <- f (Arena mvar)

    -- scramble the allocated bytearrays,
    -- they shouldn't be in use anymore!
#ifdef NO_IGNORE_ASSERTS
    mbas <- readMutVar mvar
    forM_ mbas $ \mba -> do
        size <- getSizeofMutableByteArray mba
        setByteArray mba 0 size (0x77 :: Word8)
#endif

    return result

-- | Create unmanaged arena
--
-- Never use this in non-tests code.
withUnmanagedArena :: PrimMonad m => (Arena (PrimState m) -> m a) -> m a
withUnmanagedArena k = do
    mgr <- newArenaManager
    withArena mgr k

allocateFromArena :: PrimMonad m => Arena (PrimState m)-> Size -> Alignment -> m (Offset, MutableByteArray (PrimState m))
allocateFromArena (Arena mvar) !size !alignment = do
    mba <- newAlignedPinnedByteArray size alignment
    atomicModifyMutVar' mvar $ \mbas -> (mba : mbas, ())
    return (0, mba)
