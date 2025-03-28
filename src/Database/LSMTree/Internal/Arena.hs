{-# LANGUAGE CPP             #-}
{-# LANGUAGE LambdaCase      #-}
{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_HADDOCK not-home #-}
module Database.LSMTree.Internal.Arena (
    ArenaManager,
    newArenaManager,
    Arena,
    Size,
    Offset,
    Alignment,
    withArena,
    newArena,
    closeArena,

    allocateFromArena,
    -- * Test helpers
    withUnmanagedArena,
) where

import           Control.DeepSeq (NFData (..))
import           Control.Exception (assert)
import           Control.Monad.Primitive
import           Control.Monad.ST (ST)
import           Data.Bits (complement, popCount, (.&.))
import           Data.Primitive.ByteArray
import           Data.Primitive.MutVar
import           Data.Primitive.MVar
import           Data.Primitive.PrimVar

#ifdef NO_IGNORE_ASSERTS
import           Data.Word (Word8)
#endif

data ArenaManager s = ArenaManager (MutVar s [Arena s])

newArenaManager :: PrimMonad m => m (ArenaManager (PrimState m))
newArenaManager = do
    m <- newMutVar []
    return $ ArenaManager m

-- | For use in bencmark environments
instance NFData (ArenaManager s) where
    rnf (ArenaManager !_) = ()

data Arena s = Arena
    { curr :: !(MVar s (Block s))   -- current block, also acts as a lock
    , free :: !(MutVar s [Block s])
    , full :: !(MutVar s [Block s])
    }

data Block s = Block !(PrimVar s Int) !(MutableByteArray s)

instance NFData (Arena s) where
  rnf (Arena !_ !_ !_) = ()

type Size      = Int
type Offset    = Int
type Alignment = Int

blockSize :: Int
blockSize = 0x100000

{-# SPECIALIZE
    newBlock :: ST s (Block s)
  #-}
{-# SPECIALIZE
    newBlock :: IO (Block RealWorld)
  #-}
newBlock :: PrimMonad m => m (Block (PrimState m))
newBlock = do
    off <- newPrimVar 0
    mba <- newAlignedPinnedByteArray blockSize 4096
    return (Block off mba)

{-# INLINE withArena #-}
withArena :: PrimMonad m => ArenaManager (PrimState m) -> (Arena (PrimState m) -> m a) -> m a
withArena am f = do
    a <- newArena am
    x <- f a
    closeArena am a
    pure x

{-# SPECIALIZE
    newArena :: ArenaManager s -> ST s (Arena s)
  #-}
{-# SPECIALIZE
    newArena :: ArenaManager RealWorld -> IO (Arena RealWorld)
  #-}
newArena :: PrimMonad m => ArenaManager (PrimState m) -> m (Arena (PrimState m))
newArena (ArenaManager arenas) = do
    marena <- atomicModifyMutVar' arenas $ \case
        []     -> ([], Nothing)
        (x:xs) -> (xs, Just x)

    case marena of
        Just arena -> return arena
        Nothing -> do
            curr <- newBlock >>= newMVar
            free <- newMutVar []
            full <- newMutVar []
            return Arena {..}

{-# SPECIALIZE
    closeArena :: ArenaManager s -> Arena s -> ST s ()
  #-}
{-# SPECIALIZE
    closeArena :: ArenaManager RealWorld -> Arena RealWorld -> IO ()
  #-}
closeArena :: PrimMonad m => ArenaManager (PrimState m) -> Arena (PrimState m) -> m ()
closeArena (ArenaManager arenas) arena = do
    scrambleArena arena

    -- reset the arena to clear state
    resetArena arena

    atomicModifyMutVar' arenas $ \xs -> (arena : xs, ())



scrambleArena :: PrimMonad m => Arena (PrimState m) -> m ()
#ifndef NO_IGNORE_ASSERTS
scrambleArena _ = return ()
#else
scrambleArena Arena {..} = do
    readMVar curr >>= scrambleBlock
    readMutVar full >>= mapM_ scrambleBlock
    readMutVar free >>= mapM_ scrambleBlock

scrambleBlock :: PrimMonad m => Block (PrimState m) -> m ()
scrambleBlock (Block _ mba) = do
    size <- getSizeofMutableByteArray mba
    setByteArray mba 0 size (0x77 :: Word8)
#endif

{-# SPECIALIZE
    resetArena :: Arena s -> ST s ()
  #-}
{-# SPECIALIZE
    resetArena :: Arena RealWorld -> IO ()
  #-}
-- | Reset arena, i.e. return used blocks to free list.
resetArena :: PrimMonad m => Arena (PrimState m) -> m ()
resetArena Arena {..} = do
    Block off mba <- takeMVar curr

    -- reset current block
    writePrimVar off 0

    -- move full block to free blocks.
    -- block's offset will be reset in 'newBlockWithFree'
    full' <- atomicModifyMutVar' full $ \xs -> ([], xs)
    atomicModifyMutVar' free $ \xs -> (full' <> xs, ())

    putMVar curr $! Block off mba

-- | Create unmanaged arena.
--
-- Never use this in non-tests code.
withUnmanagedArena :: PrimMonad m => (Arena (PrimState m) -> m a) -> m a
withUnmanagedArena k = do
    mgr <- newArenaManager
    withArena mgr k

{-# SPECIALIZE
    allocateFromArena :: Arena s -> Size -> Alignment -> ST s (Offset, MutableByteArray s)
  #-}
-- | Allocate a slice of mutable byte array from the arena.
allocateFromArena :: PrimMonad m => Arena (PrimState m)-> Size -> Alignment -> m (Offset, MutableByteArray (PrimState m))
allocateFromArena !arena !size !alignment =
    assert (popCount alignment == 1) $ -- powers of 2
    assert (size <= blockSize) $ -- not too large allocations
    allocateFromArena' arena size alignment

{-# SPECIALIZE
    allocateFromArena' :: Arena s -> Size -> Alignment -> ST s (Offset, MutableByteArray s)
  #-}
-- TODO!? this is not async exception safe
allocateFromArena' :: PrimMonad m => Arena (PrimState m)-> Size -> Alignment -> m (Offset, MutableByteArray (PrimState m))
allocateFromArena' arena@Arena { .. } !size !alignment = do
    -- take current block, lock the arena
    curr'@(Block off mba) <- takeMVar curr

    off' <- readPrimVar off
    let !ali = alignment - 1
    let !off'' = (off' + ali) .&. complement ali -- ceil towards next alignment
    let !end  = off'' + size
    if end <= blockSize
    then do
        -- fits into current block:
        -- * update offset
        writePrimVar off end
        -- * release lock
        putMVar curr curr'
        -- * return data
        return (off'', mba)

    else do
        -- doesn't fit into current block:
        -- * move current block into full
        atomicModifyMutVar' full (\xs -> (curr' : xs, ()))
        -- * allocate new block
        new <- newBlockWithFree free
        -- * set new block as current, release the lock
        putMVar curr new
        -- * go again
        allocateFromArena' arena size alignment

{-# SPECIALIZE newBlockWithFree :: MutVar s [Block s] -> ST s (Block s) #-}
-- | Allocate new block, possibly taking it from a free list
newBlockWithFree :: PrimMonad m => MutVar (PrimState m) [Block (PrimState m)] -> m (Block (PrimState m))
newBlockWithFree free = do
    free' <- readMutVar free
    case free' of
        []   -> newBlock
        x@(Block off _):xs -> do
            writePrimVar off 0
            writeMutVar free xs
            return x
