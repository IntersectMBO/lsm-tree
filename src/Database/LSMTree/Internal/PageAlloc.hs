{-# LANGUAGE CPP                 #-}
{-# LANGUAGE ScopedTypeVariables #-}
module Database.LSMTree.Internal.PageAlloc (
    PageAlloc,
    newPageAlloc,
    withPages,
    Pages,
    unsafeIndexPages,
    -- * Lower level
    unmanagedAllocatePages,
    freePages,
) where

import           Data.Bits
import           Data.Primitive.ByteArray
import           Data.Primitive.MutVar
import           Data.Primitive.SmallArray

import           Control.DeepSeq (NFData (..))
import           Control.Exception (assert)
import           Control.Monad.Primitive (PrimMonad, PrimState, unsafeSTToPrim)
import           Control.Monad.ST


newtype PageAlloc s = PageAlloc (MutVar s [Slab s])

-- | A slab is a cache of 16 contiguous 4k pages, aligned to 4k.
type Slab s = MutableByteArray s

-- | A set of allocated pages, as returned by 'withPages'. Use 'unsafeIndexPages'
-- to index individual pages.
newtype Pages s = Pages (SmallArray (Slab s))
  deriving newtype NFData

-- | For use in bencmark environments
instance NFData (PageAlloc s) where
    rnf _ = ()

newPageAlloc :: PrimMonad m => m (PageAlloc (PrimState m))
newPageAlloc = PageAlloc <$> newMutVar []

{-# INLINE withPages #-}
withPages :: PrimMonad m
          => PageAlloc (PrimState m)
          -> Int
          -> (Pages (PrimState m) -> m a)
          -> m a
withPages pa !n f = do
    -- no need for bracket to guarantee freeing, it'll all get GC'd.
    ps <- unmanagedAllocatePages pa n
    x <- f ps
    freePages pa ps
    return x

{-# INLINE unsafeIndexPages #-}
-- | The index must be within the number of pages requested from 'withPages'.
unsafeIndexPages :: Pages s -> Int -> (MutableByteArray s, Int)
unsafeIndexPages (Pages slabs) n =
    assert (slab_i >= 0 && slab_i < sizeofSmallArray slabs)
    (indexSmallArray slabs slab_i, page_i)
  where
    slab_i = n `unsafeShiftR` 4
    page_i = (n .&. 0xf) `unsafeShiftL` 12 -- byte offset of 4k page within slab

{-# SPECIALIZE
  unmanagedAllocatePages :: PageAlloc RealWorld
                         -> Int
                         -> IO (Pages RealWorld)
  #-}
-- | Where it is not possible to use 'withPages', use this to allocate pages
-- but this must be matched by exactly one use of 'freePages'.
unmanagedAllocatePages :: PrimMonad m
                       => PageAlloc (PrimState m)
                       -> Int
                       -> m (Pages (PrimState m))
unmanagedAllocatePages (PageAlloc slabcache) !npages = do
    let !nslabs = (npages + 0xf) `unsafeShiftR` 4
    slabs <- atomicModifyMutVar' slabcache (takeSlabs nslabs)
    let !pages = assemblePages nslabs slabs
    return pages

takeSlabs :: Int -> [a] -> ([a], [a])
takeSlabs = go []
  where
    go acc 0  ss     = (ss, acc) -- (result, state')
    go acc _  []     = (acc, [])
    go acc !n (s:ss) = go (s:acc) (n-1) ss

assemblePages :: forall s. Int -> [Slab s] -> Pages s
assemblePages !nslabs slabs0 =
    Pages $
      createSmallArray nslabs undefined $ \slabarr ->
        useCachedSlabs slabarr 0 nslabs slabs0
  where
    useCachedSlabs :: forall s'.
                      SmallMutableArray s' (Slab s)
                   -> Int -> Int
                   -> [Slab s]
                   -> ST s' ()
    useCachedSlabs !slabarr !i !n (slab:slabs) =
      assert (n > 0 && i >= 0 && i < nslabs) $ do
      writeSmallArray slabarr i slab
      useCachedSlabs slabarr (i+1) (n-1) slabs

    useCachedSlabs !slabs !i !n [] =
      allocFreshSlabs slabs i n

    allocFreshSlabs :: forall s'.
                       SmallMutableArray s' (Slab s)
                    -> Int -> Int
                    -> ST s' ()
    allocFreshSlabs !_ !_ 0 =
      return ()

    allocFreshSlabs slabarr !i !n = do
      -- Use unsafeSTToPrim to separate the s and s' here:
      -- The s is the outer tag, usually RealWorld
      -- The s' is the inner tag for createSmallArray above.
      slab <- unsafeSTToPrim $ newAlignedPinnedByteArray 0x10000 0x1000
      writeSmallArray slabarr i slab
      allocFreshSlabs slabarr (i+1) (n-1)

{-# SPECIALIZE
  freePages :: PageAlloc RealWorld
            -> Pages RealWorld
            -> IO ()
  #-}
-- | For use with unmanagedAllocatePages'. Note that this is /not/ idempotent.
-- Freeing the same pages more than once will lead to corruption of the page
-- allocator state and bugs that are hard to diagnose.
freePages :: PrimMonad m
          => PageAlloc (PrimState m)
          -> Pages (PrimState m)
          -> m ()
freePages (PageAlloc slabcache) pgs = do
#ifdef NO_IGNORE_ASSERTS
    scramblePages pgs
#endif
    atomicModifyMutVar' slabcache (putSlabs pgs)
    --TODO: in NO_IGNORE_ASSERTS mode, verify that the same pages have not
    -- been added to the slabcache already.

putSlabs :: Pages s -> [Slab s] -> ([Slab s], ())
putSlabs = \(Pages slabarr) slabs ->
    go slabarr 0 (sizeofSmallArray slabarr) slabs
  where
    go _       _ 0 slabs = (slabs, ())
    go slabarr i n slabs = let !slab = indexSmallArray slabarr i
                            in go slabarr (i+1) (n-1) (slab:slabs)

#ifdef NO_IGNORE_ASSERTS
-- | Scramble the allocated bytearrays, they shouldn't be in use anymore!
scramblePages (Pages slabs) =
    forM_ slabs $ \slab -> do
      size <- getSizeofMutableByteArray slab
      setByteArray slab 0 size (0x77 :: Word8)
#endif
