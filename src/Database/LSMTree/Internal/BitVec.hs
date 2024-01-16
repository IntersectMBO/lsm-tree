{-# LANGUAGE CPP                        #-}

{-# LANGUAGE BangPatterns               #-}
{-# LANGUAGE DeriveDataTypeable         #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE GeneralizedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE MagicHash                  #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UnboxedTuples              #-}
{-# LANGUAGE ViewPatterns               #-}

-- | This is Data.Bit.Internal module from @bitvec@ package,
-- however explicitly using 'Word64', and not machine native word.
module Database.LSMTree.Internal.BitVec
  ( Bit(..)
  , U.Vector(BitVec)
  , U.MVector(BitMVec)
  ) where

import           Control.DeepSeq (NFData)
import           Control.Exception (ArithException (DivideByZero), throw)
import           Control.Monad.Primitive (PrimMonad (PrimState))
import           Control.Monad.ST
import           Data.Bits
                     (Bits (complement, unsafeShiftL, unsafeShiftR, (.&.), (.|.)),
                     FiniteBits)
import           Data.Primitive.ByteArray (ByteArray, MutableByteArray,
                     copyMutableByteArray, indexByteArray, newByteArray,
                     readByteArray, sameMutableByteArray, setByteArray,
                     unsafeFreezeByteArray, unsafeThawByteArray, writeByteArray)
import           Data.Ratio (denominator, numerator)
import           Data.Typeable (Typeable)
import qualified Data.Vector.Generic as V
import qualified Data.Vector.Generic.Mutable as MV
import qualified Data.Vector.Unboxed as U
import           Data.Word (Word64)
import           GHC.Generics (Generic)

import           Database.LSMTree.Internal.BitUtils


-- | A newtype wrapper with a custom instance
-- for "Data.Vector.Unboxed", which packs booleans
-- as efficient as possible (8 values per byte).
-- Unboxed vectors of `Bit` use 8x less memory
-- than unboxed vectors of 'Bool' (which store one value per byte),
-- but random writes are slightly slower.
--
-- @since 0.1
newtype Bit = Bit {
  unBit :: Bool
  } deriving
  (Bounded, Enum, Eq, Ord
  , FiniteBits
  , Bits, Typeable
  , Generic
  , NFData
  )

-- | There is only one lawful 'Num' instance possible
-- with '+' = 'xor' and
-- 'fromInteger' = 'Bit' . 'odd'.
--
-- @since 1.0.1.0
instance Num Bit where
  Bit a * Bit b = Bit (a && b)
  Bit a + Bit b = Bit (a /= b)
  Bit a - Bit b = Bit (a /= b)
  negate = id
  abs    = id
  signum = id
  fromInteger = Bit . odd

-- | @since 1.0.1.0
instance Real Bit where
  toRational = fromIntegral

-- | @since 1.0.1.0
instance Integral Bit where
  quotRem _ (Bit False) = throw DivideByZero
  quotRem x (Bit True)  = (x, Bit False)
  toInteger (Bit False) = 0
  toInteger (Bit True)  = 1

-- | @since 1.0.1.0
instance Fractional Bit where
  fromRational x = fromInteger (numerator x) / fromInteger (denominator x)
  (/) = quot

instance Show Bit where
  showsPrec _ (Bit False) = showString "0"
  showsPrec _ (Bit True ) = showString "1"

instance Read Bit where
  readsPrec p (' ' : rest) = readsPrec p rest
  readsPrec _ ('0' : rest) = [(Bit False, rest)]
  readsPrec _ ('1' : rest) = [(Bit True, rest)]
  readsPrec _ _            = []

instance U.Unbox Bit

-- Ints are offset and length in bits
data instance U.MVector s Bit = BitMVec !Int !Int !(MutableByteArray s)
data instance U.Vector    Bit = BitVec  !Int !Int !ByteArray

readBit :: Int -> Word64 -> Bit
readBit i w = Bit (w .&. (1 `unsafeShiftL` i) /= 0)
{-# INLINE readBit #-}

extendToWord :: Bit -> Word64
extendToWord (Bit False) = 0
extendToWord (Bit True ) = complement 0

-- | Read a word at the given bit offset in little-endian order (i.e., the LSB will correspond to the bit at the given address, the 2's bit will correspond to the address + 1, etc.).  If the offset is such that the word extends past the end of the vector, the result is padded with memory garbage.
readWord :: PrimMonad m => U.MVector (PrimState m) Bit -> Int -> m Word64
readWord (BitMVec _ 0 _) _ = pure 0
readWord (BitMVec off len' arr) !i' = do
  let len  = off + len'
      i    = off + i'
      nMod = modWordSize i
      loIx = divWordSize i
  loWord <- readByteArray arr loIx

  if nMod == 0
    then pure loWord
    else if loIx == divWordSize (len - 1)
      then pure (loWord `unsafeShiftR` nMod)
      else do
        hiWord <- readByteArray arr (loIx + 1)
        pure
          $   (loWord `unsafeShiftR` nMod)
          .|. (hiWord `unsafeShiftL` (wordSize - nMod))
{-# SPECIALIZE readWord :: U.MVector s Bit -> Int -> ST s Word64 #-}
{-# INLINE readWord #-}

modifyByteArray
  :: PrimMonad m
  => MutableByteArray (PrimState m)
  -> Int
  -> Word64
  -> Word64
  -> m ()
modifyByteArray arr ix msk new = do
  old <- readByteArray arr ix
  writeByteArray arr ix (old .&. msk .|. new)
{-# INLINE modifyByteArray #-}

-- | Write a word at the given bit offset in little-endian order (i.e., the LSB will correspond to the bit at the given address, the 2's bit will correspond to the address + 1, etc.).  If the offset is such that the word extends past the end of the vector, the word is truncated and as many low-order bits as possible are written.
writeWord :: PrimMonad m => U.MVector (PrimState m) Bit -> Int -> Word64 -> m ()
writeWord (BitMVec _ 0 _) !_ !_ = pure ()
writeWord (BitMVec off len' arr) !i' !x
  | iMod == 0
  = if len >= i + wordSize
    then writeByteArray arr iDiv x
    else modifyByteArray arr iDiv (hiMask lenMod) (x .&. loMask lenMod)
  | iDiv == divWordSize (len - 1)
  = if lenMod == 0
    then modifyByteArray arr iDiv (loMask iMod) (x `unsafeShiftL` iMod)
    else modifyByteArray arr iDiv (loMask iMod .|. hiMask lenMod) ((x `unsafeShiftL` iMod) .&. loMask lenMod)
  | iDiv + 1 == divWordSize (len - 1)
  = do
    modifyByteArray arr iDiv (loMask iMod) (x `unsafeShiftL` iMod)
    if lenMod == 0
    then modifyByteArray arr (iDiv + 1) (hiMask iMod) (x `unsafeShiftR` (wordSize - iMod))
    else modifyByteArray arr (iDiv + 1) (hiMask iMod .|. hiMask lenMod) (x `unsafeShiftR` (wordSize - iMod) .&. loMask lenMod)
  | otherwise
  = do
    modifyByteArray arr iDiv (loMask iMod) (x `unsafeShiftL` iMod)
    modifyByteArray arr (iDiv + 1) (hiMask iMod) (x `unsafeShiftR` (wordSize - iMod))
  where
    len    = off + len'
    lenMod = modWordSize len
    i      = off + i'
    iMod   = modWordSize i
    iDiv   = divWordSize i

{-# SPECIALIZE writeWord :: U.MVector s Bit -> Int -> Word64 -> ST s () #-}
{-# INLINE writeWord #-}

instance MV.MVector U.MVector Bit where
  {-# INLINE basicInitialize #-}
  basicInitialize vec = MV.basicSet vec (Bit False)

  {-# INLINE basicUnsafeNew #-}
  basicUnsafeNew n
    | n < 0 = error $ "Data.Bit.basicUnsafeNew: negative length: " ++ show n
    | otherwise = do
      arr <- newByteArray (wordsToBytes $ nWords n)
      pure $ BitMVec 0 n arr

  {-# INLINE basicUnsafeReplicate #-}
  basicUnsafeReplicate n x
    | n < 0 =  error $  "Data.Bit.basicUnsafeReplicate: negative length: " ++ show n
    | otherwise = do
      arr <- newByteArray (wordsToBytes $ nWords n)
      setByteArray arr 0 (nWords n) (extendToWord x :: Word64)
      pure $ BitMVec 0 n arr

  {-# INLINE basicOverlaps #-}
  basicOverlaps (BitMVec i' m' arr1) (BitMVec j' n' arr2) =
    sameMutableByteArray arr1 arr2
      && (between i j (j + n) || between j i (i + m))
   where
    i = divWordSize i'
    m = nWords (i' + m') - i
    j = divWordSize j'
    n = nWords (j' + n') - j
    between x y z = x >= y && x < z

  {-# INLINE basicLength #-}
  basicLength (BitMVec _ n _) = n

  {-# INLINE basicUnsafeRead #-}
  basicUnsafeRead (BitMVec off _ arr) !i' = do
    let i = off + i'
    word <- readByteArray arr (divWordSize i)
    pure $ readBit (modWordSize i) word

  {-# INLINE basicUnsafeWrite #-}
  basicUnsafeWrite (BitMVec off _ arr) !i' !x = do
    let i  = off + i'
        j  = divWordSize i
        k  = modWordSize i
        kk = 1 `unsafeShiftL` k :: Word64
    word <- readByteArray arr j
    writeByteArray arr j (if unBit x then word .|. kk else word .&. complement kk)

  {-# INLINE basicSet #-}
  basicSet (BitMVec off len arr) (extendToWord -> x) | offBits == 0 =
    case modWordSize len of
      0    -> setByteArray arr offWords lWords (x :: Word64)
      nMod -> do
        setByteArray arr offWords (lWords - 1) (x :: Word64)
        modifyByteArray arr (offWords + lWords - 1) (hiMask nMod) (x .&. loMask nMod)
   where
    offBits  = modWordSize off
    offWords = divWordSize off
    lWords   = nWords (offBits + len)
  basicSet (BitMVec off len arr) (extendToWord -> x) =
    case modWordSize (off + len) of
      0 -> do
        modifyByteArray arr offWords (loMask offBits) (x .&. hiMask offBits)
        setByteArray arr (offWords + 1) (lWords - 1) (x :: Word64)
      nMod -> if lWords == 1
        then do
          let lohiMask = loMask offBits .|. hiMask nMod
          modifyByteArray arr offWords lohiMask (x .&. complement lohiMask)
        else do
          modifyByteArray arr offWords (loMask offBits) (x .&. hiMask offBits)
          setByteArray arr (offWords + 1) (lWords - 2) (x :: Word64)
          modifyByteArray arr (offWords + lWords - 1) (hiMask nMod) (x .&. loMask nMod)
   where
    offBits  = modWordSize off
    offWords = divWordSize off
    lWords   = nWords (offBits + len)

  {-# INLINE basicUnsafeCopy #-}
  basicUnsafeCopy (BitMVec offDst lenDst dst) (BitMVec offSrc _ src)
    | offDstBits == 0, offSrcBits == 0 = case modWordSize lenDst of
      0 -> copyMutableByteArray dst
                                (wordsToBytes offDstWords)
                                src
                                (wordsToBytes offSrcWords)
                                (wordsToBytes lDstWords)
      nMod -> do
        copyMutableByteArray dst
                             (wordsToBytes offDstWords)
                             src
                             (wordsToBytes offSrcWords)
                             (wordsToBytes $ lDstWords - 1)

        lastWordSrc <- readByteArray src (offSrcWords + lDstWords - 1)
        modifyByteArray dst (offDstWords + lDstWords - 1) (hiMask nMod) (lastWordSrc .&. loMask nMod)
   where
    offDstBits  = modWordSize offDst
    offDstWords = divWordSize offDst
    lDstWords   = nWords (offDstBits + lenDst)
    offSrcBits  = modWordSize offSrc
    offSrcWords = divWordSize offSrc
  basicUnsafeCopy (BitMVec offDst lenDst dst) (BitMVec offSrc _ src)
    | offDstBits == offSrcBits = case modWordSize (offSrc + lenDst) of
      0 -> do
        firstWordSrc <- readByteArray src offSrcWords
        modifyByteArray dst offDstWords (loMask offSrcBits) (firstWordSrc .&. hiMask offSrcBits)
        copyMutableByteArray dst
                             (wordsToBytes $ offDstWords + 1)
                             src
                             (wordsToBytes $ offSrcWords + 1)
                             (wordsToBytes $ lDstWords - 1)
      nMod -> if lDstWords == 1
        then do
          let lohiMask = loMask offSrcBits .|. hiMask nMod
          theOnlyWordSrc <- readByteArray src offSrcWords
          modifyByteArray dst offDstWords lohiMask (theOnlyWordSrc .&. complement lohiMask)
        else do
          firstWordSrc <- readByteArray src offSrcWords
          modifyByteArray dst offDstWords (loMask offSrcBits) (firstWordSrc .&. hiMask offSrcBits)
          copyMutableByteArray dst
                               (wordsToBytes $ offDstWords + 1)
                               src
                               (wordsToBytes $ offSrcWords + 1)
                               (wordsToBytes $ lDstWords - 2)
          lastWordSrc <- readByteArray src (offSrcWords + lDstWords - 1)
          modifyByteArray dst (offDstWords + lDstWords - 1) (hiMask nMod) (lastWordSrc .&. loMask nMod)
   where
    offDstBits  = modWordSize offDst
    offDstWords = divWordSize offDst
    lDstWords   = nWords (offDstBits + lenDst)
    offSrcBits  = modWordSize offSrc
    offSrcWords = divWordSize offSrc

  basicUnsafeCopy dst@(BitMVec _ len _) src = do_copy 0
   where
    n = alignUp len

    do_copy i
      | i < n = do
        x <- readWord src i
        writeWord dst i x
        do_copy (i + wordSize)
      | otherwise = pure ()

  {-# INLINE basicUnsafeMove #-}
  basicUnsafeMove !dst src@(BitMVec srcShift srcLen _)
    | MV.basicOverlaps dst src = do
          -- Align shifts of src and srcCopy to speed up basicUnsafeCopy srcCopy src
      srcCopy <- MV.drop (modWordSize srcShift)
        <$> MV.basicUnsafeNew (modWordSize srcShift + srcLen)
      MV.basicUnsafeCopy srcCopy src
      MV.basicUnsafeCopy dst srcCopy
    | otherwise = MV.basicUnsafeCopy dst src

  {-# INLINE basicUnsafeSlice #-}
  basicUnsafeSlice offset n (BitMVec off _ arr) = BitMVec (off + offset) n arr

  {-# INLINE basicUnsafeGrow #-}
  basicUnsafeGrow (BitMVec off len src) byBits
    | byWords == 0 = pure $ BitMVec off (len + byBits) src
    | otherwise = do
      dst <- newByteArray (wordsToBytes newWords)
      copyMutableByteArray dst 0 src 0 (wordsToBytes oldWords)
      pure $ BitMVec off (len + byBits) dst
   where
    oldWords = nWords (off + len)
    newWords = nWords (off + len + byBits)
    byWords  = newWords - oldWords

instance V.Vector U.Vector Bit where
  basicUnsafeFreeze (BitMVec s n v) = BitVec s n <$> unsafeFreezeByteArray v
  basicUnsafeThaw (BitVec s n v) = BitMVec s n <$> unsafeThawByteArray v
  basicLength (BitVec _ n _) = n

  basicUnsafeIndexM (BitVec off _ arr) !i' = do
    let i = off + i'
    pure $! readBit (modWordSize i) (indexByteArray arr (divWordSize i))

  basicUnsafeCopy dst src = do
    src1 <- V.basicUnsafeThaw src
    MV.basicUnsafeCopy dst src1

  {-# INLINE basicUnsafeSlice #-}
  basicUnsafeSlice offset n (BitVec off _ arr) = BitVec (off + offset) n arr
