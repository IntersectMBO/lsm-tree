{-# LANGUAGE MagicHash           #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE UnboxedTuples       #-}
module Database.LSMTree.Internal.StrictArray (
    StrictArray,
    vectorToStrictArray,
    indexStrictArray,
    sizeofStrictArray,
) where

import           Data.Elevator (Strict (Strict))
import qualified Data.Vector as V
import           GHC.Exts ((+#))
import qualified GHC.Exts as GHC
import           GHC.ST (ST (ST), runST)
import           Unsafe.Coerce (unsafeCoerce)

data StrictArray a = StrictArray !(GHC.Array# (Strict a))

vectorToStrictArray :: forall a. V.Vector a -> StrictArray a
vectorToStrictArray v =
    runST $ ST $ \s0 ->
    case GHC.newArray# (case V.length v of GHC.I# l# -> l#)
                       (Strict (unsafeCoerce ())) s0 of
                       -- initialise with (), will be overwritten.
      (# s1, a#  #) ->
        case go a# 0# s1 of
          s2 -> case GHC.unsafeFreezeArray# a# s2 of
                  (# s3, a'# #) -> (# s3, StrictArray a'# #)
  where
    go :: forall s.
          GHC.MutableArray# s (Strict a)
       -> GHC.Int#
       -> GHC.State# s
       -> GHC.State# s
    go a# i# s
      | GHC.I# i# < V.length v
      = let x = V.unsafeIndex v (GHC.I# i#)
            -- We have to use seq# here to force the array element to WHNF
            -- before putting it into the strict array. This should not be
            -- necessary. https://github.com/sgraf812/data-elevator/issues/4
         in case GHC.seq# x s of
              (# s', x' #) ->
                case GHC.writeArray# a# i# (Strict x') s' of
                  s'' -> go a# (i# +# 1#) s''
      | otherwise = s

{-# INLINE indexStrictArray #-}
indexStrictArray :: StrictArray a -> Int -> a
indexStrictArray (StrictArray a#) (GHC.I# i#) =
    case GHC.indexArray# a# i# of
      (# Strict x #) -> x

{-# INLINE sizeofStrictArray #-}
sizeofStrictArray :: StrictArray a -> Int
sizeofStrictArray (StrictArray a#) =
    GHC.I# (GHC.sizeofArray# a#)
