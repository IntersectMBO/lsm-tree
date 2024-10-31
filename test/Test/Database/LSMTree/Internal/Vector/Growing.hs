module Test.Database.LSMTree.Internal.Vector.Growing (tests) where

import           Prelude hiding (head, length, tail)

import           Control.Category ((>>>))
import           Control.Monad.ST.Strict (ST, runST)
import qualified Data.List as List (length)
import           Data.Vector (Vector, toList)
import           Database.LSMTree.Internal.Vector.Growing (GrowingVector,
                     append, freeze, new)
import           Test.QuickCheck (Arbitrary (arbitrary, shrink), Gen,
                     NonNegative (NonNegative, getNonNegative),
                     Positive (Positive), Property, Small (Small), Testable,
                     chooseInt, coverTable, shrinkList, shrinkMap, shrinkMapBy,
                     sized, tabulate, (===))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

-- * Tests

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Vector.Growing" $
        [
            testProperty "Final vector is correct"
                         prop_finalVectorIsCorrect
        ]

-- * Utilities

-- ** Segments

data Segment a = Replicate Int a deriving stock Show

segmentLength :: Segment a -> Int
segmentLength (Replicate count _) = max 0 count

segmentToList :: Segment a -> [a]
segmentToList (Replicate count val) = replicate count val

appendSegment :: GrowingVector s a -> Segment a -> ST s ()
appendSegment vec (Replicate count val) = append vec count val

-- ** Vector construction

{-
    Constructs an ordinary vector by creating a growing vector, appending
    segments to it, and finally freezing it.
-}
finalVector :: Int         -- Initial buffer size
            -> [Segment a] -- Segments
            -> Vector a    -- Final vector
finalVector initialBufferSize segments = runST $ do
    vec <- new initialBufferSize
    mapM_ (appendSegment vec) segments
    freeze vec

{-
    Supplies the final contents of a growing vector for constructing a property.

    The resulting property additionally provides information about the
    occurrence of buffer size scaling exponents, where a buffer size scaling
    exponent is the binary logarithm of the ratio between the buffer size after
    and before appending a segment. If buffer enlargement for an append would
    happen in cycles where in each cycle the buffer size is doubled, then the
    buffer size scaling exponent would be equal to the number of such cycles.
    Therefore, a buffer size scaling exponent tells if the buffer is enlarged
    and, if yes, how much.
-}
withFinalVector
    :: Testable prop
    => Int                -- Initial buffer size
    -> [Segment a]        -- Segments
    -> (Vector a -> prop) -- Property from final vector
    -> Property           -- Resulting property
withFinalVector initialBufferSize segments consumer
    = coverTable
          "Buffer size scaling exponents"
          [(show scalingExp, 10) | scalingExp <- [0 .. 3] :: [Int]]
      $
      tabulate
          "Buffer size scaling exponents"
          (show <$> bufferSizeScalingExponents availableBufferSizes 0 segments)
      $
      consumer (finalVector initialBufferSize segments)
    where

    availableBufferSizes :: [Int]
    availableBufferSizes = iterate (* 2) initialBufferSize
    {-
        We do not need to account for overflow here, because the lengths of the
        vectors we construct are small compared to @maxBound :: Int@.
    -}

    bufferSizeScalingExponents :: [Int] -> Int -> [Segment a] -> [Int]
    bufferSizeScalingExponents _ _ []
        = []
    bufferSizeScalingExponents availableSizes length (head : tail)
        = List.length insufficient :
          bufferSizeScalingExponents sufficient length' tail
        where

        length' :: Int
        !length' = length + segmentLength head

        insufficient, sufficient :: [Int]
        (insufficient, sufficient) = span (< length') availableSizes

-- * Properties to test

prop_finalVectorIsCorrect :: Positive (Small Int)
                          -> Segments Integer
                          -> Property
prop_finalVectorIsCorrect (Positive (Small initialBufferSize))
                          (Segments segments)
    = withFinalVector initialBufferSize segments $ \ vec ->
      toList vec === concatMap segmentToList segments

-- * Test case generation and shrinking

generateSegment :: Arbitrary a => Int -> Gen (Segment a)
generateSegment maxCount = Replicate <$> chooseInt (0, maxCount) <*> arbitrary

shrinkSegment :: Arbitrary a => Segment a -> [Segment a]
shrinkSegment (Replicate count val)
    = [Replicate count' val  | count' <- shrinkNat count] ++
      [Replicate count  val' | val'   <- shrink val]
    where

    shrinkNat :: Int -> [Int]
    shrinkNat = shrinkMap getNonNegative NonNegative

{-
    A list of segments to be appended to an initially empty vector. 'Segments a'
    is isomorphic to '[Segment a]' but has a special way of generating test
    cases, which avoids skewing with respect to buffer size scaling exponents.

    The key issue of segment list generation is how to generate the length of an
    individual segment. It seems worthwhile to randomly pick a natural number
    smaller than or equal to some maximum length, using a uniform distribution.
    The question to be answered is how to choose the maximum lengths for the
    different segments.

    A naïve approach is to use a common maximum length for all segments in a
    particular list, possibly computed from the size parameter. However, this
    results in most appends not enlarging the buffer, meaning that the buffer
    size scaling exponent is zero in most cases. This is because, as the buffer
    size increases, the number of elements needed to cause the next buffer
    enlargement increases too.

    Note that it does not help much to choose the maximum segment length large
    compared to the initial buffer size. If this is done, then the buffer is
    enlarged very quickly, likely during the first append, and the distributions
    of buffer size scaling exponents up to this first enlargement may actually
    be very good. However, after the first enlargement, the buffer size is
    typically of the same order of magnitude as the maximum segment length, so
    that the buffer size scaling exponents of the following appends are likely
    to be zero or close to it, with further buffer enlargements making the
    situation only worse.

    A better idea is to choose the maximum length of each segment but the first
    proportional to the length of the vector just before appending it (for the
    first segment, such a choice is not appropriate, because it would lead to a
    maximum length of zero). With this approach, the distribution of buffer size
    scaling exponents stays roughly the same as soon as the buffer has been
    enlarged for the first time, because then the ratio between the current
    buffer size and the current vector length is always in the interval \([1,
    2)\) and thus varies only slightly. Reasonable distributions of buffer size
    scaling exponents can be achieved by choosing the ratio between maximum
    segment lengths and corresponding current-vector lengths appropriately.

    A downside of this approach is that it requires tracking the current vector
    length. This tracking can be avoided by considering only the /expected/
    current vector length and choosing the maximum segment length proportional
    to it. The expected length of a vector is the sum of the expected lengths of
    the segments constituting it, and the expected length of a segment is
    proportional to the maximum length used for generating it. Therefore, this
    modified solution boils down to choosing the maximum length of each segment
    but the first proportional to the sum of the maximum lengths of the segments
    preceding it.

    Note that, with the approach just set out, the maximum lengths of the
    segments in a segment list almost form an exponential sequence, whose base
    is determined by the chosen ratio between maximum segment lengths and
    corresponding sums of maximum predecessor segment lengths. Therefore, a
    similarly good, but simpler, solution is to have the maximum segment lengths
    precisely form an exponential sequence. This is the approach that we use in
    our segment list generation algorithm.

    The concrete exponential sequences that we employ start with the size
    parameter and use 3 as the base of the exponentiation. Since initial buffer
    sizes are generated using a uniform distribution with the size parameter as
    the maximum, the choice of the size parameter as the first segment’s maximum
    length leads to non-trivial distributions of buffer size scaling exponents
    for the appends up to the one that causes the first buffer enlargement. The
    choice of 3 as the exponentiation’s base leads to a good overall
    distribution of buffer size scaling exponents, as experiments have shown.

    Because of the exponential growth of maximum segment lengths, vectors
    constructed during testing get very large for longer segment lists.
    Therefore, we do not pick the lengths of segment lists in the usual way but
    instead make all segment lists have a common, small length. Concretely, we
    use 9 as this common length, because this leads to vectors that are not
    trivial but still consume reasonable amounts of memory.
-}
newtype Segments a = Segments [Segment a] deriving stock Show

instance Arbitrary a => Arbitrary (Segments a) where

    arbitrary = sized $ iterate (* scalingFactor) >>>
                        take count                >>>
                        mapM generateSegment      >>>
                        fmap Segments
        where

        scalingFactor :: Int
        scalingFactor = 3

        count :: Int
        count = 9

    shrink = shrinkMapBy Segments (\ (Segments segments) -> segments) $
             shrinkList shrinkSegment
