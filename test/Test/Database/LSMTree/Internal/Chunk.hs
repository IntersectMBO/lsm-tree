module Test.Database.LSMTree.Internal.Chunk (tests) where

import           Prelude hiding (concat, drop, length)

import           Control.Category ((>>>))
import           Control.Monad.ST.Strict (runST)
import qualified Data.List as List (concat, drop, length)
import           Data.Maybe (catMaybes, fromJust, isJust, isNothing)
import           Data.Vector.Primitive (Vector, concat, length)
import           Data.Word (Word8)
import           Database.LSMTree.Extras.Generators ()
                     -- for @Arbitrary@ instantiation of @Vector@
import           Database.LSMTree.Internal.Chunk (Chunk, createBaler, feedBaler,
                     toByteVector, unsafeEndBaler)
import           Test.QuickCheck (Arbitrary (arbitrary, shrink),
                     NonEmptyList (NonEmpty), Positive (Positive, getPositive),
                     Property, Small (Small, getSmall), Testable, scale,
                     shrinkMap, tabulate, (===), (==>))
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck (testProperty)

-- * Tests

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Chunk" $
        [
            testProperty "Content is preserved"
                         prop_contentIsPreserved,
            testProperty "No remnant after output"
                         prop_noRemnantAfterOutput,
            testProperty "Common chunks are large"
                         prop_commonChunksAreLarge,
            testProperty "Remnant chunk is non-empty"
                         prop_remnantChunkIsNonEmpty,
            testProperty "Remnant chunk is small"
                         prop_remnantChunkIsSmall
        ]

-- * Properties to test

{-
    Feeds a freshly created baler a sequence of data portions and ends it
    afterwards, yielding all output.
-}
balingOutput :: Int                          -- Minimum chunk size
             -> [[Vector Word8]]             -- Data portions to be fed
             -> ([Maybe Chunk], Maybe Chunk) -- Feeding output and remnant
balingOutput minChunkSize food = runST $ do
    baler <- createBaler minChunkSize
    commonChunks <- mapM (flip feedBaler baler) food
    remnant <- unsafeEndBaler baler
    return (commonChunks, remnant)

{-
    Supplies the output of a complete baler run for constructing a property.

    The resulting property additionally provides statistics about the lengths of
    buildup phases, where a buildup phase is a sequence of feedings that does
    not result in chunks and is followed by an ultimate chunk production, which
    happens either due to another feeding or due to the baler run ending and
    producing a remnant chunk.
-}
withBalingOutput ::
       Testable prop
    => Int                                    -- Minimum chunk size
    -> [[Vector Word8]]                       -- Data portions to be fed
    -> ([Maybe Chunk] -> Maybe Chunk -> prop) -- Property from baler output
    -> Property                               -- Resulting property
withBalingOutput minChunkSize food consumer
    = tabulate "Lengths of buildup phases"
               (map show (buildupPhasesLengths commonChunks))
               (consumer commonChunks remnant)
    where

    commonChunks :: [Maybe Chunk]
    remnant :: Maybe Chunk
    (commonChunks, remnant) = balingOutput minChunkSize food

    buildupPhasesLengths :: [Maybe Chunk] -> [Int]
    buildupPhasesLengths []     = []
    buildupPhasesLengths chunks = List.length buildupOutput :
                                  buildupPhasesLengths (List.drop 1 followUp)
        where

        buildupOutput, followUp :: [Maybe Chunk]
        (buildupOutput, followUp) = span isNothing chunks

prop_contentIsPreserved :: MinChunkSize -> [[Vector Word8]] -> Property
prop_contentIsPreserved (MinChunkSize minChunkSize) food
    = withBalingOutput minChunkSize food $ \ commonChunks remnant ->
      let

          input :: Vector Word8
          input = concat (List.concat food)

          output :: Vector Word8
          output = concat $
                   toByteVector <$> catMaybes (commonChunks ++ [remnant])

      in input === output

prop_noRemnantAfterOutput :: MinChunkSize
                          -> NonEmptyList [Vector Word8]
                          -> Property
prop_noRemnantAfterOutput (MinChunkSize minChunkSize) (NonEmpty food)
    = withBalingOutput minChunkSize food $ \ commonChunks remnant ->
      isJust (last commonChunks) ==> isNothing remnant

prop_commonChunksAreLarge :: MinChunkSize -> [[Vector Word8]] -> Property
prop_commonChunksAreLarge (MinChunkSize minChunkSize) food
    = withBalingOutput minChunkSize food $ \ commonChunks _ ->
      all (toByteVector >>> length >>> (>= minChunkSize)) $
      catMaybes commonChunks

remnantChunkSizeIs :: (Int -> Bool) -> Int -> [[Vector Word8]] -> Property
remnantChunkSizeIs constraint minChunkSize food
    = withBalingOutput minChunkSize food $ \ _ remnant ->
      isJust remnant ==> constraint (length (toByteVector (fromJust remnant)))

prop_remnantChunkIsNonEmpty :: MinChunkSize -> [[Vector Word8]] -> Property
prop_remnantChunkIsNonEmpty (MinChunkSize minChunkSize)
    = remnantChunkSizeIs (> 0) minChunkSize

prop_remnantChunkIsSmall :: MinChunkSize -> [[Vector Word8]] -> Property
prop_remnantChunkIsSmall (MinChunkSize minChunkSize)
    = remnantChunkSizeIs (< minChunkSize) minChunkSize

-- * Test case generation and shrinking

{-
    The type of minimum chunk sizes.

    This type is isomorphic to 'Int' but has a different way of generating test
    cases. Only small, positive integers are generated, and they are generated
    using \(2 \cdot s^{2}\) as the size parameter, where \(s\) refers to the
    original size parameter.

    The reasons for the modification of the size parameter in the
    above-mentioned way are somewhat subtle.

    First, we want the ratio between the average minimum chunk size and the
    average size of a data portion that we feed to a baler to be independent of
    the size parameter. Each data portion is a list of primitive vectors of
    bytes, and arbitrarily generated lists and byte vectors have lengths that
    are small, positive integers. Such integers are \(s/2\) on average. As a
    result, the average size of data fed to a baler is \(s^{2}/4\). By
    generating minimum chunk sizes with \(a \cdot s^{2}\) as the size parameter
    for some constant \(a\), the average minimum chunk size is \(a/2 \cdot
    s^{2}\) and therefore $2a$ times the average size of a data portion fed,
    independently of \(s\).

    Second, we want prompt chunk generation as well as chunk generation after
    only two or more feedings to occur reasonably often. To achieve this to some
    degree, we can tune the parameter \(a\). It appears that \(a\) being \(2\)
    leads to reasonable results.
-}
newtype MinChunkSize = MinChunkSize Int deriving stock Show

fromMinChunkSize :: MinChunkSize -> Int
fromMinChunkSize (MinChunkSize minChunkSize) = minChunkSize

instance Arbitrary MinChunkSize where

    arbitrary = scale (\ size -> 2 * size ^ (2 :: Int)) $
                MinChunkSize <$> getSmall <$> getPositive <$> arbitrary

    shrink = shrinkMap (MinChunkSize . getSmall . getPositive)
                       (Positive . Small . fromMinChunkSize)
