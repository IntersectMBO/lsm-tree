{-# OPTIONS_GHC -Wno-orphans #-}

module Database.LSMTree.Extras.Generators (
    -- * WithSerialised
    WithSerialised (..)
    -- * A (logical\/true) page
    -- ** A true page
  , TruePageSummary (..)
  , flattenLogicalPageSummary
    -- ** A logical page
  , LogicalPageSummary (..)
  , shrinkLogicalPageSummary
  , toAppend
    -- * Sequences of (logical\/true) pages
  , Pages (..)
    -- ** Sequences of true pages
  , TruePageSummaries
  , flattenLogicalPageSummaries
    -- ** Sequences of logical pages
  , LogicalPageSummaries
  , toAppends
  , labelPages
  , shrinkPages
  , genPages
  , mkPages
  , pagesInvariant
    -- * Chunking size
  , ChunkSize (..)
  , chunkSizeInvariant
    -- * Serialised keys\/values\/blobs
  , genRawBytes
  , genRawBytesN
  , genRawBytesSized
  , packRawBytesPinnedOrUnpinned
  , LargeRawBytes (..)
  , isKeyForIndexCompact
  , KeyForIndexCompact (..)
  , BiasedKey (..)
    -- * helpers
  , shrinkVec
  ) where

import           Control.DeepSeq (NFData)
import           Control.Exception (assert)
import           Data.Coerce (coerce)
import           Data.Containers.ListUtils (nubOrd)
import           Data.Function ((&))
import           Data.List (nub, sort)
import qualified Data.Primitive.ByteArray as BA
import qualified Data.Vector.Primitive as VP
import           Data.Word
import qualified Database.LSMTree as Full
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.Index (Append (..))
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry (..), NumEntries (..))
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.Page (PageNo (..))
import           Database.LSMTree.Internal.Range (Range (..))
import           Database.LSMTree.Internal.RawBytes (RawBytes (RawBytes))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.Serialise.Class as S.Class
import           Database.LSMTree.Internal.Unsliced (Unsliced, fromUnslicedKey,
                     makeUnslicedKey)
import           Database.LSMTree.Internal.Vector (mkPrimVector)
import           GHC.Generics (Generic)
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary (..), Arbitrary1 (..),
                     Arbitrary2 (..), Gen, Property, elements, frequency)
import           Test.QuickCheck.Gen (genDouble)
import           Test.QuickCheck.Instances ()

{-------------------------------------------------------------------------------
  Common LSMTree types
-------------------------------------------------------------------------------}

instance (Arbitrary v, Arbitrary b) => Arbitrary (Full.Update v b) where
  arbitrary = QC.arbitrary2
  shrink = QC.shrink2

instance Arbitrary2 Full.Update where
  liftArbitrary2 genVal genBlob = frequency
    [ (10, Full.Insert <$> genVal <*> liftArbitrary genBlob)
    , (5, Full.Upsert <$> genVal)
    , (1, pure Full.Delete)
    ]

  liftShrink2 shrinkVal shrinkBlob = \case
    Full.Insert v blob ->
        Full.Delete
      : map (uncurry Full.Insert)
            (liftShrink2 shrinkVal (liftShrink shrinkBlob) (v, blob))
    Full.Upsert v -> Full.Insert v Nothing : map Full.Upsert (shrinkVal v)
    Full.Delete -> []

instance (Arbitrary k, Ord k) => Arbitrary (Range k) where
  arbitrary = do
    key1 <- arbitrary
    key2 <- arbitrary `QC.suchThat` (/= key1)
    (lb, ub) <- frequency
      [ (1, pure (key1, key1))                    -- lb == ub
      , (1, pure (max key1 key2, min key1 key2))  -- lb > ub
      , (8, pure (min key1 key2, max key1 key2))  -- lb < ub
      ]
    elements
      [ FromToExcluding lb ub
      , FromToIncluding lb ub
      ]

  shrink (FromToExcluding f t) =
    uncurry FromToExcluding <$> shrink (f, t)
  shrink (FromToIncluding f t) =
    uncurry FromToIncluding <$> shrink (f, t)

{-------------------------------------------------------------------------------
  Entry
-------------------------------------------------------------------------------}

instance (Arbitrary v, Arbitrary b) => Arbitrary (Entry v b) where
  arbitrary = QC.arbitrary2
  shrink = QC.shrink2

instance Arbitrary2 Entry where
  liftArbitrary2 genVal genBlob = frequency
    [ (5, Insert <$> genVal)
    , (1, InsertWithBlob <$> genVal <*> genBlob)
    , (1, Mupdate <$> genVal)
    , (1, pure Delete)
    ]

  liftShrink2 shrinkVal shrinkBlob = \case
    Insert v           -> Delete : (Insert <$> shrinkVal v)
    InsertWithBlob v b -> [Delete, Insert v]
                       ++ fmap (uncurry InsertWithBlob)
                            (liftShrink2 shrinkVal shrinkBlob (v, b))
    Mupdate v          -> Delete : Insert v : (Mupdate <$> shrinkVal v)
    Delete             -> []

{-------------------------------------------------------------------------------
  WithSerialised
-------------------------------------------------------------------------------}

-- | Cache serialised keys
--
-- Also useful for failing tests that have keys as inputs, because the printed
-- 'WithSerialised' values will show both keys and their serialised form.
data WithSerialised k = WithSerialised k SerialisedKey
  deriving stock Show

instance Eq k => Eq (WithSerialised k) where
  WithSerialised k1 _ == WithSerialised k2 _ = k1 == k2

instance Ord k => Ord (WithSerialised k) where
  WithSerialised k1 _ `compare` WithSerialised k2 _ = k1 `compare` k2

instance (Arbitrary k, SerialiseKey k) => Arbitrary (WithSerialised k) where
  arbitrary = do
    x <- arbitrary
    pure $ WithSerialised x (serialiseKey x)
  shrink (WithSerialised k _) = [WithSerialised k' (serialiseKey k') | k' <- shrink k]

instance SerialiseKey k => SerialiseKey (WithSerialised k) where
  serialiseKey (WithSerialised _ (SerialisedKey bytes)) = bytes
  deserialiseKey bytes = WithSerialised (S.Class.deserialiseKey bytes) (SerialisedKey bytes)

{-------------------------------------------------------------------------------
  Other number newtypes
-------------------------------------------------------------------------------}

instance Arbitrary PageNo where
  arbitrary = coerce (arbitrary @(QC.NonNegative Int))
  shrink = coerce (shrink @(QC.NonNegative Int))

instance Arbitrary NumEntries where
  arbitrary = coerce (arbitrary @(QC.NonNegative Int))
  shrink = coerce (shrink @(QC.NonNegative Int))

{-------------------------------------------------------------------------------
  True page
-------------------------------------------------------------------------------}

-- | A summary of min/max information for keys on a /true/ page.
--
-- A true page corresponds directly to a disk page. See 'LogicalPageSummary' for
-- contrast.
data TruePageSummary k = TruePageSummary { tpsMinKey :: k, tpsMaxKey :: k }

flattenLogicalPageSummary :: LogicalPageSummary k -> [TruePageSummary k]
flattenLogicalPageSummary = \case
    OnePageOneKey k       -> [TruePageSummary k k]
    OnePageManyKeys k1 k2 -> [TruePageSummary k1 k2]
    MultiPageOneKey k n   -> replicate (fromIntegral n+1) (TruePageSummary k k)

{-------------------------------------------------------------------------------
  Logical page
-------------------------------------------------------------------------------}

-- | A summary of min/max information for keys on a /logical/ page.
--
-- A key\/operation pair can fit onto a single page, or the operation is so
-- large that its bytes flow over into subsequent pages. A logical page makes
-- this overflow explicit. Making these cases explicit in the representation
-- makes generating and shrinking test cases easier.
data LogicalPageSummary k =
    OnePageOneKey   k
  | OnePageManyKeys k k
  | MultiPageOneKey k Word32 -- ^ number of overflow pages
  deriving stock (Show, Generic, Functor)
  deriving anyclass NFData

toAppend :: LogicalPageSummary SerialisedKey -> Append
toAppend (OnePageOneKey k)       = AppendSinglePage k k
toAppend (OnePageManyKeys k1 k2) = AppendSinglePage k1 k2
toAppend (MultiPageOneKey k n)   = AppendMultiPage k n

shrinkLogicalPageSummary :: Arbitrary k => LogicalPageSummary k -> [LogicalPageSummary k]
shrinkLogicalPageSummary = \case
    OnePageOneKey k       -> OnePageOneKey <$> shrink k
    OnePageManyKeys k1 k2 -> [
        OnePageManyKeys k1' k2'
      | (k1', k2') <- shrink (k1, k2)
      ]
    MultiPageOneKey k n   -> [
        MultiPageOneKey k' n'
      | (k', n') <- shrink (k, n)
      ]

{-------------------------------------------------------------------------------
  Sequences of (logical\/true) pages
-------------------------------------------------------------------------------}

-- | Sequences of (logical\/true) pages
--
-- INVARIANT: The sequence consists of multiple pages in sorted order (keys are
-- sorted within a page and across pages).
newtype Pages fp k = Pages { getPages :: [fp k] }
  deriving stock (Show, Generic, Functor)
  deriving anyclass NFData

class TrueNumberOfPages fp where
  trueNumberOfPages :: Pages fp k -> Int

instance TrueNumberOfPages LogicalPageSummary where
  trueNumberOfPages :: LogicalPageSummaries k -> Int
  trueNumberOfPages = length . getPages . flattenLogicalPageSummaries

instance TrueNumberOfPages TruePageSummary where
  trueNumberOfPages :: TruePageSummaries k -> Int
  trueNumberOfPages = length . getPages

{-------------------------------------------------------------------------------
  Sequences of true pages
-------------------------------------------------------------------------------}

type TruePageSummaries    k = Pages TruePageSummary k

flattenLogicalPageSummaries :: LogicalPageSummaries k -> TruePageSummaries k
flattenLogicalPageSummaries (Pages ps) = Pages (concatMap flattenLogicalPageSummary ps)

{-------------------------------------------------------------------------------
  Sequences of logical pages
-------------------------------------------------------------------------------}

type LogicalPageSummaries k = Pages LogicalPageSummary k

toAppends :: SerialiseKey k => LogicalPageSummaries k -> [Append]
toAppends (Pages ps) = fmap (toAppend . fmap serialiseKey) ps

--
-- Labelling
--

labelPages :: LogicalPageSummaries k -> (Property -> Property)
labelPages ps =
      QC.tabulate "# True pages" [showPowersOf10 nTruePages]
    . QC.tabulate "# Logical pages" [showPowersOf10 nLogicalPages]
    . QC.tabulate "# OnePageOneKey logical pages" [showPowersOf10 n1]
    . QC.tabulate "# OnePageManyKeys logical pages" [showPowersOf10 n2]
    . QC.tabulate "# MultiPageOneKey logical pages" [showPowersOf10 n3]
  where
    nLogicalPages = length $ getPages ps
    nTruePages = trueNumberOfPages ps

    (n1,n2,n3) = counts (getPages ps)

    counts :: [LogicalPageSummary k] -> (Int, Int, Int)
    counts []       = (0, 0, 0)
    counts (lp:lps) = let (x, y, z) = counts lps
                      in case lp of
                        OnePageOneKey{}   -> (x+1, y, z)
                        OnePageManyKeys{} -> (x, y+1, z)
                        MultiPageOneKey{} -> (x, y, z+1)

--
-- Generation and shrinking
--

instance (Arbitrary k, Ord k)
      => Arbitrary (LogicalPageSummaries k) where
  arbitrary = genPages 0.03 (QC.choose (0, 16)) 0.01
  shrink = shrinkPages

shrinkPages ::
     (Arbitrary k, Ord k)
  => LogicalPageSummaries k
  -> [LogicalPageSummaries k]
shrinkPages (Pages ps) = [
      Pages ps'
    | ps' <- QC.shrinkList shrinkLogicalPageSummary ps, pagesInvariant (Pages ps')
    ]

genPages ::
     (Arbitrary k, Ord k)
  => Double -- ^ Probability of a value being larger-than-page
  -> Gen Word32 -- ^ Number of overflow pages for a larger-than-page value
  -> Double -- ^ Probability of generating a page with only one key and value,
            --   which does /not/ span multiple pages.
  -> Gen (LogicalPageSummaries k)
genPages p genN p' = do
    ks <- arbitrary
    mkPages p genN p' ks

mkPages ::
     forall k. Ord k
  => Double -- ^ Probability of a value being larger-than-page
  -> Gen Word32 -- ^ Number of overflow pages for a larger-than-page value
  -> Double -- ^ Probability of generating a page with only one key and value,
            --   which does /not/ span multiple pages.
  -> [k]
  -> Gen (LogicalPageSummaries k)
mkPages p genN p' =
    fmap Pages . go . nubOrd . sort
  where
    go :: [k] -> Gen [LogicalPageSummary k]
    go []          = pure []
    go [k]         = do
      b <- largerThanPage
      if b then pure . MultiPageOneKey k <$> genN
           else pure [OnePageOneKey k]
      -- the min and max key are allowed to be the same
    go  (k1:k2:ks) = do
      b <- largerThanPage
      b' <- onePageOneKey
      if b then (:) <$> (MultiPageOneKey k1 <$> genN) <*> go (k2 : ks)
           else if b' then (OnePageOneKey   k1 :)    <$> go (k2 : ks)
                      else (OnePageManyKeys k1 k2 :) <$> go ks

    largerThanPage :: Gen Bool
    largerThanPage = genDouble >>= \x -> pure (x < p)

    onePageOneKey :: Gen Bool
    onePageOneKey = genDouble >>= \x -> pure (x < p')

pagesInvariant :: Ord k => LogicalPageSummaries k -> Bool
pagesInvariant (Pages ps0) =
       sort ks   == ks
    && nubOrd ks == ks
  where
    ks = flatten ps0

    flatten :: Eq k => [LogicalPageSummary k] -> [k]
    flatten []            = []
                          -- the min and max key are allowed to be the same
    flatten (p:ps) = case p of
      OnePageOneKey k       -> k : flatten ps
      OnePageManyKeys k1 k2 -> k1 : k2 : flatten ps
      MultiPageOneKey k _   -> k : flatten ps

{-------------------------------------------------------------------------------
  Chunking size
-------------------------------------------------------------------------------}

newtype ChunkSize = ChunkSize Int
  deriving stock Show
  deriving newtype Num

instance Arbitrary ChunkSize where
  arbitrary = ChunkSize <$> QC.chooseInt (chunkSizeLB, chunkSizeUB)
  shrink (ChunkSize csize) = [
        ChunkSize csize'
      | csize' <- shrink csize
      , chunkSizeInvariant (ChunkSize csize')
      ]

chunkSizeLB, chunkSizeUB :: Int
chunkSizeLB = 1
chunkSizeUB = 20

chunkSizeInvariant :: ChunkSize -> Bool
chunkSizeInvariant (ChunkSize csize) = chunkSizeLB <= csize && csize <= chunkSizeUB

{-------------------------------------------------------------------------------
  Serialised keys/values/blobs
-------------------------------------------------------------------------------}

instance Arbitrary RawBytes where
  arbitrary = do
    QC.NonNegative (QC.Small prefixLength)  <- arbitrary
    QC.NonNegative (QC.Small payloadLength) <- arbitrary
    QC.NonNegative (QC.Small suffixLength)  <- arbitrary
    base <- genRawBytesN (prefixLength + payloadLength + suffixLength)
    return (base & RB.drop prefixLength & RB.take payloadLength)
  shrink rb = shrinkSlice rb ++ shrinkRawBytes rb

genRawBytesN :: Int -> Gen RawBytes
genRawBytesN n =
    packRawBytesPinnedOrUnpinned <$> arbitrary <*> QC.vectorOf n arbitrary

genRawBytes :: Gen RawBytes
genRawBytes =
    packRawBytesPinnedOrUnpinned <$> arbitrary <*> QC.listOf arbitrary

genRawBytesSized :: Int -> Gen RawBytes
genRawBytesSized n = QC.resize n genRawBytes

packRawBytesPinnedOrUnpinned :: Bool -> [Word8] -> RawBytes
packRawBytesPinnedOrUnpinned False = RB.pack
packRawBytesPinnedOrUnpinned True  = \ws ->
    let len = length ws in
    RB.RawBytes $ mkPrimVector 0 len $ BA.runByteArray $ do
      mba <- BA.newPinnedByteArray len
      sequence_ [ BA.writeByteArray mba i w | (i, w) <- zip [0..] ws ]
      return mba

shrinkRawBytes :: RawBytes -> [RawBytes]
shrinkRawBytes (RawBytes pvec) =
    [ RawBytes pvec'
    | pvec' <- shrinkVec shrinkByte pvec
    ]
  where
    -- no need to try harder shrinking individual bytes
    shrinkByte b = nub (takeWhile (< b) [0, b `div` 2])

-- | Based on QuickCheck's 'shrinkList' (behaves identically, see tests).
shrinkVec :: VP.Prim a => (a -> [a]) -> VP.Vector a -> [VP.Vector a]
shrinkVec shr vec =
    concat [ removeBlockOf k | k <- takeWhile (> 0) (iterate (`div` 2) len) ]
    ++ shrinkOne
  where
    len = VP.length vec

    shrinkOne =
        [ vec VP.// [(i, x')]
        | i <- [0 .. len-1]
        , let x = vec VP.! i
        , x' <- shr x
        ]

    removeBlockOf k =
        [ VP.take i vec VP.++ VP.drop (i + k) vec
        | i <- [0, k .. len - k]
        ]

genSlice :: RawBytes -> Gen RawBytes
genSlice (RawBytes pvec) = do
    n <- QC.chooseInt (0, VP.length pvec)
    m <- QC.chooseInt (0, VP.length pvec - n)
    pure $ RawBytes (VP.slice m n pvec)

shrinkSlice :: RawBytes -> [RawBytes]
shrinkSlice (RawBytes pvec) =
    [ RawBytes (VP.take len' pvec)
    | len' <- QC.shrink len
    ] ++
    [ RawBytes (VP.drop (len - len') pvec)
    | len' <- QC.shrink len
    ]
  where
    len = VP.length pvec

deriving newtype instance Arbitrary SerialisedKey

instance Arbitrary SerialisedValue where
  -- good mix of sizes, including larger than two pages, also some slices
  arbitrary = SerialisedValue <$> frequency
      [ (16, arbitrary)
      , ( 4, genRawBytesN =<< QC.chooseInt ( 100,  1000))
      , ( 2, genRawBytesN =<< QC.chooseInt (1000,  4000))
      , ( 1, genRawBytesN =<< QC.chooseInt (4000, 10000))
      , ( 1, genSlice =<< genRawBytesN =<< QC.chooseInt (0, 10000))
      ]
  shrink (SerialisedValue rb)
      | RB.size rb > 64 = coerce (shrink (LargeRawBytes rb))
      | otherwise       = coerce (shrink rb)

deriving newtype instance Arbitrary SerialisedBlob

newtype LargeRawBytes = LargeRawBytes RawBytes
  deriving stock Show
  deriving newtype NFData

instance Arbitrary LargeRawBytes where
  arbitrary = genRawBytesSized (4096*3) >>= fmap LargeRawBytes . genSlice
  shrink (LargeRawBytes rb) =
      map LargeRawBytes (shrinkSlice rb)
      -- After shrinking length, don't shrink content using normal list shrink
      -- as that's too slow. We try zeroing out long suffixes of the bytes
      -- (since for large raw bytes in page format, the interesting information
      -- is at the start and the suffix is just the value.
   ++ [ LargeRawBytes (RawBytes pvec')
      | let (RawBytes pvec) = rb
      , n <- QC.shrink (VP.length pvec)
      , assert (n >= 0) True  -- negative values would make pvec' longer
      , let pvec' = VP.take n pvec VP.++ VP.replicate (VP.length pvec - n) 0
      , assert (VP.length pvec' == VP.length pvec) $
        pvec' /= pvec
      ]

deriving newtype instance SerialiseValue LargeRawBytes

-- Serialised keys for the compact index must be at least 8 bytes long.

genKeyForIndexCompact :: Gen RawBytes
genKeyForIndexCompact =
    genRawBytesN =<< QC.sized (\s -> QC.chooseInt (8, s + 8))

isKeyForIndexCompact :: RawBytes -> Bool
isKeyForIndexCompact rb = RB.size rb >= 8

newtype KeyForIndexCompact =
    KeyForIndexCompact { getKeyForIndexCompact :: RawBytes }
  deriving stock (Eq, Ord, Show)

instance Arbitrary KeyForIndexCompact where
  arbitrary =
      KeyForIndexCompact <$> genKeyForIndexCompact
  shrink (KeyForIndexCompact rawBytes) =
      [KeyForIndexCompact rawBytes' | rawBytes' <- shrink rawBytes,
                                      isKeyForIndexCompact rawBytes']

deriving newtype instance SerialiseKey KeyForIndexCompact

-- we try to make collisions and close keys more likely (very crudely)
arbitraryBiasedKey :: (RawBytes -> k) -> Gen RawBytes -> Gen k
arbitraryBiasedKey fromRB genUnbiased = fromRB <$> frequency
    [ (6, genUnbiased)
    , (1, do
        lastByte <- QC.sized $ skewedWithMax . fromIntegral
        return (RB.pack ([1,3,3,7,0,1,7] <> [lastByte]))
      )
    ]
    where
      -- generates a value in range from 0 to ub, but skewed towards low end
      skewedWithMax ub0 = do
        ub1 <- QC.chooseBoundedIntegral (0, ub0)
        ub2 <- QC.chooseBoundedIntegral (0, ub1)
        QC.chooseBoundedIntegral (0, ub2)

newtype BiasedKey = BiasedKey { getBiasedKey :: RawBytes }
  deriving stock (Eq, Ord, Show)
  deriving newtype NFData

instance Arbitrary BiasedKey where
  arbitrary = arbitraryBiasedKey BiasedKey arbitrary

  shrink (BiasedKey rb) = [BiasedKey rb' | rb' <- shrink rb]

deriving newtype instance SerialiseKey BiasedKey

{-------------------------------------------------------------------------------
  Unsliced
-------------------------------------------------------------------------------}

instance Arbitrary (Unsliced SerialisedKey) where
  arbitrary = makeUnslicedKey <$> arbitrary
  shrink = fmap makeUnslicedKey .  shrink . fromUnslicedKey

{-------------------------------------------------------------------------------
  BlobRef
-------------------------------------------------------------------------------}

instance Arbitrary BlobSpan where
  arbitrary = BlobSpan <$> arbitrary <*> arbitrary
  shrink (BlobSpan x y) = [ BlobSpan x' y' | (x', y') <- shrink (x, y) ]

{-------------------------------------------------------------------------------
  Merge
-------------------------------------------------------------------------------}

instance Arbitrary Merge.MergeType where
  arbitrary = QC.elements
      [Merge.MergeTypeMidLevel, Merge.MergeTypeLastLevel, Merge.MergeTypeUnion]
  shrink Merge.MergeTypeMidLevel  = []
  shrink Merge.MergeTypeLastLevel = [Merge.MergeTypeMidLevel]
  shrink Merge.MergeTypeUnion     = [Merge.MergeTypeLastLevel]

instance Arbitrary Merge.LevelMergeType where
  arbitrary = QC.elements [Merge.MergeMidLevel, Merge.MergeLastLevel]
  shrink Merge.MergeMidLevel  = []
  shrink Merge.MergeLastLevel = [Merge.MergeMidLevel]

instance Arbitrary Merge.TreeMergeType where
  arbitrary = QC.elements [Merge.MergeLevel, Merge.MergeUnion]
  shrink Merge.MergeLevel = []
  shrink Merge.MergeUnion = [Merge.MergeLevel]
