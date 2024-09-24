{-# OPTIONS_GHC -Wno-orphans #-}
{- HLINT ignore "Use camelCase" -}

module Database.LSMTree.Extras.Generators (
    -- * WriteBuffer
    TypedWriteBuffer (..)
  , genTypedWriteBuffer
  , shrinkTypedWriteBuffer
    -- * WithSerialised
  , WithSerialised (..)
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
  , KeyForIndexCompact (..)
  , keyForIndexCompactInvariant
  ) where

import           Control.DeepSeq (NFData)
import           Control.Exception (assert)
import           Data.Bifunctor (bimap)
import           Data.Coerce (coerce)
import           Data.Containers.ListUtils (nubOrd)
import           Data.List (sort)
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import qualified Data.Primitive.ByteArray as BA
import qualified Data.Vector.Primitive as VP
import           Data.Word
import           Database.LSMTree.Common (Range (..))
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.Orphans ()
import           Database.LSMTree.Internal.BlobRef (BlobSpan (..))
import           Database.LSMTree.Internal.Entry (Entry (..), NumEntries (..))
import           Database.LSMTree.Internal.IndexCompact (PageNo (..))
import           Database.LSMTree.Internal.IndexCompactAcc (Append (..))
import qualified Database.LSMTree.Internal.Merge as Merge
import           Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.Serialise
import qualified Database.LSMTree.Internal.Serialise.Class as S.Class
import           Database.LSMTree.Internal.Unsliced (Unsliced, fromUnslicedKey,
                     makeUnslicedKey)
import           Database.LSMTree.Internal.Vector (mkPrimVector)
import           Database.LSMTree.Internal.WriteBuffer (WriteBuffer)
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Monoidal as Monoidal
import qualified Database.LSMTree.Normal as Normal
import           GHC.Generics (Generic)
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Arbitrary (..), Arbitrary1 (..),
                     Arbitrary2 (..), Gen, Property, elements, frequency)
import           Test.QuickCheck.Gen (genDouble)
import           Test.QuickCheck.Instances ()

{-------------------------------------------------------------------------------
  Common LSMTree types
-------------------------------------------------------------------------------}

instance (Arbitrary v, Arbitrary blob) => Arbitrary (Normal.Update v blob) where
  arbitrary = QC.arbitrary2
  shrink = QC.shrink2

instance Arbitrary2 Normal.Update where
  liftArbitrary2 genVal genBlob = frequency
    [ (10, Normal.Insert <$> genVal <*> liftArbitrary genBlob)
    , (1, pure Normal.Delete)
    ]

  liftShrink2 shrinkVal shrinkBlob = \case
    Normal.Insert v blob ->
        Normal.Delete
      : map (uncurry Normal.Insert)
            (liftShrink2 shrinkVal (liftShrink shrinkBlob) (v, blob))
    Normal.Delete ->
      []

instance (Arbitrary v) => Arbitrary (Monoidal.Update v) where
  arbitrary = QC.arbitrary1
  shrink = QC.shrink1

instance Arbitrary1 Monoidal.Update where
  liftArbitrary genVal = frequency
    [ (10, Monoidal.Insert <$> genVal)
    , (5, Monoidal.Mupsert <$> genVal)
    , (1, pure Monoidal.Delete)
    ]

  liftShrink shrinkVal = \case
    Monoidal.Insert v  -> Monoidal.Delete : map Monoidal.Insert (shrinkVal v)
    Monoidal.Mupsert v -> Monoidal.Insert v : map Monoidal.Mupsert (shrinkVal v)
    Monoidal.Delete    -> []

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

instance (Arbitrary v, Arbitrary blob) => Arbitrary (Entry v blob) where
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
  WriteBuffer
-------------------------------------------------------------------------------}

instance Arbitrary WriteBuffer where
  arbitrary = WB.fromMap <$> arbitrary
  shrink = map WB.fromMap . shrink . WB.toMap

--TODO: Rename this. It's not a WriteBuffer (anymore), it's just a bunch of
-- key/value pairs suitable for use in test cases.
type role TypedWriteBuffer nominal nominal nominal
newtype TypedWriteBuffer k v blob = TypedWriteBuffer {
    unTypedWriteBuffer :: Map SerialisedKey (Entry SerialisedValue SerialisedBlob)
  }
  deriving stock (Eq, Show)

instance ( Arbitrary k, Arbitrary v, Arbitrary blob
         , SerialiseKey k, SerialiseValue v, SerialiseValue blob
         )=> Arbitrary (TypedWriteBuffer k v blob) where
  arbitrary = genTypedWriteBuffer
                (arbitrary @k)
                (arbitrary @v)
                (arbitrary @blob)
  shrink = shrinkTypedWriteBuffer
             (shrink @k)
             (shrink @v)
             (shrink @blob)

-- | We cannot implement 'Arbitrary2' since we have constraints on the type
-- parameters.
genTypedWriteBuffer ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => Gen k
  -> Gen v
  -> Gen blob
  -> Gen (TypedWriteBuffer k v blob)
genTypedWriteBuffer genKey genVal genBlob =
    fromKOps <$> QC.listOf (liftArbitrary2 genKey (liftArbitrary2 genVal genBlob))

shrinkTypedWriteBuffer ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => (k -> [k])
  -> (v -> [v])
  -> (blob -> [blob])
  -> TypedWriteBuffer k v blob
  -> [TypedWriteBuffer k v blob]
shrinkTypedWriteBuffer shrinkKey shrinkVal shrinkBlob =
      map fromKOps
    . liftShrink (liftShrink2 shrinkKey (liftShrink2 shrinkVal shrinkBlob))
    . toKOps

-- NOTE: for duplicate keys, only the first one is picked
fromKOps ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => [(k, Entry v blob)]
  -> TypedWriteBuffer k v blob
fromKOps = TypedWriteBuffer . Map.fromList . map serialiseKOp
  where
    serialiseKOp = bimap serialiseKey (bimap serialiseValue serialiseBlob)

toKOps ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue blob)
  => TypedWriteBuffer k v blob
  -> [(k, Entry v blob)]
toKOps = map deserialiseKOp . Map.toList . unTypedWriteBuffer
  where
    deserialiseKOp =
      bimap deserialiseKey (bimap deserialiseValue deserialiseBlob)

{-------------------------------------------------------------------------------
  WithSerialised
-------------------------------------------------------------------------------}

-- | Cache serialised keys
--
-- Also useful for failing tests that have keys as inputs, because the printed
-- 'WithSerialised' values will show both keys and their serialised form.
data WithSerialised k = TestKey k SerialisedKey
  deriving stock Show

instance Eq k => Eq (WithSerialised k) where
  TestKey k1 _ == TestKey k2 _ = k1 == k2

instance Ord k => Ord (WithSerialised k) where
  TestKey k1 _ `compare` TestKey k2 _ = k1 `compare` k2

instance (Arbitrary k, SerialiseKey k) => Arbitrary (WithSerialised k) where
  arbitrary = do
    x <- arbitrary
    pure $ TestKey x (serialiseKey x)
  shrink (TestKey k _) = [TestKey k' (serialiseKey k') | k' <- shrink k]

instance SerialiseKey k => SerialiseKey (WithSerialised k) where
  serialiseKey (TestKey _ (SerialisedKey bytes)) = bytes
  deserialiseKey bytes = TestKey (S.Class.deserialiseKey bytes) (SerialisedKey bytes)

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
    OnePageManyKeys k1 k2 -> OnePageManyKeys <$> shrink k1 <*> shrink k2
    MultiPageOneKey k n   -> [MultiPageOneKey k' n | k' <- shrink k]
                          <> [MultiPageOneKey k n' | n' <- shrink n]

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
  arbitrary = genRawBytes >>= genSlice
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
shrinkRawBytes (RawBytes pvec) = [ RawBytes (VP.fromList ws)
                                 | ws <- QC.shrink (VP.toList pvec) ]

genSlice :: RawBytes -> Gen RawBytes
genSlice (RawBytes pvec) = do
    n <- QC.chooseInt (0, VP.length pvec)
    m <- QC.chooseInt (0, VP.length pvec - n)
    pure $ RawBytes (VP.slice m n pvec)

shrinkSlice :: RawBytes -> [RawBytes]
shrinkSlice (RawBytes pvec) =
    [ RawBytes (VP.slice m n pvec)
    | n <- QC.shrink (VP.length pvec)
    , m <- QC.shrink (VP.length pvec - n)
    ]

-- TODO: makes collisions very unlikely
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
      , let pvec' = VP.take n pvec VP.++ VP.replicate (VP.length pvec - n) 0
      , assert (VP.length pvec' == VP.length pvec) $
        pvec' /= pvec
      ]

deriving newtype instance SerialiseValue LargeRawBytes

-- | Minimum length of 6 bytes.
newtype KeyForIndexCompact =
    KeyForIndexCompact { getKeyForIndexCompact :: RawBytes }
  deriving stock (Eq, Ord, Show)

instance Arbitrary KeyForIndexCompact where
  -- we try to make collisions and close keys more likely (very crudely)
  arbitrary = KeyForIndexCompact <$> frequency
      [ (6, genRawBytesN =<< QC.sized (\s -> QC.chooseInt (8, s + 8)))
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

  shrink (KeyForIndexCompact rb) =
      [ k'
      | rb' <- shrink rb
      , let k' = KeyForIndexCompact rb'
      , keyForIndexCompactInvariant k'
      ]

deriving newtype instance SerialiseKey KeyForIndexCompact

keyForIndexCompactInvariant :: KeyForIndexCompact -> Bool
keyForIndexCompactInvariant (KeyForIndexCompact rb) = RB.size rb >= 8

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
  shrink (BlobSpan x y) = BlobSpan <$> shrink x <*> shrink y

{-------------------------------------------------------------------------------
  Merge
-------------------------------------------------------------------------------}

instance Arbitrary Merge.Level where
  arbitrary = QC.elements [Merge.MidLevel, Merge.LastLevel]
  shrink Merge.LastLevel = [Merge.MidLevel]
  shrink Merge.MidLevel  = []
