{-# LANGUAGE DeriveAnyClass             #-}
{-# LANGUAGE DeriveGeneric              #-}
{-# LANGUAGE DerivingStrategies         #-}
{-# LANGUAGE FlexibleContexts           #-}
{-# LANGUAGE FlexibleInstances          #-}
{-# LANGUAGE GADTs                      #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE LambdaCase                 #-}
{-# LANGUAGE NamedFieldPuns             #-}
{-# LANGUAGE ScopedTypeVariables        #-}
{-# LANGUAGE TupleSections              #-}
{-# LANGUAGE TypeApplications           #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Database.LSMTree.Internal.Lookup (
    tests
    -- * internals
  , InMemLookupData (..)
  , SmallList (..)
  ) where

import           Control.DeepSeq
import           Control.Exception
import           Control.Monad.ST.Strict
import           Control.RefCount
import           Data.Arena (newArenaManager, withUnmanagedArena)
import           Data.Bifunctor
import           Data.BloomFilter (Bloom)
import qualified Data.BloomFilter as Bloom
import           Data.Coerce (coerce)
import           Data.Either (rights)
import qualified Data.Foldable as F
import qualified Data.List as List
import           Data.Map.Strict (Map)
import qualified Data.Map.Strict as Map
import           Data.Maybe
import           Data.Monoid (Endo (..))
import qualified Data.Set as Set
import qualified Data.Vector as V
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import           Data.Word
import           Database.LSMTree.Extras
import           Database.LSMTree.Extras.Generators
import           Database.LSMTree.Extras.RunData (RunData (..),
                     liftArbitrary2Map, liftShrink2Map,
                     unsafeFlushAsWriteBuffer)
import           Database.LSMTree.Internal.BlobRef
import           Database.LSMTree.Internal.Entry as Entry
import           Database.LSMTree.Internal.Index.Compact
import           Database.LSMTree.Internal.Lookup
import           Database.LSMTree.Internal.Page (PageNo (PageNo), PageSpan (..))
import           Database.LSMTree.Internal.Paths (RunFsPaths (..))
import qualified Database.LSMTree.Internal.RawBytes as RB
import           Database.LSMTree.Internal.RawOverflowPage
import           Database.LSMTree.Internal.RawPage
import qualified Database.LSMTree.Internal.Run as Run
import           Database.LSMTree.Internal.RunAcc as Run
import           Database.LSMTree.Internal.RunNumber
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Serialise.Class
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import qualified Database.LSMTree.Internal.WriteBufferBlobs as WBB
import           GHC.Generics
import qualified System.FS.API as FS
import           System.FS.API (Handle (..), mkFsPath)
import qualified System.FS.BlockIO.API as FS
import           System.FS.BlockIO.API
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.Arbitrary (deepseqInvariant,
                     prop_arbitraryAndShrinkPreserveInvariant,
                     prop_forAllArbitraryAndShrinkPreserveInvariant)
import           Test.Util.FS (withTempIOHasBlockIO)

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.Lookup" [
      testGroup "models" [
          testProperty "prop_bloomQueriesModel" $
            prop_bloomQueriesModel
        , testProperty "prop_indexSearchesModel" $
            prop_indexSearchesModel
        , testProperty "prop_prepLookupsModel" $
            prop_prepLookupsModel
        , testProperty "input distribution" $ \dats ->
            tabulateInMemLookupDataN (getSmallList dats) True
        ]
    , testGroup "With multi-page values" [
          testGroup "InMemLookupData" $
            prop_arbitraryAndShrinkPreserveInvariant (deepseqInvariant @(InMemLookupData SerialisedKey SerialisedValue BlobSpan))
        , localOption (QuickCheckMaxSize 1000) $
          testProperty "prop_inMemRunLookupAndConstruction" prop_inMemRunLookupAndConstruction
        ]
    , testGroup "Without multi-page values" [
          testGroup "InMemLookupData" $
            prop_forAllArbitraryAndShrinkPreserveInvariant
              genNoMultiPage
              shrinkNoMultiPage
              (deepseqInvariant @(InMemLookupData SerialisedKey SerialisedValue BlobSpan))
        , localOption (QuickCheckMaxSize 1000) $
          testProperty "prop_inMemRunLookupAndConstruction" $
            forAllShrink genNoMultiPage shrinkNoMultiPage prop_inMemRunLookupAndConstruction
        ]

    , testProperty "prop_roundtripFromWriteBufferLookupIO" $
        prop_roundtripFromWriteBufferLookupIO
    ]
  where
    genNoMultiPage = liftArbitrary2 arbitrary arbitrary
    shrinkNoMultiPage = liftShrink2 shrink shrink

{-------------------------------------------------------------------------------
  Models
-------------------------------------------------------------------------------}

prop_bloomQueriesModel ::
     SmallList (InMemLookupData SerialisedKey SerialisedValue BlobSpan)
  -> Property
prop_bloomQueriesModel dats =
    real === model
  where
    runs = getSmallList $ fmap (mkTestRun . runData) dats
    blooms = fmap snd3 runs
    lookupss = concatMap lookups $ getSmallList dats
    real  = map (\(RunIxKeyIx rix kix) -> (rix,kix)) $ VP.toList $
            bloomQueries (V.fromList blooms) (V.fromList lookupss)
    model = bloomQueriesModel blooms lookupss

bloomQueriesModel :: [Bloom SerialisedKey] -> [SerialisedKey] -> [(RunIx, KeyIx)]
bloomQueriesModel blooms ks = [
      (rix, kix)
    | (rix, b) <- rs'
    , (kix, k) <- ks'
    , Bloom.elem k b
    ]
  where
    rs' = zip [0..] blooms
    ks' = zip [0..] ks

prop_indexSearchesModel ::
     SmallList (InMemLookupData SerialisedKey SerialisedValue BlobSpan)
  -> Property
prop_indexSearchesModel dats =
    forAllShrink (rkixsGen rkixsAll) shrink $ \rkixs ->
           real (VU.fromList rkixs) === model rkixs
      .&&. real (VU.fromList rkixs) === model rkixs
      .&&. length rkixs === length (model rkixs)
  where
    rkixsAll = [(rix, kix) | (rix,_) <- zip [0..] runs, (kix,_) <- zip [0..] lookupss]
    rkixsGen [] = pure []
    rkixsGen xs = listOf (elements xs)

    runs = getSmallList $ fmap (mkTestRun . runData) dats
    lookupss = concatMap lookups $ getSmallList dats
    real rkixs = runST $ withUnmanagedArena $ \arena -> do
      let rs = V.fromList (fmap runWithHandle runs)
          ks = V.fromList lookupss
      res <- indexSearches arena (V.map thrd3 rs) (V.map fst3 rs) ks
               ((V.convert . V.map (uncurry RunIxKeyIx) . V.convert) rkixs)
      pure $ V.map ioopPageSpan res
    model rkixs = V.fromList $ indexSearchesModel (fmap thrd3 runs) lookupss $ rkixs

indexSearchesModel ::
     [IndexCompact]
  -> [SerialisedKey]
  -> [(RunIx, KeyIx)]
  -> [PageSpan]
indexSearchesModel cs ks rkixs =
    flip fmap rkixs $ \(rix, kix) ->
      let c = cs List.!! rix
          k = ks List.!! kix
      in  search k c

prop_prepLookupsModel ::
     SmallList (InMemLookupData SerialisedKey SerialisedValue BlobSpan)
  -> Property
prop_prepLookupsModel dats = real === model
  where
    runs = getSmallList $ fmap (mkTestRun . runData) dats
    lookupss = concatMap lookups $ getSmallList dats
    real = runST $ withUnmanagedArena $ \arena -> do
      let rs = V.fromList (fmap runWithHandle runs)
          ks = V.fromList lookupss
      (kixs, ioops) <- prepLookups
                         arena
                         (V.map snd3 rs)
                         (V.map thrd3 rs)
                         (V.map fst3 rs) ks
      pure ( map (\(RunIxKeyIx r k) -> (r,k)) (VP.toList kixs)
           , map ioopPageSpan (V.toList ioops)
           )
    model = prepLookupsModel (fmap (\x -> (snd3 x, thrd3 x)) runs) lookupss

prepLookupsModel ::
     [(Bloom SerialisedKey, IndexCompact)]
  -> [SerialisedKey]
  -> ([(RunIx, KeyIx)], [PageSpan])
prepLookupsModel rs ks = unzip
    [ ((rix, kix), pspan)
    | (rix, (b, c)) <- zip [0..] rs
    , (kix, k) <- zip [0..] ks
    , Bloom.elem k b
    , let pspan = search k c
    ]

{-------------------------------------------------------------------------------
  Round-trip
-------------------------------------------------------------------------------}

-- | Construct a run incrementally, then test a number of positive and negative lookups.
prop_inMemRunLookupAndConstruction ::
     InMemLookupData SerialisedKey SerialisedValue BlobSpan
  -> Property
prop_inMemRunLookupAndConstruction dat =
      tabulateInMemLookupData dat run
    $ conjoinF (fmap checkMaybeInRun keysMaybeInRun) .&&. conjoinF (fmap checkNotInRun keysNotInRun)
  where
    InMemLookupData{runData, lookups} = dat

    run = mkTestRun runData
    keys = V.fromList lookups
    -- prepLookups says that a key /could be/ in the given page
    keysMaybeInRun = runST $ withUnmanagedArena $ \arena -> do
      (kixs, ioops) <- let r = V.singleton (runWithHandle run)
                       in  prepLookups
                             arena
                             (V.map snd3 r)
                             (V.map thrd3 r)
                             (V.map fst3 r)
                             keys
      let ks = V.map (V.fromList lookups V.!)
                     (V.convert (VP.map (\(RunIxKeyIx _r k) -> k) kixs))
          pss = V.map (handleRaw . ioopHandle) ioops
          pspans = V.map (ioopPageSpan) ioops
      pure $ zip3 (V.toList ks) (V.toList pss) (V.toList pspans)
    -- prepLookups says that a key /is definitely not/ in the given page
    keysNotInRun = Set.toList (Set.fromList lookups Set.\\ Set.fromList (fmap (\(x,_,_) -> x) keysMaybeInRun))

    -- Check that a key /is definitely not/ in the given page.
    checkNotInRun :: SerialisedKey -> Property
    checkNotInRun k =
          tabulate1Pre (classifyBin (isJust truth) False)
        $ counterexample ("checkNotInRun: " <> show k)
        $ truth === test
      where truth = Map.lookup k runData
            test  = Nothing

    -- | Check that a key /could be/ in the given page
    checkMaybeInRun :: ( SerialisedKey
                       , Map Int (Either RawPage RawOverflowPage)
                       , PageSpan )
                    -> Property
    checkMaybeInRun (k, ps, PageSpan (PageNo i) (PageNo j))
      | i <= j    = tabulate "PageSpan size" [showPowersOf10 $ j - i + 1]
                  $ tabulate1Pre (classifyBin (isJust truth) True)
                  $ counterexample ("checkMaybeInRun: " <> show k)
                  $ truth === test
      | otherwise = error "impossible: end of a page span can not come before its start"
      where
        truth = Map.lookup k runData
        test  =
          case ps Map.! i of
            Left rawPage ->
              case rawPageLookup rawPage (coerce k) of
                LookupEntryNotPresent       -> Nothing
                LookupEntry entry           -> Just entry
                LookupEntryOverflow entry n -> Just (first (concatOverflow n) entry)
            Right _rawOverflowPage ->
              error "looked up overflow page!"

        -- read remaining bytes for a multi-page value, and append it to the
        -- prefix we already have
        concatOverflow :: Word32 -> SerialisedValue -> SerialisedValue
        concatOverflow = coerce $ \(n :: Word32) (v :: RawBytes) ->
            v <> RB.take (fromIntegral n)
                         (mconcat $ map rawOverflowPageRawBytes overflowPages)
          where
            start = i + 1
            size  = j - i
            overflowPages :: [RawOverflowPage]
            overflowPages = rights
                          . Map.elems
                          . Map.take size
                          . Map.drop start $ ps

    tabulate1Pre :: BinaryClassification -> Property -> Property
    tabulate1Pre  x = tabulate "Lookup classification: pre intra-page lookup"  [show x]


{-------------------------------------------------------------------------------
  Round-trip lookups in IO
-------------------------------------------------------------------------------}

prop_roundtripFromWriteBufferLookupIO ::
     SmallList (InMemLookupData SerialisedKey SerialisedValue SerialisedBlob)
  -> Property
prop_roundtripFromWriteBufferLookupIO (SmallList dats) =
    ioProperty $
    withTempIOHasBlockIO "prop_roundtripFromWriteBufferLookupIO" $ \hfs hbio ->
    withRuns hfs hbio dats $ \wb wbblobs runs -> do
    let model :: Map SerialisedKey (Entry SerialisedValue SerialisedBlob)
        model = Map.unionsWith (Entry.combine resolveV) (map runData dats)
        keys  = V.fromList [ k | InMemLookupData{lookups} <- dats
                               , k <- lookups ]
        modelres :: V.Vector (Maybe (Entry SerialisedValue SerialisedBlob))
        modelres = V.map (\k -> Map.lookup k model) keys
    arenaManager <- newArenaManager
    realres <-
      fetchBlobs hfs =<< -- retrieve blobs to match type of model result
      lookupsIO
        hbio
        arenaManager
        resolveV
        wb wbblobs
        runs
        (V.map (\(DeRef r) -> Run.runFilter   r) runs)
        (V.map (\(DeRef r) -> Run.runIndex    r) runs)
        (V.map (\(DeRef r) -> Run.runKOpsFile r) runs)
        keys
    pure $ modelres === realres
  where
    resolveV = \(SerialisedValue v1) (SerialisedValue v2) -> SerialisedValue (v1 <> v2)

    fetchBlobs :: FS.HasFS IO h
               ->    (V.Vector (Maybe (Entry v (WeakBlobRef IO h))))
               -> IO (V.Vector (Maybe (Entry v SerialisedBlob)))
    fetchBlobs hfs = traverse (traverse (traverse (readWeakBlobRef hfs)))

-- | Given a bunch of 'InMemLookupData', prepare the data into the form needed
-- for 'lookupsIO': a write buffer (and blobs) and a vector of on-disk runs.
-- Also passes the model and the keys to look up to the inner action.
--
withRuns :: FS.HasFS IO h
         -> FS.HasBlockIO IO h
         -> [InMemLookupData SerialisedKey SerialisedValue SerialisedBlob]
         -> (   WB.WriteBuffer
             -> Ref (WBB.WriteBufferBlobs IO h)
             -> V.Vector (Ref (Run.Run IO h))
             -> IO a)
         -> IO a
withRuns hfs _ [] action =
    bracket
      (WBB.new hfs (FS.mkFsPath ["wbblobs"]))
      releaseRef
      (\wbblobs -> action WB.empty wbblobs V.empty)

withRuns hfs hbio (wbdat:rundats) action =
    bracket
      (do wbblobs <- WBB.new hfs (FS.mkFsPath ["wbblobs"])
          wbkops <- traverse (traverse (WBB.addBlob hfs wbblobs))
                             (runData wbdat)
          let wb = WB.fromMap wbkops
          runs <-
            V.fromList <$>
            sequence
              [ unsafeFlushAsWriteBuffer hfs hbio fsPaths (RunData runData)
              | (i, InMemLookupData{runData}) <- zip [1..] rundats
              , let fsPaths = RunFsPaths (FS.mkFsPath []) (RunNumber i)
              ]
          return (wb, wbblobs, runs))

      (\(_wb, wbblobs, runs) -> do
          V.mapM_ releaseRef runs
          releaseRef wbblobs)

      (\(wb, wbblobs, runs) ->
          action wb wbblobs runs)

{-------------------------------------------------------------------------------
  Utils
-------------------------------------------------------------------------------}

newtype SmallList a = SmallList { getSmallList :: [a] }
  deriving stock (Show, Eq)
  deriving newtype (Functor, Foldable)

instance Arbitrary a => Arbitrary (SmallList a) where
  arbitrary = do
      n <- chooseInt (0, 5)
      SmallList <$> vectorOf n arbitrary
  shrink = fmap SmallList . shrink . getSmallList

conjoinF :: (Testable prop, Foldable f) => f prop -> Property
conjoinF = conjoin . F.toList

ioopPageSpan :: IOOp s h -> PageSpan
ioopPageSpan ioop =
    assert (fst x `mod` 4096 == 0) $
    assert (snd x `mod` 4096 == 0) $
    assert (start >= 0) $
    assert (end >= start)
    PageSpan {
        pageSpanStart = PageNo start
      , pageSpanEnd   = PageNo end
      }
  where
    start = fst x `div` 4096
    size  = (snd x `div` 4096) - 1
    end = start + size

    x = case ioop of
      IOOpRead  _ foff _ _ c -> (fromIntegral foff, fromIntegral c)
      IOOpWrite _ foff _ _ c -> (fromIntegral foff, fromIntegral c)

fst3 :: (a, b, c) -> a
fst3 (h, _, _) = h

snd3 :: (a, b, c) -> b
snd3 (_, b, _) = b

thrd3 :: (a, b, c) -> c
thrd3 (_, _, c) = c

{-------------------------------------------------------------------------------
  Test run
-------------------------------------------------------------------------------}

runWithHandle ::
     TestRun
  -> ( Handle (Map Int (Either RawPage RawOverflowPage))
     , Bloom SerialisedKey, IndexCompact
     )
runWithHandle (rawPages, b, ic) = (Handle rawPages (mkFsPath ["do not use"]), b, ic)

type TestRun = (Map Int (Either RawPage RawOverflowPage), Bloom SerialisedKey, IndexCompact)

mkTestRun :: Map SerialisedKey (Entry SerialisedValue BlobSpan) -> TestRun
mkTestRun dat = (rawPages, b, ic)
  where
    nentries = NumEntries (Map.size dat)

    -- one-shot run construction
    (pages, b, ic) = runST $ do
      racc <- Run.new nentries (RunAllocFixed 10)
      let kops = Map.toList dat
      psopss <- traverse (uncurry (Run.addKeyOp racc)) kops
      (mp, _ , b', ic', _) <- Run.unsafeFinalise racc
      let pages' = [ p | (ps, ops, _) <- psopss
                      , p <- map Left ps ++ map Right ops ]
               ++ [ Left p | p <- maybeToList mp ]
      pure (pages', b', ic')

    -- create a mapping of page numbers to raw pages, which we can use to do
    -- intra-page lookups on after first probing the bloom filter and index
    rawPages :: Map Int (Either RawPage RawOverflowPage)
    rawPages   = Map.fromList (zip [0..] pages)

{-------------------------------------------------------------------------------
  Labelling
-------------------------------------------------------------------------------}

-- | Binary classification of truth vs. a test result
classifyBin :: Bool -> Bool -> BinaryClassification
classifyBin truth test
    |     truth &&     test = TruePositive
    | not truth &&     test = FalsePositive
    |     truth && not test = FalseNegative
  --  not truth && not test =
    | otherwise             = TrueNegative

data BinaryClassification =
    TruePositive  | FalsePositive
  | FalseNegative | TrueNegative
  deriving stock Show

tabulateInMemLookupDataN ::
     forall prop. Testable prop
  => [InMemLookupData SerialisedKey SerialisedValue BlobSpan]
  -> (prop -> Property)
tabulateInMemLookupDataN dats = appEndo (foldMap Endo [
      let run = mkTestRun (runData dat)
      in  tabulateInMemLookupData dat run
    | dat <- dats
    ])
    . tabulate "Number of runs" [show $ length dats]

tabulateInMemLookupData ::
     forall prop. Testable prop
  => InMemLookupData SerialisedKey SerialisedValue BlobSpan
  -> TestRun
  -> (prop -> Property)
tabulateInMemLookupData dat run =
        tabulateKeySizes
      . tabulateValueSizes
      . tabulateNumKeyEntryPairs
      . tabulateNumPages
      . tabulateNumLookups
      . tabulateEntryType
  where
    InMemLookupData{runData, lookups} = dat
    tabulateKeySizes = tabulate "Size of key in run" [showPowersOf10 $ sizeofKey k | k <- Map.keys runData ]
    tabulateValueSizes = tabulate "Size of value in run" [showPowersOf10 $ onValue 0 sizeofValue e | e <- Map.elems runData]
    tabulateNumKeyEntryPairs = tabulate "Number of key-entry pairs" [showPowersOf10 (Map.size runData) ]
    tabulateNumPages = tabulate "Number of pages" [showPowersOf10 (Map.size ps) | let (ps,_,_) = run]
    tabulateNumLookups = tabulate "Number of lookups" [showPowersOf10 (length lookups)]
    tabulateEntryType = tabulate "Entry type" (map (takeWhile (/= ' ') . show) (Map.elems runData))

{-------------------------------------------------------------------------------
  Arbitrary
-------------------------------------------------------------------------------}

data InMemLookupData k v b = InMemLookupData {
    -- | Data for constructing a run
    runData :: Map k (Entry v b)
    -- | Keys to look up. Expected lookup results are obtained by querying
    -- runData.
  , lookups :: [k]
  }
  deriving stock (Show, Generic)
  deriving anyclass NFData

instance Arbitrary (InMemLookupData SerialisedKey SerialisedValue BlobSpan) where
  arbitrary = liftArbitrary3InMemLookupData genSerialisedKey genSerialisedValue genBlobSpan
  shrink = liftShrink3InMemLookupData shrinkSerialisedKey shrinkSerialisedValue shrinkBlobSpan

instance Arbitrary1 (InMemLookupData SerialisedKey SerialisedValue) where
  liftArbitrary = liftArbitrary3InMemLookupData genSerialisedKey genSerialisedValue

instance Arbitrary2 (InMemLookupData SerialisedKey) where
  liftArbitrary2 = liftArbitrary3InMemLookupData genSerialisedKey

liftArbitrary3InMemLookupData ::
     Ord k
  => Gen k
  -> Gen v
  -> Gen b
  -> Gen (InMemLookupData k v b)
liftArbitrary3InMemLookupData genKey genValue genBlob = do
    kops <- liftArbitrary2Map genKey (liftArbitrary genEntry)
              `suchThat` (\x -> Map.size (Map.filter isJust x) > 0)
    let runData = Map.mapMaybe id kops
    lookups <- (sublistOf (Map.keys kops) >>= shuffle)
    pure InMemLookupData{ runData, lookups }
  where
    genEntry = liftArbitrary2 genValue genBlob

liftShrink3InMemLookupData ::
     Ord k
  => (k -> [k])
  -> (v -> [v])
  -> (b -> [b])
  -> InMemLookupData k v b
  -> [InMemLookupData k v b]
liftShrink3InMemLookupData shrinkKey shrinkValue shrinkBlob InMemLookupData{ runData, lookups } =
         [ InMemLookupData runData' lookups
         | runData' <- liftShrink2Map shrinkKey shrinkEntry runData
         , Map.size runData' > 0 ]
      ++ [ InMemLookupData runData lookups'
         | lookups' <- liftShrink shrinkKey lookups ]
    where
      shrinkEntry = liftShrink2 shrinkValue shrinkBlob

genSerialisedKey :: Gen SerialisedKey
genSerialisedKey = frequency [
      (9, arbitrary `suchThat` (\k -> sizeofKey k >= 8))
    , (1, do x <- getSmall <$> arbitrary
             pure $ SerialisedKey (RB.pack [0,0,0,0,0,0,0, x]))
    ]

shrinkSerialisedKey :: SerialisedKey -> [SerialisedKey]
shrinkSerialisedKey k = [k' | k' <- shrink k, sizeofKey k' >= 8]

genSerialisedValue :: Gen SerialisedValue
genSerialisedValue = frequency [ (50, arbitrary), (1, genLongValue) ]
  where genLongValue = coerce ((<>) <$> genRawBytesSized 65536 <*> arbitrary)

-- Shrinking as lists can normally be quite slow, so if the value is larger than
-- a threshold, use less exhaustive shrinking.
shrinkSerialisedValue :: SerialisedValue -> [SerialisedValue]
shrinkSerialisedValue v
  | sizeofValue v > 64 = -- shrink towards fewer bytes
                          [ coerce RB.take n' v | n' <- shrinkIntegral n ]
                          -- shrink towards a value of all 0-bytes
                      ++ [ v' | let v' = coerce (VP.fromList $ replicate n 0), v' /= v ]
  | otherwise          = shrink v -- expensive, but thorough
  where n = sizeofValue v

genBlobSpan :: Gen BlobSpan
genBlobSpan = arbitrary

shrinkBlobSpan :: BlobSpan -> [BlobSpan]
shrinkBlobSpan = shrink

instance Arbitrary (InMemLookupData SerialisedKey SerialisedValue SerialisedBlob) where
  arbitrary = liftArbitrary3InMemLookupData genSerialisedKey genSerialisedValue genSerialisedBlob
  shrink = liftShrink3InMemLookupData shrinkSerialisedKey shrinkSerialisedValue shrinkSerialisedBlob

genSerialisedBlob :: Gen SerialisedBlob
genSerialisedBlob = arbitrary

shrinkSerialisedBlob :: SerialisedBlob -> [SerialisedBlob]
shrinkSerialisedBlob = shrink
