{-# LANGUAGE TypeApplications #-}

{-|
    Benchmarks for the prefix index, compared with the compact and the
    ordinary index.

    There are two kinds of keys:

      * /Uniform/ keys are random UTxO keys, which share hardly any prefixes.

      * /Overlapping/ keys are of the form @account\/NNNNNNNN\/balance@, so
        that consecutive keys share long prefixes.
-}
module Bench.Database.LSMTree.Internal.Index.Prefix (benchmarks) where

import           Control.DeepSeq (deepseq)
import           Control.Monad.ST.Strict (ST, runST)
import           Criterion.Main (Benchmark, bench, bgroup, env, nf, whnf)
import           Data.ByteString.Lazy (LazyByteString)
import qualified Data.ByteString.Lazy as LazyByteString (fromChunks, toStrict)
import           Data.ByteString.Short (ShortByteString)
import qualified Data.ByteString.Short as ShortByteString (toShort)
import qualified Data.Foldable as Foldable (foldl')
import qualified Data.List as List (sort)
import qualified Data.List.NonEmpty as NonEmpty (head, last)
import           Data.Maybe (maybeToList)
import           Database.LSMTree.Extras (groupsOfN)
import           Database.LSMTree.Extras.Index (Append (AppendSinglePage),
                     appendToCompact, appendToOrdinary, appendToPrefix)
import           Database.LSMTree.Extras.Random (uniformWithReplacement,
                     uniformWithoutReplacement)
import           Database.LSMTree.Extras.UTxO (UTxOKey)
import           Database.LSMTree.Internal.Chunk (Chunk)
import qualified Database.LSMTree.Internal.Chunk as Chunk (toByteString)
import           Database.LSMTree.Internal.Entry (NumEntries (NumEntries))
import           Database.LSMTree.Internal.Index.Compact (IndexCompact)
import qualified Database.LSMTree.Internal.Index.Compact as Compact (search)
import qualified Database.LSMTree.Internal.Index.CompactAcc as CompactAcc (new,
                     unsafeEnd)
import           Database.LSMTree.Internal.Index.Ordinary (IndexOrdinary)
import qualified Database.LSMTree.Internal.Index.Ordinary as Ordinary (finalLBS,
                     fromSBS, headerLBS, search)
import qualified Database.LSMTree.Internal.Index.OrdinaryAcc as OrdinaryAcc
                     (new, unsafeEnd)
import           Database.LSMTree.Internal.Index.Prefix (IndexPrefix)
import qualified Database.LSMTree.Internal.Index.Prefix as Prefix (finalLBS,
                     fromSBS, headerLBS, search)
import qualified Database.LSMTree.Internal.Index.PrefixAcc as PrefixAcc
                     (newWithDefaults, unsafeEnd)
import           Database.LSMTree.Internal.Page (PageSpan)
import qualified Database.LSMTree.Internal.RawBytes as RB (pack)
import           Database.LSMTree.Internal.Serialise
                     (SerialisedKey (SerialisedKey), serialiseKey)
import           System.Random (mkStdGen, uniformR)

-- * Benchmarks

benchmarks :: Benchmark
benchmarks = bgroup "Bench.Database.LSMTree.Internal.Index.Prefix" $
             [
                 benchmarksForKeys "uniform" uniformKeySet,
                 benchmarksForKeys "overlapping" overlappingKeySet
             ]

-- | The number of pages of the indexes.
pageCount :: Int
pageCount = 10000

-- | The number of searches per benchmark.
searchCount :: Int
searchCount = 1000

{-|
    A set of keys: the appends that construct the indexes, and the keys to
    search for.
-}
data KeySet = KeySet [Append] [SerialisedKey]

benchmarksForKeys :: String -> KeySet -> Benchmark
benchmarksForKeys name (KeySet appends keys)
    = bgroup name $
      [
          bgroup "Search" $
          [
              env (pure (constructCompact appends, keys)) $ \ ~(index, ks) ->
              bench "Compact" $ whnf (searches Compact.search index) ks,

              env (pure (constructOrdinary appends, keys)) $ \ ~(index, ks) ->
              bench "Ordinary" $ whnf (searches Ordinary.search index) ks,

              env (pure (constructPrefix appends, keys)) $ \ ~(index, ks) ->
              bench "Prefix" $ whnf (searches Prefix.search index) ks
          ],
          env (pure appends) $ \ as ->
          bgroup "Incremental construction" $
          [
              bench "Compact"  $ whnf constructCompact  as,
              bench "Ordinary" $ whnf constructOrdinary as,
              bench "Prefix"   $ whnf constructPrefix   as
          ],
          bgroup "Deserialisation" $
          [
              env (pure (serialiseOrdinary appends)) $ \ serialised ->
              bench "Ordinary" $ nf Ordinary.fromSBS serialised,

              env (pure (serialisePrefix appends)) $ \ serialised ->
              bench "Prefix" $ nf Prefix.fromSBS serialised
          ]
      ]

-- * Keys

-- | Random UTxO keys, two per page.
uniformKeySet :: KeySet
uniformKeySet = KeySet appends keys where

    appends :: [Append]
    appends = map (\ page -> AppendSinglePage
                                 (serialiseKey (NonEmpty.head page))
                                 (serialiseKey (NonEmpty.last page))) $
              groupsOfN 2                                              $
              List.sort                                                $
              uniformWithoutReplacement @UTxOKey (mkStdGen 17) (2 * pageCount)

    keys :: [SerialisedKey]
    keys = serialiseKey <$>
           uniformWithReplacement @UTxOKey (mkStdGen 42) searchCount

{-|
    Keys of the form @account\/NNNNNNNN\/balance@ with even numbers, two per
    page. The keys to search for have random numbers, so that about half of
    them are present.
-}
overlappingKeySet :: KeySet
overlappingKeySet = KeySet appends keys where

    appends :: [Append]
    appends = [ AppendSinglePage (accountKey (4 * page))
                                 (accountKey (4 * page + 2))
              | page <- [0 .. pred pageCount] ]

    keys :: [SerialisedKey]
    keys = map accountKey                          $
           take searchCount                        $
           randomNumbers (mkStdGen 42)
        where

        randomNumbers gen = let

                                (number, gen') = uniformR (0, 4 * pageCount)
                                                          gen

                            in number : randomNumbers gen'

accountKey :: Int -> SerialisedKey
accountKey number = SerialisedKey                         $
                    RB.pack                               $
                    map (fromIntegral . fromEnum)         $
                    "account/" ++ padded ++ "/balance"
    where

    padded :: String
    padded = let digits = show number in
             replicate (8 - length digits) '0' ++ digits

-- * Utilities

-- | Searches for all keys and forces the results.
searches :: (SerialisedKey -> index -> PageSpan)
         -> index
         -> [SerialisedKey]
         -> ()
searches search index
    = Foldable.foldl' (\ acc key -> search key index `deepseq` acc) ()

-- | Constructs an index from appends with a given accumulator.
constructWith :: ST s acc
              -> (Append -> acc -> ST s [Chunk])
              -> (acc -> ST s (Maybe Chunk, index))
              -> [Append]
              -> ST s ([Chunk], index)
constructWith newAcc appendToAcc endAcc appends = do
    acc <- newAcc
    chunks <- concat <$> mapM (`appendToAcc` acc) appends
    (remnant, index) <- endAcc acc
    pure (chunks ++ maybeToList remnant, index)

constructCompact :: [Append] -> IndexCompact
constructCompact appends = runST $
    snd <$> constructWith (CompactAcc.new 100)
                          appendToCompact
                          CompactAcc.unsafeEnd
                          appends

constructOrdinary :: [Append] -> IndexOrdinary
constructOrdinary appends = runST $
    snd <$> constructWith (OrdinaryAcc.new 0x100 0x1000)
                          appendToOrdinary
                          OrdinaryAcc.unsafeEnd
                          appends

constructPrefix :: [Append] -> IndexPrefix
constructPrefix appends = runST $
    snd <$> constructWith PrefixAcc.newWithDefaults
                          appendToPrefix
                          PrefixAcc.unsafeEnd
                          appends

-- | Assembles a serialised index from its parts.
assemble :: LazyByteString -> [Chunk] -> LazyByteString -> ShortByteString
assemble header chunks final
    = ShortByteString.toShort                                    $
      LazyByteString.toStrict                                    $
      header                                                  <>
      LazyByteString.fromChunks (map Chunk.toByteString chunks) <>
      final

serialiseOrdinary :: [Append] -> ShortByteString
serialiseOrdinary appends = runST $ do
    (chunks, index) <- constructWith (OrdinaryAcc.new 0x100 0x1000)
                                     appendToOrdinary
                                     OrdinaryAcc.unsafeEnd
                                     appends
    pure $ assemble Ordinary.headerLBS
                    chunks
                    (Ordinary.finalLBS (NumEntries (2 * length appends)) index)

serialisePrefix :: [Append] -> ShortByteString
serialisePrefix appends = runST $ do
    (chunks, index) <- constructWith PrefixAcc.newWithDefaults
                                     appendToPrefix
                                     PrefixAcc.unsafeEnd
                                     appends
    pure $ assemble Prefix.headerLBS
                    chunks
                    (Prefix.finalLBS (NumEntries (2 * length appends)) index)
