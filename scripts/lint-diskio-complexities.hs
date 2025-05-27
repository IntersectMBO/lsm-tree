#!/usr/bin/env cabal
{- cabal:
build-depends:
  , base           >=4.16 && <5
  , bytestring    ^>=0.11
  , cassava       ^>=0.5
  , containers    ^>=0.6 || ^>=0.7 || ^>=0.8
  , process       ^>=1.6
  , text          ^>=2.1
  , vector        ^>=0.12 || ^>=0.13
-}
{-# LANGUAGE InstanceSigs        #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
{-# LANGUAGE RecordWildCards     #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}

import           Control.Applicative (Alternative (..))
import qualified Data.ByteString.Lazy as BSL
import           Data.Csv
import           Data.List (zip4)
import           Data.Map (Map)
import qualified Data.Map as M
import           Data.Maybe (fromMaybe, isNothing)
import           Data.Set (Set)
import qualified Data.Set as S
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.Encoding as TE
import qualified Data.Text.IO as TIO
import           Data.Traversable (for)
import           Data.Vector (Vector)
import qualified Data.Vector as V
import           System.Process (readProcess)

type Function = Text
type MergePolicy = Text
type MergeSchedule = Text
type WorstCaseDiskIOComplexity = Text
type Condition = Text

data DiskIOComplexity = DiskIOComplexity
  { function                  :: Function
  , mergePolicy               :: Maybe MergePolicy
  , mergeSchedule             :: Maybe MergeSchedule
  , worstCaseDiskIOComplexity :: WorstCaseDiskIOComplexity
  , condition                 :: Maybe Condition
  }
  deriving (Eq, Show)

looseEq :: DiskIOComplexity -> DiskIOComplexity -> Bool
dioc1 `looseEq` dioc2 =
  and
    [ dioc1.function == dioc2.function
    , dioc1.mergePolicy == dioc2.mergePolicy
    , dioc1.mergeSchedule == dioc2.mergeSchedule
    , dioc1.worstCaseDiskIOComplexity == dioc2.worstCaseDiskIOComplexity
    , isNothing dioc1.condition || dioc1.condition == dioc2.condition
    ]

main :: IO ()
main = do
  -- Get the disk I/O complexities from the package description
  mapForPackageDescription <-
    buildDiskIOComplexityMap . decodeDiskIOComplexities
      <$> readProcess "./scripts/lint-diskio-complexities/dump-from-package-description.hs" [] ""

  -- Get the disk I/O complexities from Database.LSMTree
  mapForFullApi <-
    buildDiskIOComplexityMap . decodeDiskIOComplexities
      <$> readProcess "./scripts/lint-diskio-complexities/dump-from-source.sh" ["./src/Database/LSMTree.hs"] ""

  -- Get the disk I/O complexities from Database.LSMTree.Simple
  mapForSimpleApi <-
    buildDiskIOComplexityMap . decodeDiskIOComplexities
      <$> readProcess "./scripts/lint-diskio-complexities/dump-from-source.sh" ["./src/Database/LSMTree/Simple.hs"] ""

  -- Comparing Database.LSMTree.Simple to Database.LSMTree
  putStrLn "Comparing Database.LSMTree.Simple to Database.LSMTree:"
  diskIOComplexityComparisonSimpleToFull <-
    for (concat . M.elems $ mapForSimpleApi) $ \diskIOComplexity@DiskIOComplexity{..} -> do
      case M.lookup function mapForFullApi of
        Nothing ->
          pure [("Database.LSMTree.Simple", diskIOComplexity)]
        Just fullDiskIOComplexities
          | diskIOComplexity `elem` fullDiskIOComplexities -> pure []
          | otherwise -> pure (("Database.LSMTree.Simple", diskIOComplexity) : (("Database.LSMTree",) <$> fullDiskIOComplexities))
  TIO.putStrLn . prettyDiskIOComplexityTable . concat $ diskIOComplexityComparisonSimpleToFull

  -- Comparing lsm-tree.cabal to Database.LSMTree
  putStrLn "Comparing lsm-tree.cabal to Database.LSMTree:"
  diskIOComplexityComparisonPackageDescriptionToFull <-
    for (concat . M.elems $ mapForPackageDescription) $ \diskIOComplexity@DiskIOComplexity{..} -> do
      case M.lookup function mapForFullApi of
        Nothing ->
          pure [("lsm-tree.cabal", diskIOComplexity)]
        Just fullDiskIOComplexities
          | any (looseEq diskIOComplexity) fullDiskIOComplexities -> pure []
          | otherwise -> pure (("lsm-tree.cabal", diskIOComplexity) : (("Database.LSMTree",) <$> fullDiskIOComplexities))
  TIO.putStrLn . prettyDiskIOComplexityTable . concat $ diskIOComplexityComparisonPackageDescriptionToFull

--------------------------------------------------------------------------------
-- Helper functions
--------------------------------------------------------------------------------

-- | Typeset a tagged list of 'DiskIOComplexity' records as an aligned table.
prettyDiskIOComplexityTable :: [(Text, DiskIOComplexity)] -> Text
prettyDiskIOComplexityTable diskIOComplexities =
  T.unlines
    [ T.unwords
      [ tag `padUpTo` maxTagLen
      , function `padUpTo` maxFunctionLen
      , condition `padUpTo` maxConditionLen
      , worstCaseDiskIOComplexity `padUpTo` maxWorstCaseDiskIOComplexityLen
      ]
    | (tag, function, condition, worstCaseDiskIOComplexity) <-
        zip4 tags functions conditions worstCaseDiskIOComplexities
    ]
 where
  tags = fst <$> diskIOComplexities
  maxTagLen = maximum (T.length <$> tags)

  functions = ((.function) . snd) <$> diskIOComplexities
  maxFunctionLen = maximum (T.length <$> functions)

  conditions = (prettyCondition . snd) <$> diskIOComplexities
  maxConditionLen = maximum (T.length <$> conditions)

  worstCaseDiskIOComplexities = ((.worstCaseDiskIOComplexity) . snd) <$> diskIOComplexities
  maxWorstCaseDiskIOComplexityLen = maximum (T.length <$> worstCaseDiskIOComplexities)

  padUpTo :: Text -> Int -> Text
  padUpTo txt len = txt <> T.replicate (len - T.length txt) " "

  prettyCondition :: DiskIOComplexity -> Text
  prettyCondition DiskIOComplexity{..} =
    fromMaybe "*" (unionMaybeWith slash mergePolicy (unionMaybeWith slash mergeSchedule condition))
   where
    slash :: Text -> Text -> Text
    x `slash` y = x <> "/" <> y

    unionMaybeWith :: (a -> a -> a) -> Maybe a -> Maybe a -> Maybe a
    unionMaybeWith op (Just x) (Just y) = Just (x `op` y)
    unionMaybeWith _op (Just x) Nothing = Just x
    unionMaybeWith _op Nothing (Just y) = Just y
    unionMaybeWith _op Nothing Nothing  = Nothing

-- | Structure vector of 'DiskIOComplexity' records into lookup table by function name.
buildDiskIOComplexityMap :: Vector DiskIOComplexity -> Map Function [DiskIOComplexity]
buildDiskIOComplexityMap = M.unionsWith (<>) . fmap toSingletonMap . V.toList
 where
  toSingletonMap :: DiskIOComplexity -> Map Function [DiskIOComplexity]
  toSingletonMap diskIOComplexity = M.singleton diskIOComplexity.function [diskIOComplexity]

-- | Parse CSV file into vector of 'DiskIOComplexity' records.
decodeDiskIOComplexities :: String -> Vector DiskIOComplexity
decodeDiskIOComplexities =
  either error snd . decodeByName . BSL.fromStrict . TE.encodeUtf8 . T.pack

-- | CSV file header for 'DiskIOComplexity' records.
diskIOComplexityHeader :: Header
diskIOComplexityHeader =
  header ["Function", "Merge policy", "Merge schedule", "Worst-case disk I/O complexity", "Condition"]

normaliseWorstCaseDiskIOComplexity :: WorstCaseDiskIOComplexity -> WorstCaseDiskIOComplexity
normaliseWorstCaseDiskIOComplexity =
  T.replace "+ " "+" . T.replace " +" "+" . T.replace " \\" "\\" . T.replace "  " " " . T.replace "\\:" ""

-- | Parse CSV row into 'DiskIOComplexity' record.
instance FromNamedRecord DiskIOComplexity where
  parseNamedRecord :: NamedRecord -> Parser DiskIOComplexity
  parseNamedRecord m = do
    function <- m .: "Function"
    mergePolicy <- orNotApplicable (m .: "Merge policy")
    mergeSchedule <- orNotApplicable (m .: "Merge schedule")
    worstCaseDiskIOComplexity <- normaliseWorstCaseDiskIOComplexity <$> (m .: "Worst-case disk I/O complexity")
    condition <- orNotApplicable (m .: "Condition")
    pure DiskIOComplexity{..}
   where
    orNotApplicable :: Parser Text -> Parser (Maybe Text)
    orNotApplicable pText = (emptyTextToNothing <$> pText) <|> pure Nothing
     where
      emptyTextToNothing :: Text -> Maybe Text
      emptyTextToNothing txt =
        if T.null txt then Nothing else Just txt
