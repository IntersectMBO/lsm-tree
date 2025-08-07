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
import           Control.Monad (unless)
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
import           System.Exit (ExitCode (..), exitWith)
import           System.Process (readProcess)

main :: IO ()
main = do
  -- Get the disk I/O complexities from the package description
  mapForPackageDescription <-
    buildDiskIOComplexityMap . decodeDiskIOComplexities
      <$> readProcess "./scripts/lint-diskio-complexities/dump-from-package-description.hs" [] ""

  -- Get the disk I/O complexities from Database.LSMTree
  mapForFullApi <-
    buildDiskIOComplexityMap . decodeDiskIOComplexities
      <$> readProcess "./scripts/lint-diskio-complexities/dump-from-source.sh" ["./lsm-tree/src/Database/LSMTree.hs"] ""

  -- Get the disk I/O complexities from Database.LSMTree.Simple
  mapForSimpleApi <-
    buildDiskIOComplexityMap . decodeDiskIOComplexities
      <$> readProcess "./scripts/lint-diskio-complexities/dump-from-source.sh" ["./lsm-tree/src/Database/LSMTree/Simple.hs"] ""

  -- Comparing Database.LSMTree.Simple to Database.LSMTree
  putStrLn "Comparing Database.LSMTree.Simple to Database.LSMTree:"
  comparisonSimpleToFull <-
    fmap concat . for (concat . M.elems $ mapForSimpleApi) $ \simpleEntry@DiskIOComplexity{..} -> do
      case M.lookup function mapForFullApi of
        Nothing ->
          pure [("Database.LSMTree.Simple", simpleEntry)]
        Just fullEntries
          | simpleEntry `elem` fullEntries -> pure []
          | otherwise -> pure (("Database.LSMTree.Simple", simpleEntry) : (("Database.LSMTree",) <$> fullEntries))
  TIO.putStrLn (prettyDiskIOComplexityTable comparisonSimpleToFull)

  -- Comparing lsm-tree.cabal to Database.LSMTree
  putStrLn "Comparing lsm-tree.cabal to Database.LSMTree:"
  comparisonPackageDescriptionToFull <-
    fmap concat . for (concat . M.elems $ mapForPackageDescription) $ \simpleEntry@DiskIOComplexity{..} -> do
      case M.lookup function mapForFullApi of
        Nothing ->
          pure [("lsm-tree.cabal", simpleEntry)]
        Just fullEntries
          | any (looseEq simpleEntry) fullEntries -> pure []
          | otherwise -> pure (("lsm-tree.cabal", simpleEntry) : (("Database.LSMTree",) <$> fullEntries))
  TIO.putStrLn (prettyDiskIOComplexityTable comparisonPackageDescriptionToFull)

  -- Set the exit code based on whether any differences were found
  unless (null comparisonSimpleToFull && null comparisonPackageDescriptionToFull) $
    exitWith (ExitFailure 1)

--------------------------------------------------------------------------------
-- Helper functions
--------------------------------------------------------------------------------

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

-- | Loose equality which is used when comparing the disk I/O complexities
--   listed in the package description to those in the modules. Those in the
--   package description do not list complex side conditions, such as all
--   tables having been closed beforehand, or all tables having the same merge
--   policy. Therefore, this equality disregards mismatches when the first
--   entry does not list a condition.
looseEq :: DiskIOComplexity -> DiskIOComplexity -> Bool
entry1 `looseEq` entry2 =
  and
    [ entry1.function == entry2.function
    , entry1.mergePolicy == entry2.mergePolicy
    , entry1.mergeSchedule == entry2.mergeSchedule
    , entry1.worstCaseDiskIOComplexity == entry2.worstCaseDiskIOComplexity
    , isNothing entry1.condition || entry1.condition == entry2.condition
    ]

-- | Typeset a tagged list of 'DiskIOComplexity' records as an aligned table.
prettyDiskIOComplexityTable :: [(Text, DiskIOComplexity)] -> Text
prettyDiskIOComplexityTable [] = "No differences found.\n"
prettyDiskIOComplexityTable entries =
  T.unlines
    [ T.unwords
      [ prettyCellForColumn tag tags
      , prettyCellForColumn function functions
      , prettyCellForColumn fullCondition fullConditions
      , prettyCellForColumn worstCaseDiskIOComplexity worstCaseDiskIOComplexities
      ]
    | (tag, function, fullCondition, worstCaseDiskIOComplexity) <-
        zip4 tags functions fullConditions worstCaseDiskIOComplexities
    ]
 where
  tags = fst <$> entries
  functions = ((.function) . snd) <$> entries
  fullConditions = (prettyFullCondition . snd) <$> entries
  worstCaseDiskIOComplexities = ((.worstCaseDiskIOComplexity) . snd) <$> entries

  prettyCellForColumn :: Text -> [Text] -> Text
  prettyCellForColumn cell column = cell <> T.replicate (maximum (T.length <$> column) - T.length cell) " "

  prettyFullCondition :: DiskIOComplexity -> Text
  prettyFullCondition DiskIOComplexity{..} =
    fromMaybe "*" mergePolicy `slashWith` mergeSchedule `slashWith` condition
   where
    slashWith :: Text -> Maybe Text -> Text
    slashWith x my = maybe x (\y -> x <> "/" <> y) my

-- | Structure vector of 'DiskIOComplexity' records into lookup table by function name.
buildDiskIOComplexityMap :: Vector DiskIOComplexity -> Map Function [DiskIOComplexity]
buildDiskIOComplexityMap = M.unionsWith (<>) . fmap toSingletonMap . V.toList
 where
  toSingletonMap :: DiskIOComplexity -> Map Function [DiskIOComplexity]
  toSingletonMap simpleEntry = M.singleton simpleEntry.function [simpleEntry]

-- | Parse CSV file into vector of 'DiskIOComplexity' records.
decodeDiskIOComplexities :: String -> Vector DiskIOComplexity
decodeDiskIOComplexities =
  either error snd . decodeByName . BSL.fromStrict . TE.encodeUtf8 . T.pack

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
