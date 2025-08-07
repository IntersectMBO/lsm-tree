#!/usr/bin/env cabal
{- cabal:
build-depends:
    , base          >=4.16
    , bytestring    >=0.11
    , Cabal-syntax ^>=3.10  || ^>=3.12
    , cassava      ^>=0.5
    , pandoc       ^>=3.6.4
    , pandoc-types ^>=1.23.1
    , text          >=2.1
-}
{-# LANGUAGE FlexibleContexts  #-}
{-# LANGUAGE LambdaCase        #-}
{-# LANGUAGE OverloadedStrings #-}
{-# LANGUAGE RecordWildCards   #-}

module Main (main) where

import qualified Data.ByteString as BS
import qualified Data.ByteString.Lazy as BSL (putStr)
import           Data.Csv (Header, NamedRecord, ToField (..),
                     ToNamedRecord (..), encodeByName, header, namedRecord,
                     (.=))
import           Data.IORef (atomicModifyIORef, newIORef)
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import           Data.Traversable (for)
import           Debug.Trace (traceShow)
import           Distribution.PackageDescription.Parsec
                     (parseGenericPackageDescriptionMaybe)
import qualified Distribution.Types.GenericPackageDescription as GenericPackageDescription
import qualified Distribution.Types.PackageDescription as PackageDescription
import           Distribution.Utils.ShortText (fromShortText)
import           System.IO (hPutStrLn, stderr)
import           Text.Pandoc (runIOorExplode)
import           Text.Pandoc.Definition (Block (..), Inline (..), Row (..),
                     TableBody (..))
import           Text.Pandoc.Extensions (getDefaultExtensions)
import           Text.Pandoc.Options (ReaderOptions (..), WriterOptions (..),
                     def)
import           Text.Pandoc.Readers (readHaddock)
import           Text.Pandoc.Walk (Walkable (query))

tableEntryToFunction :: Text -> Text -> Text
tableEntryToFunction resource operation
    | (resource == "Table" || resource == "Cursor") && operation `notElem` ["New", "Close"] = toCamel (T.splitOn " " operation)
    | resource == "Snapshot" && operation == "Open" = "openTableFromSnapshot"
    | resource == "Snapshot" && operation == "List" = "listSnapshots" -- plural
    | otherwise = toCamel [operation, resource]

main :: IO ()
main = do
    let lsmTreeCabalFile = "./lsm-tree/lsm-tree.cabal"
    lsmTreeCabalContent <- BS.readFile lsmTreeCabalFile
    case parseGenericPackageDescriptionMaybe lsmTreeCabalContent of
        Nothing -> hPutStrLn stderr $ "error: Could not parse '" <> lsmTreeCabalFile <> "'"
        Just genericPackageDescription -> do
            let packageDescription = GenericPackageDescription.packageDescription genericPackageDescription
            let description = T.pack . fromShortText $ PackageDescription.description packageDescription
            doc <- runIOorExplode $ readHaddock def description
            -- Get the disk I/O complexity table
            let diskIOComplexityTable = query dumpDiskIOComplexityTable doc
            resourceRef <- newIORef ""
            operationsRef <- newIORef ""
            entries <-
                fmap concat . for diskIOComplexityTable $ \row -> do
                    let fullRow = replicate (5 - length row) "" <> row
                    let [newResource, newOperations, newMergePolicy, newMergeSchedule, rawWorstCaseDiskIOComplexity] = fullRow
                    resource <- atomicModifyIORef resourceRef (merge newResource)
                    operations <- atomicModifyIORef operationsRef (merge newOperations)
                    let mergePolicy = if newMergePolicy == "N/A" then Nothing else Just newMergePolicy
                    let mergeSchedule = if newMergeSchedule == "N/A" then Nothing else Just newMergeSchedule
                    let worstCaseDiskIOComplexity = T.dropWhileEnd (`elem`[' ','*']) rawWorstCaseDiskIOComplexity
                    for (T.splitOn "/" operations) $ \operation -> do
                        let function = tableEntryToFunction resource operation
                        pure $ DiskIOComplexity {..}
            let csvData = encodeByName diskIOComplexityHeader entries
            BSL.putStr csvData

--------------------------------------------------------------------------------
-- Helper functions
--------------------------------------------------------------------------------

type Function = Text
type MergePolicy = Text
type MergeSchedule = Text
type WorstCaseDiskIOComplexity = Text

data DiskIOComplexity = DiskIOComplexity
  { function                  :: Function
  , mergePolicy               :: Maybe MergePolicy
  , mergeSchedule             :: Maybe MergeSchedule
  , worstCaseDiskIOComplexity :: WorstCaseDiskIOComplexity
  }
  deriving (Eq, Show)

diskIOComplexityHeader :: Header
diskIOComplexityHeader =
  header
    ["Function", "Merge policy", "Merge schedule", "Worst-case disk I/O complexity"]

instance ToNamedRecord DiskIOComplexity where
  toNamedRecord DiskIOComplexity {..} =
    namedRecord
      [ "Function" .= toField function
      , "Merge policy" .= toField mergePolicy
      , "Merge schedule" .= toField mergeSchedule
      , "Worst-case disk I/O complexity" .= toField worstCaseDiskIOComplexity
      ]

contents :: Walkable Block a => a -> [Text]
contents = query forBlock
    where
        forBlock :: Block -> [Text]
        forBlock block = [T.unwords (query forInline block)]
        forInline :: Inline -> [Text]
        forInline (Code _ text) = [text]
        forInline (Math _ text) = [text]
        forInline (Str text)    = [text]
        forInline _             = []

diskIOComplexityTableHeader :: [Text]
diskIOComplexityTableHeader =
    ["Resource", "Operation", "Merge policy", "Merge schedule", "Worst-case disk I/O complexity"]

dumpDiskIOComplexityTable :: Block -> [[Text]]
dumpDiskIOComplexityTable table@(Table _attr _caption _cols tableHead tableBodies _tableFoot)
    | contents tableHead == diskIOComplexityTableHeader =
        [ [T.unwords (contents cell) | cell <- cells]
        | TableBody _ _ _ rows <- tableBodies
        , Row _ cells <- rows
        ]
dumpDiskIOComplexityTable _ = []

merge :: Text -> Text -> (Text, Text)
merge new old = let res = if T.null new then old else new in (res, res)

toCamel :: [Text] -> Text
toCamel []       = mempty
toCamel (x : xs) = T.toLower x <> T.concat (T.toTitle <$> xs)
