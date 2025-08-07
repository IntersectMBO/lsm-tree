#!/usr/bin/env cabal
{- cabal:
build-depends:
    , base          >=4.16
    , bytestring    >=0.11
    , Cabal-syntax ^>=3.10  || ^>=3.12
    , pandoc       ^>=3.6.4
    , text          >=2.1
-}
{-# LANGUAGE LambdaCase #-}

module Main (main) where

import qualified Data.ByteString as BS
import           Data.Text (Text)
import qualified Data.Text as T
import qualified Data.Text.IO as TIO
import           Distribution.PackageDescription.Parsec
                     (parseGenericPackageDescriptionMaybe)
import qualified Distribution.Types.GenericPackageDescription as GenericPackageDescription
import qualified Distribution.Types.PackageDescription as PackageDescription
import           Distribution.Utils.ShortText (fromShortText)
import           System.IO (hPutStrLn, stderr)
import           Text.Pandoc (runIOorExplode)
import           Text.Pandoc.Extensions (githubMarkdownExtensions)
import           Text.Pandoc.Options (ReaderOptions (..), WriterOptions (..),
                     def)
import           Text.Pandoc.Readers (readHaddock, readMarkdown)
import           Text.Pandoc.Transforms (headerShift)
import           Text.Pandoc.Writers (writeHaddock)

main :: IO ()
main = do
    putStrLn "Generating prologue.haddock from package description..."
    let readmeHeaderFile = "scripts/generate-readme-header.md"
    readmeHeaderContent <- TIO.readFile readmeHeaderFile
    let lsmTreeCabalFile = "./lsm-tree/lsm-tree.cabal"
    lsmTreeCabalContent <- BS.readFile lsmTreeCabalFile
    case parseGenericPackageDescriptionMaybe lsmTreeCabalContent of
        Nothing -> hPutStrLn stderr $ "error: Could not parse '" <> lsmTreeCabalFile <> "'"
        Just genericPackageDescription -> do
            let packageDescription = GenericPackageDescription.packageDescription genericPackageDescription
            let description = T.pack . fromShortText $ PackageDescription.description packageDescription
            header <-
                runIOorExplode $ do
                    doc <- readMarkdown def{readerExtensions = githubMarkdownExtensions} readmeHeaderContent
                    writeHaddock def doc
            let prologue = T.unlines [header, description]
            TIO.writeFile "prologue.haddock" prologue
