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
import           Text.Pandoc.Readers (readHaddock)
import           Text.Pandoc.Transforms (headerShift)
import           Text.Pandoc.Writers (writeMarkdown)

main :: IO ()
main = do
    putStrLn "Generating README.md from package description..."
    let readmeHeaderFile = "scripts/generate-readme-header.md"
    readmeHeaderContent <- TIO.readFile readmeHeaderFile
    let lsmTreeCabalFile = "lsm-tree.cabal"
    lsmTreeCabalContent <- BS.readFile lsmTreeCabalFile
    case parseGenericPackageDescriptionMaybe lsmTreeCabalContent of
        Nothing -> hPutStrLn stderr $ "error: Could not parse '" <> lsmTreeCabalFile <> "'"
        Just genericPackageDescription -> do
            let packageDescription = GenericPackageDescription.packageDescription genericPackageDescription
            let description = T.pack . fromShortText $ PackageDescription.description packageDescription
            body <-
                runIOorExplode $ do
                    doc1 <- readHaddock def description
                    let doc2 = headerShift 1 doc1
                    writeMarkdown def{writerExtensions = githubMarkdownExtensions} doc2
            let readme = T.unlines [readmeHeaderContent, body]
            TIO.writeFile "README.md" readme
