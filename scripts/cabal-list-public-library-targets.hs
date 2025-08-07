#!/usr/bin/env cabal
{- cabal:
build-depends:
    , base          >=4.16
    , bytestring    >=0.11
    , Cabal-syntax ^>=3.10  || ^>=3.12

ghc-options:
  -Wall -Wcompat -Wincomplete-uni-patterns
  -Wincomplete-record-updates -Wpartial-fields -Widentities
  -Wredundant-constraints -Wmissing-export-lists
  -Wno-unticked-promoted-constructors -Wunused-packages

ghc-options: -Werror=missing-deriving-strategies

ghc-options: -Werror

ghc-options: -Wmissing-import-lists
-}
{-# LANGUAGE LambdaCase #-}

module Main (main) where

import           Control.Monad (forM_)
import qualified Data.ByteString as BS
import           Data.Maybe (mapMaybe)
import           Distribution.PackageDescription.Parsec
                     (parseGenericPackageDescriptionMaybe)
import           Distribution.Types.CondTree (ignoreConditions)
import qualified Distribution.Types.GenericPackageDescription as GenericPackageDescription
import           Distribution.Types.Library (libVisibility)
import           Distribution.Types.LibraryVisibility
                     (LibraryVisibility (LibraryVisibilityPrivate, LibraryVisibilityPublic))
import           Distribution.Types.PackageDescription (package)
import           Distribution.Types.PackageId (pkgName)
import           Distribution.Types.PackageName (unPackageName)
import           Distribution.Types.UnqualComponentName (unUnqualComponentName)
import           System.Environment (getArgs)
import           System.IO (hPutStrLn, stderr)

-- | Given a cabal file, list all public libraries in that cabal file (including
-- the main library) and print them as valid cabal targets.
--
-- For example:
--
-- @
-- ❯ ./scripts/cabal-list-public-library-targets.hs ./blockio/blockio.cabal
-- Finding sub-libraries with public visibility in './blockio/blockio.cabal' ...
-- blockio:blockio
-- blockio:sim
-- @
main :: IO ()
main = do
    cabalFiles <- getArgs
    forM_ cabalFiles $ \cabalFile -> do
      putStrLn ("Finding sub-libraries with public visibility in '" ++ cabalFile ++ "' ...")
      cabalContent <- BS.readFile cabalFile
      case parseGenericPackageDescriptionMaybe cabalContent of
          Nothing -> hPutStrLn stderr $ "error: Could not parse '" <> cabalFile <> "'"
          Just genericPackageDescription -> do
              let packageDescription = GenericPackageDescription.packageDescription genericPackageDescription
                  packageName = unPackageName $ pkgName $ package packageDescription
                  subLibraries = GenericPackageDescription.condSubLibraries genericPackageDescription
                  publicSubLibraries = mapMaybe isPublic subLibraries
                  mkCabalTarget s = packageName ++ ":" ++ s
              forM_ (packageName : publicSubLibraries) $ \n ->
                putStrLn $ mkCabalTarget n
  where
    isPublic (componentName, condTree) =
      let vis = libVisibility (fst $ ignoreConditions condTree) in
      case vis of
        LibraryVisibilityPublic  -> Just (unUnqualComponentName componentName)
        LibraryVisibilityPrivate -> Nothing
