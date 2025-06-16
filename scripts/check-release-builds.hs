#!/usr/bin/env cabal
{- cabal:
build-depends:
  , base         >=4.16   && <4.22
  , directory    ^>=1.3
  , filepath     ^>=1.5
  , process      ^>=1.6

ghc-options:
    -Wall -Wcompat -Wincomplete-uni-patterns
    -Wincomplete-record-updates -Wpartial-fields -Widentities
    -Wredundant-constraints -Wmissing-export-lists
    -Wno-unticked-promoted-constructors -Wunused-packages

ghc-options: -Werror=missing-deriving-strategies
-}
{-# LANGUAGE DerivingStrategies #-}
{-# LANGUAGE GHC2021            #-}
{-# LANGUAGE LambdaCase         #-}

import           Control.Exception
import           Control.Monad
import           System.Directory
import           System.Environment
import           System.Exit
import           System.FilePath
import           System.Process
import           Text.Printf

main :: IO ()
main = do
    args <- getArgs
    case args of
      ghcVersions : cabalFiles
        | Just vs <- case ghcVersions of
            "Default" -> Just [Default]
            "All" -> Just allGhcVersions
            _ -> Nothing
        -> do
          printf "Checking release builds for %s with ghc versions %s...\n" (show cabalFiles) (show $ fmap ghcVersionExecutableName vs)

          forM_ vs findGhcExecutable

          withTempProjectFile cabalFiles $ \projectFile ->
            forM_ cabalFiles $ \cabalFile -> do
              let component = (dropExtension $ takeFileName cabalFile)
              forM_ vs $ buildComponentWith projectFile component

          printf "All release builds successful for GHC versions: %s" (unwords $ fmap ghcVersionExecutableName vs)
      _ -> do
        putStrLn "Usage: [Default|All] FILES"

-- TODO: I wanted to use the --ignore-project cabal option, which should be
-- a globally configurable value according to the cabal user guide, but for
-- some reason cabal thinks it's a valid option. So, I'm creating a
-- temporary project file instead.
tempProjectFilePath :: FilePath
tempProjectFilePath = "cabal.project.temp"

withTempProjectFile :: [FilePath] -> (FilePath -> IO ()) -> IO ()
withTempProjectFile cabalFiles k = do
    let contents = unwords ("packages:" : cabalFiles)
    printf "Creating temporary project file at %s with contents %s...\n" tempProjectFilePath contents
    bracket
      (do writeFile tempProjectFilePath contents
          pure tempProjectFilePath)
      (\_ -> removeFile tempProjectFilePath)
      k

data GhcVersion = Default | Ghc9_2 | Ghc9_4 | Ghc9_6 | Ghc9_8 | Ghc9_10 | Ghc9_12
  deriving stock (Enum, Bounded)

allGhcVersions :: [GhcVersion]
allGhcVersions = [Ghc9_2 .. maxBound]

ghcVersionExecutableName :: GhcVersion -> String
ghcVersionExecutableName = \case
    Default -> "ghc"
    Ghc9_2  -> "ghc-9.2"
    Ghc9_4  -> "ghc-9.4"
    Ghc9_6  -> "ghc-9.6"
    Ghc9_8  -> "ghc-9.8"
    Ghc9_10 -> "ghc-9.10"
    Ghc9_12 -> "ghc-9.12"

findGhcExecutable :: GhcVersion -> IO ()
findGhcExecutable v = do
    let exe = ghcVersionExecutableName v
    printf "Finding executable for %s...\n" exe
    ec <- readProcessWithExitCode' "which" [exe] ""
    unless (ec == ExitSuccess) $ do
      printf "Could not find executable for %s...\n" exe
      exitWith ec

buildComponentWith :: String -> FilePath -> GhcVersion -> IO ()
buildComponentWith projectFile component v = do
      let ghcExe = ghcVersionExecutableName v
      printf "Building %s with %s...\n" component ghcExe
      let args = [
              "--project-file="++projectFile
            , "--disable-tests"
            , "--disable-benchmarks"
            , "--index-state=HEAD"
            , "--with-compiler=" ++ ghcExe
            ]
      ec <- readProcessWithExitCode' "cabal" ("build" : component : args) ""
      unless (ec == ExitSuccess) $ do
        printf "Failure during building %s with %s...\n" component ghcExe
        exitWith ec

readProcessWithExitCode' :: FilePath -> [String] -> String -> IO ExitCode
readProcessWithExitCode' cmd args inp = do
    printf "Running '%s %s%s'...\n" cmd (unwords args) inp
    (ec, sStdout, sStderr) <- readProcessWithExitCode cmd args inp

    -- TODO: ideally stdout and stderr would be streamed to the terminal that
    -- the script was called from, but for now it only prints both when the
    -- build process has terminated.
    putStrLn "Printing stdout..."
    putStr sStdout
    putStrLn "Printing stderr..."
    putStr sStderr
    pure ec
