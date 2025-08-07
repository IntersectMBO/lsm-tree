module Main (main) where

import           Database.LSMTree.Demo (demo)
import           System.Environment (getArgs)
import           System.IO (BufferMode (..), hSetBuffering, stdout)

main :: IO ()
main = do
  args <- getArgs
  let isInteractive = args == ["Interactive"]
  if isInteractive
    then putStrLn "Running in Interactive mode"
    else putStrLn "Running in NonInteractive mode"
  hSetBuffering stdout NoBuffering
  demo isInteractive
