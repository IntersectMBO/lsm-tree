module Main (main) where

import           Database.LSMTree.Demo (demo)
import           System.IO (BufferMode (..), hSetBuffering, stdout)

main :: IO ()
main = do
  hSetBuffering stdout NoBuffering
  demo
