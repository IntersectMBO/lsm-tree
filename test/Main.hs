module Main (main) where

import           Test.Database.LSMTree (tests)
import qualified Test.Database.LSMTree.Model.Monoidal
import qualified Test.Database.LSMTree.Model.Normal
import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "lsm-tree"
    [ tests
    , Test.Database.LSMTree.Model.Normal.tests
    , Test.Database.LSMTree.Model.Monoidal.tests
    ]
