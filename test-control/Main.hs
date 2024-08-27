module Main (main) where

import qualified Test.Control.Concurrent.Class.MonadSTM.RWVar
import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "control"
    [ Test.Control.Concurrent.Class.MonadSTM.RWVar.tests
    ]
