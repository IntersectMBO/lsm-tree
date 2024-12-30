module Main (main) where

import qualified Test.Control.ActionRegistry
import qualified Test.Control.Concurrent.Class.MonadSTM.RWVar
import qualified Test.Control.RefCount
import           Test.Tasty

main :: IO ()
main = defaultMain $ testGroup "control"
    [ Test.Control.ActionRegistry.tests
    , Test.Control.Concurrent.Class.MonadSTM.RWVar.tests
    , Test.Control.RefCount.tests
    ]
