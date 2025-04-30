{-# OPTIONS_GHC -Wno-orphans #-}

module Main (main) where

import qualified System.FS.API as FS
import           System.FS.BlockIO.API
import           System.FS.BlockIO.Sim (simHasBlockIO)
import qualified System.FS.Sim.MockFS as MockFS
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck (testProperty)

import           Control.Concurrent.Class.MonadSTM.Strict

main :: IO ()
main = defaultMain tests

tests :: TestTree
tests = testGroup "blockio:test-sim" [
      testProperty "prop_tryLockFileTwice" prop_tryLockFileTwice
    ]

{-------------------------------------------------------------------------------
  File locks
-------------------------------------------------------------------------------}

instance Arbitrary LockMode where
  arbitrary = elements [SharedLock, ExclusiveLock]
  shrink SharedLock    = []
  shrink ExclusiveLock = []

-- TODO: belongs in base
deriving stock instance Show LockMode

prop_tryLockFileTwice :: LockMode -> LockMode -> Property
prop_tryLockFileTwice mode1 mode2 = ioProperty $ do
    fsvar <- newTMVarIO MockFS.empty
    (_hfs, hbio) <- simHasBlockIO fsvar
    let path = FS.mkFsPath ["lockfile"]

    let expected@(x1, y1) = case (mode1, mode2) of
          (ExclusiveLock, ExclusiveLock) -> (True, False)
          (ExclusiveLock, SharedLock   ) -> (True, False)
          (SharedLock   , ExclusiveLock) -> (True, False)
          (SharedLock   , SharedLock   ) -> (True, True)

    before <- atomically (readTMVar fsvar)
    x2 <- tryLockFile hbio path mode1
    after1 <- atomically (readTMVar fsvar)
    y2 <- tryLockFile hbio path mode2
    after2 <- atomically (readTMVar fsvar)

    let addLabel = tabulate "modes" [show (mode1, mode2)]

    let addCounterexample = counterexample
                              ( "Expecting: " <> showExpected expected <>
                                "\nbut got:   " <> showReal (x2, y2) )
                          . counterexample
                              ( "FS before: " ++ show before ++ "\n"
                            <>  "FS after1: " ++ show after1 ++ "\n"
                            <>  "FS after2: " ++ show after2)

    pure $ addCounterexample $ addLabel $
      cmpBoolMaybeConstructor x1 x2 .&&. cmpBoolMaybeConstructor y1 y2

cmpBoolMaybeConstructor :: Bool -> Maybe a -> Bool
cmpBoolMaybeConstructor True  (Just _) = True
cmpBoolMaybeConstructor False Nothing  = True
cmpBoolMaybeConstructor _     _        = False

showExpected :: (Bool, Bool) -> String
showExpected (x, y) =
    "("  <> showBoolAsMaybeConstructor x <>
    ", " <> showBoolAsMaybeConstructor y <>
    ")"

showBoolAsMaybeConstructor :: Bool -> String
showBoolAsMaybeConstructor b
  | b         = "Just _"
  | otherwise = "Nothing"

showReal :: (Maybe a, Maybe a) -> String
showReal (x, y) =
    "("  <> showMaybeConstructor x <>
    ", " <> showMaybeConstructor y <>
    ")"

showMaybeConstructor :: Maybe a -> String
showMaybeConstructor Nothing  = "Nothing"
showMaybeConstructor (Just _) = "Just _"
