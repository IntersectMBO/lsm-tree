module Test.Control.Concurrent.Class.MonadSTM.RWVar (tests) where

import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.Monad.Class.MonadAsync
import           Control.Monad.Class.MonadSay (MonadSay (say))
import           Control.Monad.Class.MonadTimer
import           Control.Monad.IOSim
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Control.Concurrent.Class.MonadSTM.RWVar" [
      testProperty "temp" prop_noRace
    ]

data Action a = Read a | Incr a
  deriving stock (Show, Eq, Read)

instance Arbitrary (Action (Small Word, Small Word)) where
  arbitrary = do
    n <- oneof [ arbitrary, Small <$> choose (1, 5) ]
    m <- oneof [ arbitrary, Small <$> choose (1, 5) ]
    elements [ Read (n, m), Incr (n, m) ]

-- | Performing reads and increments on an @'RWVar' Int@ in parallel should not
-- lead to races. We observe this by looking at the trace and seeing that the
-- value inside the 'RWVar' increases monotonically.
prop_noRace :: [Action (Small Word, Small Word)] -> Property
prop_noRace as = exploreSimTrace id prop (\_ st -> sayMonotonic 0 st)
  where
    prop :: IOSim s ()
    prop = do
      var <- RW.new (0 :: Int)
      forConcurrently_ as $ \case
        Read (Small n, Small m) -> do
          threadDelay (fromIntegral n)
          RW.withReadAccess var $ \x -> do
            threadDelay (fromIntegral m)
            say (show (Read x))
        Incr (Small n, Small m) -> do
          threadDelay (fromIntegral n)
          RW.withWriteAccess_ var $ \x -> do
            threadDelay (fromIntegral m)
            let x' = x + 1
            say (show (Incr x'))
            pure x'

    sayMonotonic ::  Int -> SimTrace () -> Property
    sayMonotonic _ (Nil MainReturn{}) = property True
    sayMonotonic prev (Cons se st') = (case se of
        SimPOREvent{seType} -> case seType of
          EventSay s -> case read s of
            Read x -> counterexample "Unexpected Read result"
                    $ prev     === x .&&. sayMonotonic x st'
            Incr x -> counterexample "Unexpected Write result"
                    $ prev + 1 === x .&&. sayMonotonic x st'
          _ -> sayMonotonic prev st'
        _ -> property False)
    sayMonotonic _ _ = property False
