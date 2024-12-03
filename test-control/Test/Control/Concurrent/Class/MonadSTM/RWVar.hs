module Test.Control.Concurrent.Class.MonadSTM.RWVar (tests) where

import qualified Control.Concurrent.Class.MonadSTM.RWVar as RW
import           Control.Monad.Class.MonadAsync
import           Control.Monad.Class.MonadSay (MonadSay (say))
import           Control.Monad.Class.MonadTest (MonadTest (exploreRaces))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.IOSim
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Control.Concurrent.Class.MonadSTM.RWVar" [
      testProperty "prop_noRace" prop_noRace
    ]

data Action a = Read a | Incr a
  deriving stock (Show, Eq, Read)

instance Arbitrary a => Arbitrary (Action a) where
  arbitrary = oneof [
        Read <$> arbitrary
      , Incr <$> arbitrary
      ]
  shrink (Read x) = [Read x' | x' <- shrink x]
  shrink (Incr x) = [Incr x' | x' <- shrink x]

-- | Performing reads and increments on an @'RWVar' Int@ in parallel should not
-- lead to races. We let IOSimPOR check that there are no deadlocks. We also
-- look at the trace to see if the value inside the 'RWVar' increases
-- monotonically.
prop_noRace ::
     Action ()
  -> Action ()
  -> Action ()
  -> Property
prop_noRace a1 a2 a3 = exploreSimTrace modifyOpts prop $ \_ tr ->
    case traceResult False tr of
      Left e -> counterexample (show e) (property False)
      _      -> propSayMonotonic tr
  where
    modifyOpts = id

    prop :: IOSim s ()
    prop = do
      exploreRaces
      var <- RW.new (0 :: Int)
      let g = \case
            Read () ->
              RW.withReadAccess var $ \x -> do
                say (show (Read x))
            Incr () ->
              RW.withWriteAccess_ var $ \x -> do
                let x' = x + 1
                say (show (Incr x'))
                pure x'
      t1 <- async (g a1)
      t2 <- async (g a2)
      t3 <- async (g a3)
      async (cancel t1) >>= wait
      (_ :: Either AsyncCancelled ()) <- try (wait t1)
      (_ :: Either AsyncCancelled ()) <- try (wait t2)
      (_ :: Either AsyncCancelled ()) <- try (wait t3)
      pure ()

propSayMonotonic :: SimTrace () -> Property
propSayMonotonic simTrace = propResultsMonotonic (actionResults simTrace)

propResultsMonotonic :: [Action Int] -> Property
propResultsMonotonic as =
    counterexample
      ("Action results are not monotonic: " ++ show as)
      (resultsMonotonic as)

resultsMonotonic :: [Action Int] -> Bool
resultsMonotonic = go 0
  where
    go _ []               = True
    go prev (Read x : as) = prev == x     && go x as
    go prev (Incr x : as) = prev + 1 == x && go x as

actionResults :: SimTrace () -> [Action Int]
actionResults = map read . selectTraceEventsSay
