module ScheduledMergesTest (tests) where

import           Data.Foldable (traverse_)
import           Data.STRef
import           Control.Exception
import           Control.Monad (replicateM_, when)
import           Control.Monad.ST
import           Control.Tracer (Tracer (Tracer))
import qualified Control.Tracer as Tracer

import           ScheduledMerges as LSM

import           Test.Tasty
import           Test.Tasty.HUnit (HasCallStack, testCase)

tests :: TestTree
tests = testGroup "Unit tests"
    [ testCase "regression_empty_run" test_regression_empty_run
    , testCase "merge_again_with_incoming" test_merge_again_with_incoming
    ]

-- | Results in an empty run on level 2.
test_regression_empty_run :: IO ()
test_regression_empty_run =
    runWithTracer $ \tracer -> do
      stToIO $ do
        lsm <- LSM.new
        let ins k = LSM.insert tracer lsm (K k) (V 0)
        let del k = LSM.delete tracer lsm (K k)
        -- run 1
        ins 0
        ins 1
        ins 2
        ins 3
        -- run 2
        ins 0
        ins 1
        ins 2
        ins 3
        -- run 3
        ins 0
        ins 1
        ins 2
        ins 3
        -- run 4, deletes all previous elements
        del 0
        del 1
        del 2
        del 3

        expectShape lsm
          [ ([], [4,4,4,4])
          ]

        -- run 5, results in last level merge of run 1-4
        ins 0
        ins 1
        ins 2
        ins 3

        expectShape lsm
          [ ([], [4])
          , ([4,4,4,4], [])
          ]

        -- finish merge
        LSM.supply lsm 16

        expectShape lsm
          [ ([], [4])
          , ([], [0])
          ]

-- | Covers the case where a run ends up too small for a level, so it gets
-- merged again with the next incoming runs.
-- That 5-way merge gets completed by supplying credits That merge gets
-- completed by supplying credits and then becomes part of another merge.
test_merge_again_with_incoming :: IO ()
test_merge_again_with_incoming =
    runWithTracer $ \tracer -> do
      stToIO $ do
        lsm <- LSM.new
        let ins k = LSM.insert tracer lsm (K k) (V 0)
        -- get something to 3rd level (so 2nd level is not levelling)
        -- (needs 5 runs to go to level 2 so the resulting run becomes too big)
        traverse_ ins [101..100+(5*16)]

        expectShape lsm  -- not yet arrived at level 3, but will soon
          [ ([], [4,4,4,4])
          , ([16,16,16,16], [])
          ]

        -- get a very small run (4 elements) to 2nd level
        replicateM_ 4 $
          traverse_ ins [201..200+4]

        expectShape lsm
          [ ([], [4,4,4,4])  -- these runs share the same keys
          , ([4,4,4,4,64], [])
          ]

        -- get another run to 2nd level, which the small run can be merged with
        traverse_ ins [301..300+16]

        expectShape lsm
          [ ([], [4,4,4,4])
          , ([4,4,4,4], [])
          , ([], [80])
          ]

        -- add just one more run so the 5-way merge on 2nd level gets created
        traverse_ ins [401..400+4]

        expectShape lsm
          [ ([], [4])
          , ([4,4,4,4,4], [])
          , ([], [80])
          ]

        -- complete the merge (20 entries, but credits get scaled up by 1.25)
        LSM.supply lsm 16

        expectShape lsm
          [ ([], [4])
          , ([], [20])
          , ([], [80])
          ]

        -- get 3 more runs to 2nd level, so the 5-way merge completes
        -- and becomes part of a new merge.
        -- (actually 4, as runs only move once a fifth run arrives...)
        traverse_ ins [501..500+(4*16)]

        expectShape lsm
          [ ([], [4])
          , ([4,4,4,4], [])
          , ([16,16,16,20,80], [])
          ]

-------------------------------------------------------------------------------
-- tracing and expectations on LSM shape
--

-- | Provides a tracer and will add the log of traced events to the reported
-- failure.
runWithTracer :: (Tracer (ST RealWorld) Event -> IO a) -> IO a
runWithTracer action = do
    events <- stToIO $ newSTRef []
    let tracer = Tracer $ Tracer.emit $ \e -> modifySTRef events (e :)
    action tracer `catch` \e -> do
      ev <- reverse <$> stToIO (readSTRef events)
      throwIO (Traced e ev)

data TracedException = Traced SomeException [Event]
  deriving stock (Show)

instance Exception TracedException where
  displayException (Traced e ev) =
    displayException e <> "\ntrace:\n" <> unlines (map show ev)

expectShape :: HasCallStack => LSM s -> [([Int], [Int])] -> ST s ()
expectShape lsm expected = do
    shape <- representationShape <$> dumpRepresentation lsm
    when (shape == expected) $
      error $ unlines
        [ "expected shape: " <> show expected
        , "actual shape:   " <> show shape
        ]
