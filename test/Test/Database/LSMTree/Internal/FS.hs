{-# LANGUAGE OverloadedStrings #-}

{-# OPTIONS_GHC -Wno-orphans #-}

-- | Tests for the top-level @Internal@ module and its interaction with the file
-- system
module Test.Database.LSMTree.Internal.FS (tests) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Concurrent.Class.MonadSTM.Strict
                     (MonadSTM (atomically), readTMVar)
import           Control.Exception ()
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Primitive
import           Control.RefCount (checkForgottenRefs)
import           Control.Tracer
import           Data.Bifunctor (Bifunctor (..))
import           Data.Maybe (fromJust)
import           Data.Typeable
import qualified Data.Vector as V
import           Data.Word
import           Database.LSMTree.Extras.Generators ()
import           Database.LSMTree.Internal
import           Database.LSMTree.Internal.Config
import           Database.LSMTree.Internal.Entry
import           Database.LSMTree.Internal.Paths
import           Database.LSMTree.Internal.Serialise
import           Database.LSMTree.Internal.Snapshot
import           GHC.Show
import           GHC.Stack (HasCallStack)
import qualified System.FS.API as FS
import           System.FS.Sim.Error hiding (genErrors)
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck
import           Test.Util.FS

tests :: TestTree
tests = testGroup "Test.Database.LSMTree.Internal.FS" [
      testProperty "prop_fsRoundtripSnapshot" prop_fsRoundtripSnapshot
    ]

{-------------------------------------------------------------------------------
  Snapshot roundtrip with error injection
-------------------------------------------------------------------------------}

prop_fsRoundtripSnapshot ::
     TestErrors
  -> Positive (Small Int)
  -> V.Vector (Word64, Entry Word64 Word64)
  -> Property
prop_fsRoundtripSnapshot testErrs (Positive (Small bufferSize)) es =
    ioProperty $
    withCheckForgottenRefs $
    withSim $ \hfs hbio fsVar errsVar -> do
      ((res1, res2), closeExc) <-
        withSession nullTracer hfs hbio (FS.mkFsPath []) $ \s -> do
          withTableTry s conf $ \t -> do
            updates resolve es' t
            res1 <-
              withErrors errsVar (createSnapshotErrors testErrs) $
                try @_ @SomeException $ createSnapshot
                  resolve
                  snapName
                  snapLabel
                  SnapFullTable
                  t
            res2 <-
                withErrors errsVar (openSnapshotErrors testErrs) $
                  try @_ @SomeException $ openSnapshot
                    s
                    snapLabel
                    SnapFullTable
                    configNoOverride
                    snapName
                    resolve
            pure (res1, res2)

      fs <- atomically $ readTMVar fsVar

      pure $
        counterexample ("createSnapshot result: " <> show res1) $
        counterexample ("openSnapshot result: " <> show res2) $
        tabulate "Result" [showT (res1, res2)] $
        (case closeExc of
          Left e ->
            counterexample ("close exception: " <> displayException e) $ property False
          Right {} -> property True
            ) .&&.
        propNoOpenHandles fs
  where
    withSim = withSimErrorHasBlockIO (\_ -> True) emptyErrors
    withCheckForgottenRefs k = k >>= \x -> checkForgottenRefs >> pure x

    conf = defaultTableConfig {
        confWriteBufferAlloc = AllocNumEntries (NumEntries bufferSize)
      }
    es' = fmap (bimap serialiseKey (bimap serialiseValue serialiseBlob)) es

    resolve (SerialisedValue x) (SerialisedValue y) =
        SerialisedValue (x <> y)

    snapName = fromJust $ mkSnapshotName "snap"
    snapLabel = SnapshotLabel "label"


withTableTry ::
     (HasCallStack, MonadMask m, MonadSTM m, MonadMVar m, PrimMonad m)
  => Session m h
  -> TableConfig
  -> (Table m h -> m a)
  -> m (a, Either SomeException ())
withTableTry sesh conf k =
    generalBracket
      (new sesh conf)
      (\t _ -> try (close t))
      k
{-   where
    exc (ExitCaseSuccess _) = Nothing
    exc (ExitCaseException e) = Just e
    exc ExitCaseAbort = Nothing -}

--
-- Generators
--

data TestErrors = TestErrors {
    createSnapshotErrors :: Errors
  , openSnapshotErrors   :: Errors
  }
  deriving stock Show

instance Arbitrary TestErrors where
  arbitrary = TestErrors <$> arbitrary <*> arbitrary
  shrink TestErrors{createSnapshotErrors, openSnapshotErrors} =
      [ TestErrors createSnapshotErrors' openSnapshotErrors'
      | (createSnapshotErrors', openSnapshotErrors')
      <- shrink (createSnapshotErrors, openSnapshotErrors)
      ]

--
-- Tabulation
--

showT :: Tabulate a => a -> String
showT x = showsPrecT 0 x ""

class Tabulate a where
  showsPrecT :: Int -> a -> ShowS

instance (Tabulate a, Tabulate b) => Tabulate (a, b) where
  showsPrecT _ (x, y) =
        showString "( "
      . showsPrecT 0 x
      . showString ", "
      . showsPrecT 0 y
      . showString ")"

instance (Tabulate a, Tabulate b) => Tabulate (Either a b) where
  showsPrecT d (Left x) = showParen (d > appPrec) $
      showString "Left " . showsPrecT appPrec1 x
  showsPrecT d (Right y) = showParen (d > appPrec) $
      showString "Right " . showsPrecT appPrec1 y

instance Tabulate SomeException where
  showsPrecT d e = showsPrec d $ typeOf e

instance Tabulate () where
  showsPrecT _ () = showString "()"

instance Tabulate (Table m h) where
  showsPrecT = showsPrec

instance Show (Table m h) where
  showsPrec d t =
        showParen (d > app_prec)
      $ showString "Table "
      . showsPrec (app_prec + 1) (tableId t)
    where
      app_prec = 10


