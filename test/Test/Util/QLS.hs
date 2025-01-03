{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilies #-}

-- | Utilities for the @quickcheck-lockstep@ package
--
-- TODO: it might be nice to upstream these utilities to @quickcheck-lockstep@
-- at some point.
module Test.Util.QLS (
    runActionsBracket
    -- * Taggin sequences
  , tagAndRender
  , LockstepTagSequence (..)
  , tagFinalState
  , renderTabulated
  ) where

import           Prelude hiding (init)

import           Control.Exception
import           Control.Monad (void)
import           Data.Typeable
import qualified Test.QuickCheck as QC
import Data.Kind
import           Test.QuickCheck (Property, Testable)
import           Test.QuickCheck.Monadic
import qualified Test.QuickCheck.StateModel as StateModel
import           Test.QuickCheck.StateModel hiding (runActions)
import           Test.QuickCheck.StateModel.Lockstep (Lockstep, RunLockstep)

runActionsBracket ::
     ( RunLockstep state m
     , e ~ Error (Lockstep state)
     , forall a. IsPerformResult e a
     )
  => Proxy state
  -> IO st -- ^ Initialisation
  -> (st -> IO Property) -- ^ Cleanup
  -> (m Property -> st -> IO Property) -- ^ Runner
  -> Actions (Lockstep state)
  -> Property
runActionsBracket _ init cleanup runner actions =
    monadicBracketIO init cleanup runner $
      void $ StateModel.runActions actions

ioPropertyBracket ::
     (Testable a, Testable b)
  => IO st
  -> (st -> IO b)
  -> (m a -> st -> IO a)
  -> m a
  -> Property
ioPropertyBracket init cleanup runner prop =
    QC.ioProperty $ mask $ \restore -> do
      st <- init
      a <- restore (runner prop st) `onException` cleanup st
      b <- cleanup st
      return $ a QC..&&. b

monadicBracketIO :: forall st a m.
     (Monad m, Testable a)
  => IO st
  -> (st -> IO Property)
  -> (m Property -> st -> IO Property)
  -> PropertyM m a
  -> Property
monadicBracketIO init cleanup runner =
    monadic (ioPropertyBracket init cleanup runner)


{-------------------------------------------------------------------------------
  Tagging sequences
-------------------------------------------------------------------------------}

-- TODO: does not have to be lockstep-specific

tagAndRender ::
     forall state. LockstepTagSequence state
  => Actions (Lockstep state)
  -> Property
  -> Property
tagAndRender actions = renderSequence (tagSequence actions)

class LockstepTagSequence state where
  -- | A tag for a sequence of actions
  data SeqTag state :: Type

  tagSequence :: Actions (Lockstep state) -> [SeqTag state]

  renderSequence :: [SeqTag state] -> (Property -> Property)

-- TODO: document helper
tagFinalState ::
     forall state. StateModel (Lockstep state)
  => Actions (Lockstep state)
  -> (Lockstep state -> [SeqTag state])
  -> [SeqTag state]
tagFinalState actions getTags = getTags $ underlyingState finalAnnState
  where
    finalAnnState :: Annotated (Lockstep state)
    finalAnnState = stateAfter @(Lockstep state) actions

-- TODO: document helper
renderTabulated ::
     (SeqTag state -> String)
  -> (SeqTag state -> [String])
  -> [SeqTag state]
  -> Property
  -> Property
renderTabulated fkey fvalue tags prop =
    foldr (\tag -> QC.tabulate (fkey tag) (fvalue tag)) prop tags