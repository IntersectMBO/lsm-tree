{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilies          #-}

-- | Utilities for the @quickcheck-lockstep@ package
--
-- TODO: it might be nice to upstream these utilities to @quickcheck-lockstep@
-- at some point.
module Test.Util.QLS (
    IOProperty (..)
  , runActionsBracket
  ) where

import           Prelude hiding (init)

import           Control.Monad (void)
import           Control.Monad.Class.MonadST
import           Control.Monad.Class.MonadThrow
import           Control.Monad.IOSim (IOSim, runSimTraceST, traceResult)
import           Control.Monad.Primitive (RealWorld)
import qualified Control.Monad.ST.Lazy as ST
import           Data.Typeable
import qualified Test.QuickCheck as QC
import           Test.QuickCheck (Property, Testable)
import           Test.QuickCheck.Monadic
import qualified Test.QuickCheck.StateModel as StateModel
import           Test.QuickCheck.StateModel hiding (runActions)
import           Test.QuickCheck.StateModel.Lockstep

class IOProperty m where
  ioProperty :: m Property -> Property

instance IOProperty IO where
  ioProperty = QC.ioProperty

instance IOProperty (IOSim RealWorld) where
  ioProperty x = QC.ioProperty $ do
      trac <- ST.stToIO $ runSimTraceST x
      case traceResult False trac of
        Left e  -> pure $ QC.counterexample (show e) False
        Right p -> pure p

runActionsBracket ::
     ( RunLockstep state m
     , e ~ Error (Lockstep state) m
     , forall a. IsPerformResult e a
     , Testable prop
     , MonadMask n
     , MonadST n
     , IOProperty n
     )
  => Proxy state
  -> n st -- ^ Initialisation
  -> (st -> n prop) -- ^ Cleanup
  -> (m Property -> st -> n Property) -- ^ Runner
  -> Actions (Lockstep state)
  -> Property
runActionsBracket _ init cleanup runner actions =
    monadicBracketIO init cleanup runner $
      void $ StateModel.runActions actions

ioPropertyBracket ::
     (Testable a, Testable b, MonadMask n, MonadST n, IOProperty n)
  => n st
  -> (st -> n b)
  -> (m a -> st -> n a)
  -> m a
  -> Property
ioPropertyBracket init cleanup runner prop =
    ioProperty $ mask $ \restore -> do
      st <- init
      a <- restore (runner prop st) `onException` cleanup st
      b <- cleanup st
      pure $ a QC..&&. b

monadicBracketIO :: forall st a b m n.
     (Monad m, Testable a, Testable b, MonadMask n, MonadST n, IOProperty n)
  => n st
  -> (st -> n b)
  -> (m Property -> st -> n Property)
  -> PropertyM m a
  -> Property
monadicBracketIO init cleanup runner =
    monadic (ioPropertyBracket init cleanup runner)
