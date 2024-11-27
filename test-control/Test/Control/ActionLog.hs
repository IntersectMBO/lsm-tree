{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE UndecidableInstances  #-}

module Test.Control.ActionLog (tests) where

import           Control.ActionLog
import           Control.Applicative
import           Control.Monad (void)
import           Control.Monad.Class.MonadAsync
import           Control.Monad.Class.MonadSay (MonadSay (..))
import           Control.Monad.Class.MonadTest (MonadTest (exploreRaces))
import           Control.Monad.Class.MonadThrow
import           Control.Monad.Class.MonadTimer.SI
import           Control.Monad.IOSim
import           Control.Monad.Primitive
import           Data.Maybe (fromJust)
import           Test.QuickCheck
import           Test.Tasty
import           Test.Tasty.QuickCheck

tests :: TestTree
tests = testGroup "Test.Control.ActionLog" [
      testProperty "prop_allocate" prop_allocate
    ]

prop_allocate ::
     Small Int
  -> Small Int
  -> Small Int
  -> Property
prop_allocate n1 n2 n3 = exploreSimTrace modifyOpts scenario $ \_ str ->
    case traceResult False str of
      Left e   -> counterexample ("Simulation failed: " <> show e) (property False)
      Right () -> propTrace str
  where
    modifyOpts = id

    scenario :: IOSim s ()
    scenario = do
        exploreRaces

        t1 <- async ( action1)
        t2 <- async (threadDelay (fromIntegral n2) >> action2 t1)

        _ <- try @_ @AsyncCancelled (wait t1)
        wait t2
      where
        action1 = do
          threadDelay (fromIntegral n1)
          withActionLogTraced (1 :: Int) $ \alog -> do
            void $
              allocate_Traced alog
                (pure (17 :: Int))
                (pure ())
            threadDelay (fromIntegral n3)

        action2 t1 = do
          cancel t1

    propTrace str =
        let es = collectEvents @Int str
        in  counterexample (show es) $
            case brackets es of
              Left e   -> counterexample (show e) False
              Right () -> property True

brackets :: (Eq a, Show a) => [Event a] -> Either String ()
brackets = \case
    [] -> Right ()
    (e@(ActionLogEvent (ActionLogStarted l)) : es) ->
      case eventually (finalised l) es of
        Nothing -> Left ("Not bracketed: " <> show e)
        Just (es', e') ->
          if isCommitted e' then
            Right ()
          else
            go es'
    _ -> Left ("Should start with starting an action log")
  where
    go [] = Right ()
    go (e@(AllocateEvent (Allocate l)) : es) =
      case eventually (released l) es of
        Nothing       -> Left ("Not bracketed: " <> show e)
        Just (es', _) -> go es'
    go _ = Left ("No mutual recursion")

    finalised l (ActionLogEvent e) =
      case e of
        ActionLogCommitted l' -> l == l'
        ActionLogAborted l'   -> l == l'
        _                     -> False
    finalised _ _ = False

    isCommitted (ActionLogEvent ActionLogCommitted{}) = True
    isCommitted _                                     = False

    released l (AllocateEvent (Release l')) = l == l'
    released _ _                            = False


eventually ::  (a -> Bool) -> [a] -> Maybe ([a], a)
eventually p xs
  | (ys, [z]) <- break p xs
  = Just (ys, z)
  | otherwise
  = Nothing

{-------------------------------------------------------------------------------
  ActionLog instrumented with tracing
-------------------------------------------------------------------------------}

class MonadTrace m where
  trace :: Show a => a -> m ()

instance MonadTrace (IOSim s) where
  {-# INLINE trace #-}
  trace x = say (show x)

withActionLogTraced ::
     (PrimMonad m, MonadCatch m, MonadTrace m, Show l)
  => l
  -> (ActionLog m -> m a)
  -> m (ExitCase a)
withActionLogTraced l k = snd <$> generalBracket acquire release k
  where
    acquire = do
      trace (ActionLogStarted l)
      unsafeNewActionLog
    release reg ec = do
      unsafeFinaliseActionLog reg ec
      trace (exitCaseEvent ec)
      pure ec

    exitCaseEvent ExitCaseSuccess{}   = ActionLogCommitted l
    exitCaseEvent ExitCaseAbort{}     = ActionLogAborted l
    exitCaseEvent ExitCaseException{} = ActionLogAborted l

data ActionLogEvent a =
    ActionLogStarted a
  | ActionLogCommitted a
  | ActionLogAborted a
  deriving stock (Show, Eq, Read)

allocate_Traced ::
     (PrimMonad m, MonadMask m, MonadTrace m, Show a)
  => ActionLog m
  -> m a
  -> m ()
  -> m a
allocate_Traced alog acquire release =
    allocateFun alog Just acquireTraced releaseTraced
  where
    acquireTraced = do
      x <- acquire
      trace (Allocate x)
      pure x
    releaseTraced x = do
      release
      trace (Release x)

data AllocateEvent a =
    Allocate a
  | Release a
  deriving stock (Show, Eq, Read)

data Event a =
    ActionLogEvent (ActionLogEvent a)
  | AllocateEvent (AllocateEvent a)
  deriving stock (Show, Eq, Read)

collectEvents :: Read a => SimTrace () -> [Event a]
collectEvents str =
    [ fromJust (parseEvent s)
    | s <- selectTraceEventsSay str
    ]
  where
    parseEvent s =
          (ActionLogEvent <$> safeRead s)
      <|> (AllocateEvent <$> safeRead s)

safeRead :: Read a => String -> Maybe a
safeRead s
  | [(x, "")] <- reads s = Just x
  | otherwise = Nothing
