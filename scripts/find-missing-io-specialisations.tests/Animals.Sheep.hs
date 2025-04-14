{-
    Pronunciation note:

    The identifiers in this module are somehow considered to be German. They
    used to contain the German ä and ö, but since the script only treats English
    letters as letters eligible to be part of identifiers, ä and ö were replaced
    by their standard alternatives ae and oe. This all should give some
    indication regarding how to pronounce the identifiers. The author of this
    module thought this note to be necessary, not least to justify the choice of
    module name. 😉
-}
module Animals.Sheep where

{-# SPECIALISE
    boerk
    ::
    Show a => a -> m ()
    #-}
boerk ::
     (Monad m, Show a) -- ^ The general way of constraining
  => a                 -- ^ A value
  -> m a               -- ^ An effectful computation
{-# SPECIALISE
    schnoerk
    ::
    Show a => m a
    #-}
schnoerk
  :: (Monad, m, Show a) -- ^ The general way of constraining
  => m a                -- ^ An effectful computation

{-# SPECIALISE
    bloek
    ::
    IO a
    #-}
bloek ::
     IO a

maeh :: a -> (b -> IO (a, b))
maeh = curry return

moeh :: Monad m => a -> (b -> m (a, b))
moeh = curry return
