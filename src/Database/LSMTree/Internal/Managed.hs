-- | Subset of the @managed@ package, but this @Managed@ can run in any @m@, not
-- just @IO@.
module Database.LSMTree.Internal.Managed (
    Managed (..)
  , with
  ) where

newtype Managed m a = Managed { (>>-) :: forall r . (a -> m r) -> m r }

instance Functor (Managed m) where
    fmap f mx = Managed (\return_ ->
        mx >>- \x ->
        return_ (f x) )

instance Applicative (Managed m) where
    pure r    = Managed (\return_ ->
        return_ r )

    mf <*> mx = Managed (\return_ ->
        mf >>- \f ->
        mx >>- \x ->
        return_ (f x) )

instance Monad (Managed m) where
    ma >>= f = Managed (\return_ ->
        ma  >>- \a ->
        f a >>- \b ->
        return_ b )

with :: Managed m a -> (a -> m r) -> m r
with m = (>>-) m
