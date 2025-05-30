{-# OPTIONS_HADDOCK not-home #-}

module Database.LSMTree.Internal.Entry (
    Entry (..)
  , hasBlob
  , onValue
  , onBlobRef
  , NumEntries (..)
  , unNumEntries
    -- * Value resolution/merging
  , combine
  , combineUnion
  , combineMaybe
  ) where

import           Control.DeepSeq (NFData (..))
import           Data.Bifoldable (Bifoldable (..))
import           Data.Bifunctor (Bifunctor (..))

data Entry v b
    = Insert !v
    | InsertWithBlob !v !b
    | Upsert !v
    | Delete
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

hasBlob :: Entry v b -> Bool
hasBlob Insert{}         = False
hasBlob InsertWithBlob{} = True
hasBlob Upsert{}         = False
hasBlob Delete{}         = False

instance (NFData v, NFData b) => NFData (Entry v b) where
    rnf (Insert v)            = rnf v
    rnf (InsertWithBlob v br) = rnf v `seq` rnf br
    rnf (Upsert v)            = rnf v
    rnf Delete                = ()

onValue :: v' -> (v -> v') -> Entry v b -> v'
onValue def f = \case
    Insert v           -> f v
    InsertWithBlob v _ -> f v
    Upsert v          -> f v
    Delete             -> def

onBlobRef :: b' -> (b -> b') -> Entry v b -> b'
onBlobRef def g = \case
    Insert{}            -> def
    InsertWithBlob _ br -> g br
    Upsert{}           -> def
    Delete              -> def

instance Bifunctor Entry where
  first f = \case
      Insert v            -> Insert (f v)
      InsertWithBlob v br -> InsertWithBlob (f v) br
      Upsert v           -> Upsert (f v)
      Delete              -> Delete

  second g = \case
      Insert v            -> Insert v
      InsertWithBlob v br -> InsertWithBlob v (g br)
      Upsert v           -> Upsert v
      Delete              -> Delete

instance Bifoldable Entry where
  bifoldMap f g = \case
      Insert v            -> f v
      InsertWithBlob v br -> f v <> g br
      Upsert v           -> f v
      Delete              -> mempty

-- | A count of entries, for example the number of entries in a run.
--
-- This number is limited by the machine's word size. On 32-bit systems, the
-- maximum number we can represent is @2^31@ which is roughly 2 billion. This
-- should be a sufficiently large limit that we never reach it in practice. By
-- extension for 64-bit and higher-bit systems this limit is also sufficiently
-- large.
newtype NumEntries = NumEntries Int
  deriving stock (Eq, Ord, Show)
  deriving newtype NFData

instance Semigroup NumEntries where
  NumEntries a <> NumEntries b = NumEntries (a + b)

instance Monoid NumEntries where
  mempty = NumEntries 0

unNumEntries :: NumEntries -> Int
unNumEntries (NumEntries x) = x

{-------------------------------------------------------------------------------
  Value resolution/merging
-------------------------------------------------------------------------------}

-- | Given a value-merge function, combine entries. Only take a blob from the
-- left entry.
--
-- Note: 'Entry' is a semigroup with 'combine' if the @(v -> v -> v)@ argument
-- is associative.
combine :: (v -> v -> v) -> Entry v b -> Entry v b -> Entry v b
combine _ e@Delete            _                   = e
combine _ e@Insert {}         _                   = e
combine _ e@InsertWithBlob {} _                   = e
combine _   (Upsert u)       Delete               = Insert u
combine f   (Upsert u)       (Insert v)           = Insert (f u v)
combine f   (Upsert u)       (InsertWithBlob v _) = Insert (f u v)
combine f   (Upsert u)       (Upsert v)           = Upsert (f u v)

-- | Combine two entries of runs that have been 'union'ed together. If any one
-- has a value, the result should have a value (represented by 'Insert'). If
-- both have a value, these values get combined monoidally. Only take a blob
-- from the left entry.
--
-- Note: 'Entry' is a semigroup with 'combineUnion' if the @(v -> v -> v)@
-- argument is associative.
combineUnion :: (v -> v -> v) -> Entry v b -> Entry v b -> Entry v b
combineUnion f = go
  where
    go Delete               (Upsert v)           = Insert v
    go Delete               e                    = e
    go (Upsert u)          Delete                = Insert u
    go e                    Delete               = e
    go (Insert u)           (Insert v)           = Insert (f u v)
    go (Insert u)           (InsertWithBlob v _) = Insert (f u v)
    go (Insert u)           (Upsert v)           = Insert (f u v)
    go (InsertWithBlob u b) (Insert v)           = InsertWithBlob (f u v) b
    go (InsertWithBlob u b) (InsertWithBlob v _) = InsertWithBlob (f u v) b
    go (InsertWithBlob u b) (Upsert v)           = InsertWithBlob (f u v) b
    go (Upsert u)          (Insert v)            = Insert (f u v)
    go (Upsert u)          (InsertWithBlob v _)  = Insert (f u v)
    go (Upsert u)          (Upsert v)            = Insert (f u v)

combineMaybe :: (v -> v -> v) -> Maybe (Entry v b) -> Maybe (Entry v b) -> Maybe (Entry v b)
combineMaybe _ e1 Nothing          = e1
combineMaybe _ Nothing e2          = e2
combineMaybe f (Just e1) (Just e2) = Just $! combine f e1 e2
