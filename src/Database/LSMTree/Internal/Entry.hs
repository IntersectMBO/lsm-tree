module Database.LSMTree.Internal.Entry (
    Entry (..)
  , hasBlob
  , onValue
  , onBlobRef
  , NumEntries (..)
  , unNumEntries
    -- * Value resolution/merging
  , combine
  , combineMaybe
  ) where

import           Control.DeepSeq (NFData (..))
import           Data.Bifoldable (Bifoldable (..))
import           Data.Bifunctor (Bifunctor (..))

data Entry v b
    = Insert !v
    | InsertWithBlob !v !b
    | Mupdate !v
    | Delete
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

hasBlob :: Entry v b -> Bool
hasBlob Insert{}         = False
hasBlob InsertWithBlob{} = True
hasBlob Mupdate{}        = False
hasBlob Delete{}         = False

instance (NFData v, NFData b) => NFData (Entry v b) where
    rnf (Insert v)            = rnf v
    rnf (InsertWithBlob v br) = rnf v `seq` rnf br
    rnf (Mupdate v)           = rnf v
    rnf Delete                = ()

onValue :: v' -> (v -> v') -> Entry v b -> v'
onValue def f = \case
    Insert v           -> f v
    InsertWithBlob v _ -> f v
    Mupdate v          -> f v
    Delete             -> def

onBlobRef :: b' -> (b -> b') -> Entry v b -> b'
onBlobRef def g = \case
    Insert{}            -> def
    InsertWithBlob _ br -> g br
    Mupdate{}           -> def
    Delete              -> def

instance Bifunctor Entry where
  first f = \case
      Insert v            -> Insert (f v)
      InsertWithBlob v br -> InsertWithBlob (f v) br
      Mupdate v           -> Mupdate (f v)
      Delete              -> Delete

  second g = \case
      Insert v            -> Insert v
      InsertWithBlob v br -> InsertWithBlob v (g br)
      Mupdate v           -> Mupdate v
      Delete              -> Delete

instance Bifoldable Entry where
  bifoldMap f g = \case
      Insert v            -> f v
      InsertWithBlob v br -> f v <> g br
      Mupdate v           -> f v
      Delete              -> mempty

-- | TODO: we should change this to be a Word64, so that it is in line with the
-- disk format.
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

-- | As long as values are a semigroup, an Entry is too
instance Semigroup v => Semigroup (Entry v b) where
  e1 <> e2 = combine (<>) e1 e2

-- | Given a value-merge function, combine entries
combine :: (v -> v -> v) -> Entry v b -> Entry v b -> Entry v b
combine _ e@Delete            _                       = e
combine _ e@Insert {}         _                       = e
combine _ e@InsertWithBlob {} _                       = e
combine _   (Mupdate u)       Delete                  = Insert u
combine f   (Mupdate u)       (Insert v)              = Insert (f u v)
combine f   (Mupdate u)       (InsertWithBlob v blob) = InsertWithBlob (f u v) blob
combine f   (Mupdate u)       (Mupdate v)             = Mupdate (f u v)

combineMaybe :: (v -> v -> v) -> Maybe (Entry v b) -> Maybe (Entry v b) -> Maybe (Entry v b)
combineMaybe _ e1 Nothing          = e1
combineMaybe _ Nothing e2          = e2
combineMaybe f (Just e1) (Just e2) = Just $! combine f e1 e2
