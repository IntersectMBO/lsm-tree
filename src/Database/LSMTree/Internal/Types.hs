{-# OPTIONS_HADDOCK not-home #-}
{-# LANGUAGE DefaultSignatures #-}

module Database.LSMTree.Internal.Types (
    Salt,
    Session (..),
    Table (..),
    BlobRef (..),
    Cursor (..),
    ResolveValue (..),
    resolveCompatibility,
    resolveAssociativity,
    resolveValidOutput,
    ResolveViaSemigroup (..),
    ResolveAsFirst (..),
    ) where

import           Control.DeepSeq (NFData (..), deepseq)
import           Data.Kind (Type)
import           Data.Semigroup (Sum)
import           Data.Typeable
import           Data.Word (Word64)
import qualified Database.LSMTree.Internal.BlobRef as Unsafe
import           Database.LSMTree.Internal.RawBytes (RawBytes (..))
import           Database.LSMTree.Internal.Serialise.Class (SerialiseValue (..))
import qualified Database.LSMTree.Internal.Unsafe as Unsafe

{- |
The session salt is used to secure the hash operations in the Bloom filters.

The value of the salt must be kept secret.
Otherwise, there are no restrictions on the value.
-}
type Salt = Word64

{- |
A session stores context that is shared by multiple tables.

Each session is associated with one session directory where the files
containing table data are stored. Each session locks its session directory.
There can only be one active session for each session directory at a time.
If a database is must be accessed from multiple parts of a program,
one session should be opened and shared between those parts of the program.
Session directories cannot be shared between OS processes.

A session may contain multiple tables, which may each have a different configuration and different key, value, and BLOB types.
Furthermore, sessions may contain both [simple]("Database.LSMTree.Simple") and [full-featured]("Database.LSMTree") tables.
-}
type Session :: (Type -> Type) -> Type
data Session m
  = forall h.
    (Typeable h) =>
    Session !(Unsafe.Session m h)

instance NFData (Session m) where
  rnf :: Session m -> ()
  rnf (Session session) = rnf session

{- |
A table is a handle to an individual LSM-tree key\/value store with both in-memory and on-disk parts.

__Warning:__
Tables are ephemeral. Once you close a table, its data is lost forever.
To persist tables, use [snapshots]("Database.LSMTree#g:snapshots").
-}
type role Table nominal nominal nominal nominal

type Table :: (Type -> Type) -> Type -> Type -> Type -> Type
data Table m k v b
  = forall h.
    (Typeable h) =>
    Table !(Unsafe.Table m h)

instance NFData (Table m k v b) where
  rnf :: Table m k v b -> ()
  rnf (Table table) = rnf table

{- |
A blob reference is a reference to an on-disk blob.

__Warning:__ A blob reference is /not stable/. Any operation that modifies the table,
cursor, or session that corresponds to a blob reference may cause it to be invalidated.

The word \"blob\" in this type comes from the acronym Binary Large Object.
-}
type role BlobRef nominal nominal

type BlobRef :: (Type -> Type) -> Type -> Type
data BlobRef m b
  = forall h.
    (Typeable h) =>
    BlobRef !(Unsafe.WeakBlobRef m h)

instance Show (BlobRef m b) where
  showsPrec :: Int -> BlobRef m b -> ShowS
  showsPrec d (BlobRef b) = showsPrec d b

{- |
A cursor is a stable read-only iterator for a table.

A cursor iterates over the entries in a table following the order of the
serialised keys. After the cursor is created, updates to the referenced table
do not affect the cursor.

The name of this type references [database cursors](https://en.wikipedia.org/wiki/Cursor_(databases\)), not, e.g., text editor cursors.
-}
type role Cursor nominal nominal nominal nominal

type Cursor :: (Type -> Type) -> Type -> Type -> Type -> Type
data Cursor m k v b
  = forall h.
    (Typeable h) =>
    Cursor !(Unsafe.Cursor m h)

instance NFData (Cursor m k v b) where
  rnf :: Cursor m k v b -> ()
  rnf (Cursor cursor) = rnf cursor

--------------------------------------------------------------------------------
-- Monoidal value resolution
--------------------------------------------------------------------------------

{- |
An instance of @'ResolveValue' v@ specifies how to merge values when using
monoidal upsert.

The class has two functions.
The function 'resolve' acts on values, whereas the function 'resolveSerialised' acts on serialised values.
Each function has a default implementation in terms of the other and serialisation\/deserialisation.
The user is encouraged to implement 'resolveSerialised'.

Instances should satisfy the following:

[Compatibility]:
    The functions 'resolve' and 'resolveSerialised' should be compatible:

    prop> serialiseValue (resolve v1 v2) == resolveSerialised (Proxy @v) (serialiseValue v1) (serialiseValue v2)

[Associativity]:
    The function 'resolve' should be associative:

    prop> serialiseValue (v1 `resolve` (v2 `resolve` v3)) == serialiseValue ((v1 `resolve` v2) `resolve` v3)

[Valid Output]:
    The function 'resolveSerialised' should only return deserialisable values:

    prop> deserialiseValue (resolveSerialised (Proxy @v) rb1 rb2) `deepseq` True
-}
-- TODO(optimisation): Include a function that determines whether or not it is safe to remove and Update from the last level of an LSM-tree.
-- TODO(optimisation): Include a function @v -> RawBytes -> RawBytes@ that can be used to merged deserialised and serialised values.
--                     This can be used when inserting values into the write buffer.
class ResolveValue v where
  {-# MINIMAL resolve | resolveSerialised #-}

  {- |
  Combine two values.
  -}
  resolve :: v -> v -> v

  default resolve :: SerialiseValue v => v -> v -> v
  resolve v1 v2 =
    deserialiseValue $
      resolveSerialised (Proxy :: Proxy v) (serialiseValue v1) (serialiseValue v2)

  {- |
  Combine two serialised values.

  The user may assume that the input bytes are valid and can be deserialised using 'deserialiseValue'.
  The inputs are only ever produced by 'serialiseValue' and 'resolveSerialised'.
  -}
  resolveSerialised :: Proxy v -> RawBytes -> RawBytes -> RawBytes
  default resolveSerialised :: SerialiseValue v => Proxy v -> RawBytes -> RawBytes -> RawBytes
  resolveSerialised Proxy rb1 rb2 =
    serialiseValue (resolve (deserialiseValue @v rb1) (deserialiseValue @v rb2))

{- |
Test the __Compatibility__ law for the 'ResolveValue' class.
-}
resolveCompatibility :: (SerialiseValue v, ResolveValue v) => v -> v -> Bool
resolveCompatibility (v1 :: v) v2 =
  serialiseValue (resolve v1 v2) == resolveSerialised (Proxy @v) (serialiseValue v1) (serialiseValue v2)

{- |
Test the __Associativity__ law for the 'ResolveValue' class.
-}
resolveAssociativity :: (SerialiseValue v, ResolveValue v) => v -> v -> v -> Bool
resolveAssociativity v1 v2 v3 =
  serialiseValue (v1 `resolve` (v2 `resolve` v3)) == serialiseValue ((v1 `resolve` v2) `resolve` v3)

{- |
Test the __Valid Output__ law for the 'ResolveValue' class.
-}
resolveValidOutput :: (SerialiseValue v, ResolveValue v, NFData v) => v -> v -> Bool
resolveValidOutput (v1 :: v) (v2 :: v) =
  deserialiseValue @v (resolveSerialised (Proxy @v) (serialiseValue v1) (serialiseValue v2)) `deepseq` True

{- |
Wrapper that provides an instance of 'ResolveValue' via the 'Semigroup'
instance of the underlying type.

prop> resolve (ResolveViaSemigroup v1) (ResolveViaSemigroup v2) = ResolveViaSemigroup (v1 <> v2)
-}
newtype ResolveViaSemigroup v = ResolveViaSemigroup v
  deriving stock (Eq, Show)
  deriving newtype (SerialiseValue)

instance (SerialiseValue v, Semigroup v) => ResolveValue (ResolveViaSemigroup v) where
  resolve :: ResolveViaSemigroup v -> ResolveViaSemigroup v -> ResolveViaSemigroup v
  resolve (ResolveViaSemigroup v1) (ResolveViaSemigroup v2) = ResolveViaSemigroup (v1 <> v2)

{- |
Wrapper that provides an instance of 'ResolveValue' such that 'Database.LSMTree.upsert' behaves as 'Database.LSMTree.insert'.

The name 'ResolveAsFirst' is in reference to the wrapper 'Data.Semigroup.First' from "Data.Semigroup".

prop> resolve = const
-}
newtype ResolveAsFirst v = ResolveAsFirst {unResolveAsFirst:: v}
  deriving stock (Eq, Show)
  deriving newtype (SerialiseValue)

instance ResolveValue (ResolveAsFirst v) where
  resolve :: ResolveAsFirst v -> ResolveAsFirst v -> ResolveAsFirst v
  resolve = const
  resolveSerialised :: Proxy (ResolveAsFirst v) -> RawBytes -> RawBytes -> RawBytes
  resolveSerialised _p = const

deriving via (ResolveViaSemigroup (Sum v)) instance (Num v, SerialiseValue v) => ResolveValue (Sum v)
