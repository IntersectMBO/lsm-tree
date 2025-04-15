{-# LANGUAGE MagicHash #-}

-- | This module is experimental. It is mainly used for testing purposes.
--
-- See the 'Normal' and 'Monoidal' modules for documentation.
--
-- TODO: we have to decide whether we want to duplicate haddocks across public API
-- modules, or if we want to pick a single source of reference for haddocks.
-- Until then, the documentation on definitions in this module is omitted.
module Database.LSMTree (
    -- * Exceptions
    Common.SessionDirDoesNotExistError (..)
  , Common.SessionDirLockedError (..)
  , Common.SessionDirCorruptedError (..)
  , Common.SessionClosedError (..)
  , Common.TableClosedError (..)
  , Common.TableCorruptedError (..)
  , Common.TableTooLargeError (..)
  , Common.TableUnionNotCompatibleError (..)
  , Common.SnapshotExistsError (..)
  , Common.SnapshotDoesNotExistError (..)
  , Common.SnapshotCorruptedError (..)
  , Common.SnapshotNotCompatibleError (..)
  , Common.BlobRefInvalidError (..)
  , Common.CursorClosedError (..)
  , Common.FileFormat (..)
  , Common.FileCorruptedError (..)
  , Common.InvalidSnapshotNameError (..)

    -- * Tracing
  , Common.LSMTreeTrace (..)
  , Common.TableTrace (..)
  , Common.MergeTrace (..)

    -- * Table sessions
  , Session
  , withSession
  , openSession
  , closeSession

    -- * Table
  , Table
  , Common.TableConfig (..)
  , Common.defaultTableConfig
  , Common.SizeRatio (..)
  , Common.MergePolicy (..)
  , Common.WriteBufferAlloc (..)
  , Common.NumEntries (..)
  , Common.BloomFilterAlloc (..)
  , Common.defaultBloomFilterAlloc
  , Common.FencePointerIndexType (..)
  , Common.DiskCachePolicy (..)
  , Common.MergeSchedule (..)
  , Common.defaultMergeSchedule
  , withTable
  , new
  , close

    -- * Table queries and updates
    -- ** Queries
  , lookups
  , LookupResult (..)
  , rangeLookup
  , Range (..)
  , QueryResult (..)
    -- ** Cursor
  , Cursor
  , withCursor
  , withCursorAtOffset
  , newCursor
  , newCursorAtOffset
  , closeCursor
  , readCursor
    -- ** Updates
  , inserts
  , deletes
  , mupserts
  , updates
  , Update (..)
    -- ** Blobs
  , BlobRef
  , retrieveBlobs

    -- * Durability (snapshots)
  , Common.SnapshotName
  , Common.isValidSnapshotName
  , Common.toSnapshotName
  , Common.SnapshotLabel (..)
  , createSnapshot
  , openSnapshot
  , Common.OverrideDiskCachePolicy (..)
  , deleteSnapshot
  , listSnapshots

    -- * Persistence
  , duplicate

    -- * Table union
  , union
  , unions
  , UnionDebt (..)
  , remainingUnionDebt
  , UnionCredits (..)
  , supplyUnionCredits

    -- * Serialisation
  , SerialiseKey
  , SerialiseValue

    -- * Monoidal value resolution
  , ResolveValue (..)
  , resolveDeserialised
    -- ** Properties
  , resolveValueValidOutput
  , resolveValueAssociativity
    -- ** DerivingVia wrappers
  , ResolveAsFirst (..)

    -- * Utility types
  , IOLike
  ) where

import           Control.DeepSeq
import           Control.Monad
import           Control.Monad.Class.MonadThrow
import           Data.Bifunctor (Bifunctor (..))
import           Data.Coerce (coerce)
import           Data.Kind (Type)
import           Data.List.NonEmpty (NonEmpty (..))
import           Data.Monoid (Sum (..))
import           Data.Typeable (Proxy (..), Typeable, eqT, type (:~:) (Refl),
                     typeRep)
import qualified Data.Vector as V
import           Database.LSMTree.Common (BlobRef (BlobRef), IOLike, Range (..),
                     SerialiseKey, SerialiseValue, Session, UnionCredits (..),
                     UnionDebt (..), closeSession, deleteSnapshot,
                     deserialiseValue, listSnapshots, openSession,
                     serialiseValue, withSession)
import qualified Database.LSMTree.Common as Common
import qualified Database.LSMTree.Internal as Internal
import qualified Database.LSMTree.Internal.BlobRef as Internal
import qualified Database.LSMTree.Internal.Entry as Entry
import qualified Database.LSMTree.Internal.RawBytes as RB
import qualified Database.LSMTree.Internal.Serialise as Internal
import qualified Database.LSMTree.Internal.Snapshot as Internal
import qualified Database.LSMTree.Internal.Vector as V
import           GHC.Exts (Proxy#, proxy#)

{-------------------------------------------------------------------------------
  Tables
-------------------------------------------------------------------------------}

type Table = Internal.Table'

{-# SPECIALISE withTable ::
     Session IO
  -> Common.TableConfig
  -> (Table IO k v b -> IO a)
  -> IO a #-}
withTable ::
     IOLike m
  => Session m
  -> Common.TableConfig
  -> (Table m k v b -> m a)
  -> m a
withTable (Internal.Session' sesh) conf action =
    Internal.withTable sesh conf (action . Internal.Table')

{-# SPECIALISE new ::
     Session IO
  -> Common.TableConfig
  -> IO (Table IO k v b) #-}
new ::
     IOLike m
  => Session m
  -> Common.TableConfig
  -> m (Table m k v b)
new (Internal.Session' sesh) conf = Internal.Table' <$> Internal.new sesh conf

{-# SPECIALISE close ::
     Table IO k v b
  -> IO () #-}
close ::
     IOLike m
  => Table m k v b
  -> m ()
close (Internal.Table' t) = Internal.close t

{-------------------------------------------------------------------------------
  Table queries
-------------------------------------------------------------------------------}

-- | Result of a single point lookup.
data LookupResult v b =
    NotFound
  | Found         !v
  | FoundWithBlob !v !b
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

instance Bifunctor LookupResult where
  first f = \case
      NotFound          -> NotFound
      Found v           -> Found (f v)
      FoundWithBlob v b -> FoundWithBlob (f v) b

  second g = \case
      NotFound          -> NotFound
      Found v           -> Found v
      FoundWithBlob v b -> FoundWithBlob v (g b)

{-# SPECIALISE lookups ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table IO k v b
  -> V.Vector k
  -> IO (V.Vector (LookupResult v (BlobRef IO b))) #-}
{-# INLINEABLE lookups #-}
lookups ::
     forall m k v b. (
       IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     )
  => Table m k v b
  -> V.Vector k
  -> m (V.Vector (LookupResult v (BlobRef m b)))
lookups (Internal.Table' t) ks =
    V.map toLookupResult <$>
    Internal.lookups (resolve @v Proxy) (V.map Internal.serialiseKey ks) t
  where
    toLookupResult (Just e) = case e of
      Entry.Insert v            -> Found (Internal.deserialiseValue v)
      Entry.InsertWithBlob v br -> FoundWithBlob (Internal.deserialiseValue v)
                                                 (BlobRef br)
      Entry.Mupdate v           -> Found (Internal.deserialiseValue v)
      Entry.Delete              -> NotFound
    toLookupResult Nothing = NotFound

data QueryResult k v b =
    FoundInQuery         !k !v
  | FoundInQueryWithBlob !k !v !b
  deriving stock (Eq, Show, Functor, Foldable, Traversable)

instance Bifunctor (QueryResult k) where
  bimap f g = \case
      FoundInQuery k v           -> FoundInQuery k (f v)
      FoundInQueryWithBlob k v b -> FoundInQueryWithBlob k (f v) (g b)

{-# SPECIALISE rangeLookup ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Table IO k v b
  -> Range k
  -> IO (V.Vector (QueryResult k v (BlobRef IO b))) #-}
rangeLookup ::
     forall m k v b. (
       IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     )
  => Table m k v b
  -> Range k
  -> m (V.Vector (QueryResult k v (BlobRef m b)))
rangeLookup (Internal.Table' t) range =
    Internal.rangeLookup (resolve @v Proxy) (Internal.serialiseKey <$> range) t $ \k v mblob ->
      toQueryResult
        (Internal.deserialiseKey k)
        (Internal.deserialiseValue v)
        (BlobRef <$> mblob)

{-------------------------------------------------------------------------------
  Cursor
-------------------------------------------------------------------------------}

type Cursor :: (Type -> Type) -> Type -> Type -> Type -> Type
type Cursor = Internal.Cursor'

{-# SPECIALISE withCursor ::
     ResolveValue v
  => Table IO k v b
  -> (Cursor IO k v b -> IO a)
  -> IO a #-}
withCursor ::
     forall m k v b a.
     (IOLike m, ResolveValue v)
  => Table m k v b
  -> (Cursor m k v b -> m a)
  -> m a
withCursor (Internal.Table' t) action =
    Internal.withCursor (resolve (Proxy @v)) Internal.NoOffsetKey t (action . Internal.Cursor')

{-# SPECIALISE withCursorAtOffset ::
     (SerialiseKey k, ResolveValue v)
  => k
  -> Table IO k v b
  -> (Cursor IO k v b -> IO a)
  -> IO a #-}
withCursorAtOffset ::
     forall m k v b a.
     ( IOLike m
     , SerialiseKey k
     , ResolveValue v
     )
  => k
  -> Table m k v b
  -> (Cursor m k v b -> m a)
  -> m a
withCursorAtOffset offset (Internal.Table' t) action =
    Internal.withCursor (resolve (Proxy @v)) (Internal.OffsetKey (Internal.serialiseKey offset)) t $
      action . Internal.Cursor'

{-# SPECIALISE newCursor ::
     ResolveValue v
  => Table IO k v b
  -> IO (Cursor IO k v b) #-}
newCursor ::
     forall m k v b.
     (IOLike m, ResolveValue v)
  => Table m k v b
  -> m (Cursor m k v b)
newCursor (Internal.Table' t) =
    Internal.Cursor' <$!> Internal.newCursor (resolve (Proxy @v)) Internal.NoOffsetKey t

{-# SPECIALISE newCursorAtOffset ::
     (SerialiseKey k, ResolveValue v)
  => k
  -> Table IO k v b
  -> IO (Cursor IO k v b) #-}
newCursorAtOffset ::
     forall m k v b.
     ( IOLike m
     , SerialiseKey k
     , ResolveValue v
     )
  => k
  -> Table m k v b
  -> m (Cursor m k v b)
newCursorAtOffset offset (Internal.Table' t) =
    Internal.Cursor' <$!>
      Internal.newCursor (resolve (Proxy @v)) (Internal.OffsetKey (Internal.serialiseKey offset)) t

{-# SPECIALISE closeCursor ::
     Cursor IO k v b
  -> IO () #-}
closeCursor ::
     IOLike m
  => Cursor m k v b
  -> m ()
closeCursor (Internal.Cursor' c) = Internal.closeCursor c

{-# SPECIALISE readCursor ::
     (SerialiseKey k, SerialiseValue v, ResolveValue v)
  => Int
  -> Cursor IO k v b
  -> IO (V.Vector (QueryResult k v (BlobRef IO b))) #-}
readCursor ::
     forall m k v b. (
       IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , ResolveValue v
     )
  => Int
  -> Cursor m k v b
  -> m (V.Vector (QueryResult k v (BlobRef m b)))
readCursor n (Internal.Cursor' c) =
    Internal.readCursor (resolve (Proxy @v)) n c $ \k v mblob ->
      toQueryResult
        (Internal.deserialiseKey k)
        (Internal.deserialiseValue v)
        (BlobRef <$> mblob)

toQueryResult :: k -> v -> Maybe b -> QueryResult k v b
toQueryResult k v = \case
    Nothing    -> FoundInQuery k v
    Just b  -> FoundInQueryWithBlob k v b

{-------------------------------------------------------------------------------
  Table updates
-------------------------------------------------------------------------------}

data Update v b =
    Insert !v !(Maybe b)
  | Delete
  | Mupsert !v
  deriving stock (Show, Eq)

instance (NFData v, NFData b) => NFData (Update v b) where
  rnf Delete       = ()
  rnf (Insert v b) = rnf v `seq` rnf b
  rnf (Mupsert v)  = rnf v

{-# SPECIALISE updates ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b, ResolveValue v)
  => Table IO k v b
  -> V.Vector (k, Update v b)
  -> IO () #-}
updates ::
     forall m k v b. (
       IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue b
     , ResolveValue v
     )
  => Table m k v b
  -> V.Vector (k, Update v b)
  -> m ()
updates (Internal.Table' t) es = do
    Internal.updates (resolve @v Proxy) (V.mapStrict serialiseEntry es) t
  where
    serialiseEntry = bimap Internal.serialiseKey serialiseOp
    serialiseOp = bimap Internal.serialiseValue Internal.serialiseBlob
                . updateToEntry

    updateToEntry :: Update v b -> Entry.Entry v b
    updateToEntry = \case
        Insert v Nothing  -> Entry.Insert v
        Insert v (Just b) -> Entry.InsertWithBlob v b
        Delete            -> Entry.Delete
        Mupsert v         -> Entry.Mupdate v

{-# SPECIALISE inserts ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b, ResolveValue v)
  => Table IO k v b
  -> V.Vector (k, v, Maybe b)
  -> IO () #-}
inserts ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue b
     , ResolveValue v
     )
  => Table m k v b
  -> V.Vector (k, v, Maybe b)
  -> m ()
inserts t = updates t . fmap (\(k, v, b) -> (k, Insert v b))

{-# SPECIALISE deletes ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b, ResolveValue v)
  => Table IO k v b
  -> V.Vector k
  -> IO () #-}
deletes ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue b
     , ResolveValue v
     )
  => Table m k v b
  -> V.Vector k
  -> m ()
deletes t = updates t . fmap (,Delete)

{-# SPECIALISE mupserts ::
     (SerialiseKey k, SerialiseValue v, SerialiseValue b, ResolveValue v)
  => Table IO k v b
  -> V.Vector (k, v)
  -> IO () #-}
mupserts ::
     ( IOLike m
     , SerialiseKey k
     , SerialiseValue v
     , SerialiseValue b
     , ResolveValue v
     )
  => Table m k v b
  -> V.Vector (k, v)
  -> m ()
mupserts t = updates t . fmap (second Mupsert)

{-# SPECIALISE retrieveBlobs ::
     SerialiseValue b
  => Session IO
  -> V.Vector (BlobRef IO b)
  -> IO (V.Vector b) #-}
retrieveBlobs ::
     ( IOLike m
     , SerialiseValue b
     )
  => Session m
  -> V.Vector (BlobRef m b)
  -> m (V.Vector b)
retrieveBlobs (Internal.Session' (sesh :: Internal.Session m h)) refs =
    V.map Internal.deserialiseBlob <$>
      (Internal.retrieveBlobs sesh =<< V.imapM checkBlobRefType refs)
  where
    checkBlobRefType _ (BlobRef (ref :: Internal.WeakBlobRef m h'))
      | Just Refl <- eqT @h @h' = pure ref
    checkBlobRefType i _ = throwIO (Internal.ErrBlobRefInvalid i)

{-------------------------------------------------------------------------------
  Snapshots
-------------------------------------------------------------------------------}

{-# SPECIALISE createSnapshot ::
     Common.SnapshotLabel
  -> Common.SnapshotName
  -> Table IO k v b
  -> IO () #-}
createSnapshot :: forall m k v b.
     IOLike m
  => Common.SnapshotLabel
  -> Common.SnapshotName
  -> Table m k v b
  -> m ()
createSnapshot label snap (Internal.Table' t) =
    void $ Internal.createSnapshot snap label Internal.SnapFullTable t

{-# SPECIALISE openSnapshot ::
     ResolveValue v
  => Session IO
  -> Common.OverrideDiskCachePolicy
  -> Common.SnapshotLabel
  -> Common.SnapshotName
  -> IO (Table IO k v b ) #-}
openSnapshot :: forall m k v b.
     ( IOLike m
     , ResolveValue v
     )
  => Session m
  -> Common.OverrideDiskCachePolicy
  -> Common.SnapshotLabel
  -> Common.SnapshotName
  -> m (Table m k v b)
openSnapshot (Internal.Session' sesh) policyOveride label snap =
    Internal.Table' <$!>
      Internal.openSnapshot
        sesh
        policyOveride
        label
        Internal.SnapFullTable
        snap
        (resolve (Proxy @v))

{-------------------------------------------------------------------------------
  Mutiple writable tables
-------------------------------------------------------------------------------}

{-# SPECIALISE duplicate ::
     Table IO k v b
  -> IO (Table IO k v b) #-}
duplicate ::
     IOLike m
  => Table m k v b
  -> m (Table m k v b)
duplicate (Internal.Table' t) = Internal.Table' <$!> Internal.duplicate t

{-------------------------------------------------------------------------------
  Table union
-------------------------------------------------------------------------------}

{-# SPECIALISE union ::
     Table IO k v b
  -> Table IO k v b
  -> IO (Table IO k v b) #-}
union :: forall m k v b.
     IOLike m
  => Table m k v b
  -> Table m k v b
  -> m (Table m k v b)
union t1 t2 = unions $ t1 :| [t2]

{-# SPECIALISE unions ::
     NonEmpty (Table IO k v b)
  -> IO (Table IO k v b) #-}
unions :: forall m k v b.
     IOLike m
  => NonEmpty (Table m k v b)
  -> m (Table m k v b)
unions (t :| ts) =
    case t of
      Internal.Table' (t' :: Internal.Table m h) -> do
        ts' <- zipWithM (checkTableType (proxy# @h)) [1..] ts
        Internal.Table' <$> Internal.unions (t' :| ts')
  where
    checkTableType ::
         forall h. Typeable h
      => Proxy# h
      -> Int
      -> Table m k v b
      -> m (Internal.Table m h)
    checkTableType _ i (Internal.Table' (t' :: Internal.Table m h'))
      | Just Refl <- eqT @h @h' = pure t'
      | otherwise = throwIO $ Common.ErrTableUnionHandleTypeMismatch 0 (typeRep $ Proxy @h) i (typeRep $ Proxy @h')

{-# SPECIALISE remainingUnionDebt :: Table IO k v b -> IO UnionDebt #-}
remainingUnionDebt :: IOLike m => Table m k v b -> m UnionDebt
remainingUnionDebt (Internal.Table' t) =
    (\(Internal.UnionDebt x) -> UnionDebt x) <$>
      Internal.remainingUnionDebt t

{-# SPECIALISE supplyUnionCredits ::
     ResolveValue v => Table IO k v b -> UnionCredits -> IO UnionCredits #-}
supplyUnionCredits ::
     forall m k v b. (IOLike m, ResolveValue v)
  => Table m k v b
  -> UnionCredits
  -> m UnionCredits
supplyUnionCredits (Internal.Table' t) (UnionCredits credits) =
    (\(Internal.UnionCredits x) -> UnionCredits x) <$>
      Internal.supplyUnionCredits
        (resolve (Proxy @v))
        t
        (Internal.UnionCredits credits)

{-------------------------------------------------------------------------------
  Monoidal value resolution
-------------------------------------------------------------------------------}

-- | A class to specify how to resolve/merge values when using monoidal updates
-- ('Mupsert'). This is required for merging entries during compaction and also
-- for doing lookups, resolving multiple entries of the same key on the fly.
-- The class has some laws, which should be tested (e.g. with QuickCheck).
--
-- It is okay to assume that the input bytes can be deserialised using
-- 'deserialiseValue', as they are produced by either 'serialiseValue' or
-- 'resolveValue' itself, which are required to produce deserialisable output.
--
-- Prerequisites:
--
-- * [Valid Output] The result of resolution should always be deserialisable.
--   See 'resolveValueValidOutput'.
-- * [Associativity] Resolving values should be associative.
--   See 'resolveValueAssociativity'.
--
-- Future opportunities for optimisations:
--
-- * Include a function that determines whether it is safe to remove an 'Update'
--   from the last level of an LSM tree.
--
-- * Include a function @v -> RawBytes -> RawBytes@, which can then be used when
--   inserting into the write buffer. Currently, using 'resolveDeserialised'
--   means that the inserted value is serialised and (if there is another value
--   with the same key in the write buffer) immediately deserialised again.
--
-- TODO: The laws depend on 'SerialiseValue', should we make it a superclass?
-- TODO: should we additionally require Totality (for any input 'RawBytes',
--       resolution should successfully provide a result)? This could reduce the
--       risk of encountering errors during a run merge.
class ResolveValue v where
  resolveValue :: Proxy v -> RB.RawBytes -> RB.RawBytes -> RB.RawBytes

-- | Test the __Valid Output__ law for the 'ResolveValue' class
resolveValueValidOutput :: forall v.
     (SerialiseValue v, ResolveValue v, NFData v)
  => v -> v -> Bool
resolveValueValidOutput (serialiseValue -> x) (serialiseValue -> y) =
    (deserialiseValue (resolveValue (Proxy @v) x y) :: v) `deepseq` True

-- | Test the __Associativity__ law for the 'ResolveValue' class
resolveValueAssociativity :: forall v.
     (SerialiseValue v, ResolveValue v)
  => v -> v -> v -> Bool
resolveValueAssociativity (serialiseValue -> x) (serialiseValue -> y) (serialiseValue -> z) =
    x <+> (y <+> z) == (x <+> y) <+> z
  where
    (<+>) = resolveValue (Proxy @v)

-- | A helper function to implement 'resolveValue' by operating on the
-- deserialised representation. Note that especially for simple types it
-- should be possible to provide a more efficient implementation by directly
-- operating on the 'RawBytes'.
--
-- This function could potentially be used to provide a default implementation
-- for 'resolveValue', but it's probably best to be explicit about instances.
--
-- To satisfy the prerequisites of 'ResolveValue', the function provided to
-- 'resolveDeserialised' should itself satisfy some properties:
--
-- * [Associativity] The provided function should be associative.
-- * [Totality] The provided function should be total.
resolveDeserialised :: forall v.
     SerialiseValue v
  => (v -> v -> v) -> Proxy v -> RB.RawBytes -> RB.RawBytes -> RB.RawBytes
resolveDeserialised f _ x y =
    serialiseValue (f (deserialiseValue x) (deserialiseValue y))

resolve ::  forall v. ResolveValue v => Proxy v -> Internal.ResolveSerialisedValue
resolve = coerce . resolveValue

-- | Mostly to give an example instance (plus the property tests for it).
-- Additionally, this instance for 'Sum' provides a nice monoidal, numerical
-- aggregator.
instance (Num a, SerialiseValue a) => ResolveValue (Sum a) where
  resolveValue = resolveDeserialised (+)

-- | Newtype wrapper for values, so that 'Mupsert's behave like 'Insert's.
--
-- If there is no intent to use 'Mupsert's, then the user still has to define a
-- 'ResolveValue' instance for their values, unless they use this newtype, which
-- provides a sensible default instance.

-- This wrapper can be used to give values a 'ResolveValue' instance that
-- resolves values as a 'Data.Semigroup.First' semigroup. 'ResolveValue' can be
-- used in conjunction with @deriving via@ to give the values an instance
-- directly, or the newtype can be used in the table/cursor type like @'Table' k
-- (ResolveAsFirst v) b@.
newtype ResolveAsFirst v = ResolveAsFirst v
  deriving stock (Show, Eq, Ord)
  deriving newtype SerialiseValue

instance ResolveValue (ResolveAsFirst v) where
  resolveValue ::
       Proxy (ResolveAsFirst v)
    -> RB.RawBytes
    -> RB.RawBytes
    -> RB.RawBytes
  resolveValue _ x _ = x
