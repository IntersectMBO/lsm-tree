
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE MagicHash             #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE QuantifiedConstraints #-}
{-# LANGUAGE TypeFamilies          #-}

{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-orphans #-}

module Test.Util.FS.HKD (

  ) where

import           Control.Concurrent.Class.MonadMVar
import           Control.Concurrent.Class.MonadSTM (MonadSTM)
import           Control.Concurrent.Class.MonadSTM.Strict (StrictTMVar,
                     StrictTVar, readTVarIO)
import           Control.Monad (void)
import           Control.Monad.Class.MonadThrow (MonadThrow)
import           Control.Monad.Primitive
import qualified Data.ByteString as BS
import           Data.Int
import           Data.Kind (Constraint, Type)
import           Data.Primitive.ByteArray
import           Data.Primitive.PrimVar
import           Data.Set
import           Data.SOP
import           Data.SOP.Constraint (SListIN)
import           Data.Word
import           Generics.SOP
import           GHC.Exts (Proxy#, proxy#)
import qualified GHC.Generics as GHC
import           System.FS.API
import           System.FS.IO
import           System.FS.Sim.Error
import           System.FS.Sim.MockFS (HandleMock, MockFS)
import           System.FS.Sim.Stream (Stream (UnsafeStream))
import           System.Posix.Types

type DumpState m h = m String
type HOpen m h = FsPath -> OpenMode -> m (Handle h)
type HClose m h = Handle h -> m ()
type HIsOpen m h = Handle h -> m Bool
type HSeek m h = Handle h -> SeekMode -> Int64 -> m ()
type HGetSome m h = Handle h -> Word64 -> m BS.ByteString
type HGetSomeAt m h = Handle h -> Word64 -> AbsOffset -> m BS.ByteString
type HPutSome m h = Handle h -> BS.ByteString -> m Word64
type HTruncate m h = Handle h -> Word64 -> m ()
type HGetSize m h = Handle h -> m Word64
type CreateDirectory m h = FsPath -> m ()
type CreateDirectoryIfMissing m h = Bool -> FsPath -> m ()
type ListDirectory m h = FsPath -> m (Set String)
type DoesDirectoryExist m h = FsPath -> m Bool
type DoesFileExist m h = FsPath -> m Bool
type RemoveDirectoryRecursive m h = FsPath -> m ()
type RemoveFile m h = FsPath -> m ()
type RenameFile m h = FsPath -> FsPath -> m ()
type MkFsErrorPath m h = FsPath -> FsErrorPath
type UnsafeToFilePath m h = FsPath -> m FilePath
type HGetBufSome m h = Handle h -> MutableByteArray (PrimState m) -> BufferOffset -> ByteCount -> m ByteCount
type HGetBufSomeAt m h = Handle h -> MutableByteArray (PrimState m) -> BufferOffset  -> ByteCount -> AbsOffset -> m ByteCount
type HPutBufSome m h = Handle h -> MutableByteArray (PrimState m) -> BufferOffset  -> ByteCount  -> m ByteCount
type HPutBufSomeAt m h = Handle h -> MutableByteArray (PrimState m) -> BufferOffset -> ByteCount -> AbsOffset -> m ByteCount

deriving stock instance GHC.Generic Errors
deriving anyclass instance Generic Errors

-- pattern CodeFS ::
--      (IsProductType (HasFS m h) (InnerCode m h), All Top (InnerCode m h))
--   => f (DumpState m h)
--   -> f (HOpen m h)
--   -> g f (Code (HasFS m h))
-- pattern CodeFS{cfsDumpState, cfsHOpen} <- (productTypeFrom -> I cfsDumpState :* I cfsHOpen :* _)

type InnerCode m h = '[
          DumpState m h
        , HOpen m h
        , HClose m h
        , HIsOpen m h
        , HSeek m h
        , HGetSome m h
        , HGetSomeAt m h
        , HPutSome m h
        , HTruncate m h
        , HGetSize m h
        , CreateDirectory m h
        , CreateDirectoryIfMissing m h
        , ListDirectory m h
        , DoesDirectoryExist m h
        , DoesFileExist m h
        , RemoveDirectoryRecursive m h
        , RemoveFile m h
        , RenameFile m h
        , MkFsErrorPath m h
        , UnsafeToFilePath m h
        , HGetBufSome m h
        , HGetBufSomeAt m h
        , HPutBufSome m h
        , HPutBufSomeAt m h
        ]

instance Generic (HasFS m h) where
  type Code (HasFS m h) = '[ InnerCode m h ]

  from HasFS{..} = SOP $ Z $
         I dumpState
        -- file operatoins
      :* I hOpen
      :* I hClose
      :* I hIsOpen
      :* I hSeek
      :* I hGetSome
      :* I hGetSomeAt
      :* I hPutSome
      :* I hTruncate
      :* I hGetSize
        -- directory operations
      :* I createDirectory
      :* I createDirectoryIfMissing
      :* I listDirectory
      :* I doesDirectoryExist
      :* I doesFileExist
      :* I removeDirectoryRecursive
      :* I removeFile
      :* I renameFile
      :* I mkFsErrorPath
      :* I unsafeToFilePath
        -- file I\/O with user-supplied buffers
      :* I hGetBufSome
      :* I hGetBufSomeAt
      :* I hPutBufSome
      :* I hPutBufSomeAt
      :* Nil

  to (SOP (Z x)) = HasFS {..}
    where
      I dumpState
          -- file operatoins
        :* I hOpen
        :* I hClose
        :* I hIsOpen
        :* I hSeek
        :* I hGetSome
        :* I hGetSomeAt
        :* I hPutSome
        :* I hTruncate
        :* I hGetSize
          -- directory operations
        :* I createDirectory
        :* I createDirectoryIfMissing
        :* I listDirectory
        :* I doesDirectoryExist
        :* I doesFileExist
        :* I removeDirectoryRecursive
        :* I removeFile
        :* I renameFile
        :* I mkFsErrorPath
        :* I unsafeToFilePath
          -- file I\/O with user-supplied buffers
        :* I hGetBufSome
        :* I hGetBufSomeAt
        :* I hPutBufSome
        :* I hPutBufSomeAt
        :* Nil = x
  to (SOP (S x)) = case x of {}

{-------------------------------------------------------------------------------
  Indices
-------------------------------------------------------------------------------}

type Index :: [k] -> k -> Type
data Index xs x where
  IZ ::               Index (x ': xs) x
  IS :: Index xs x -> Index (y ': xs) x

type Indexx :: [[k]] -> k -> Type
data Indexx xss x where
  IZZ :: Index xs x -> Indexx (xs ': xss) x
  ISS :: Indexx xss x -> Indexx (ys ': xss) x

type Ind :: ((k -> Type) -> l -> Type) -> l -> k -> Type
type family Ind h

type instance Ind NP = Index
type instance Ind NS = Index
type instance Ind POP = Indexx
type instance Ind SOP = Indexx

type HIndices :: ((k -> Type) -> l -> Type) -> Constraint
class HIndices h where
  hindices :: (SListIN h l) => Proxy# h -> (Prod h) (Ind h l) l

instance HIndices NP where
  hindices :: forall xs. SListI xs => Proxy# NP -> NP (Index xs) xs
  hindices _ = case sList @xs of
    SNil  -> Nil
    SCons -> IZ :* hmap IS (hindices (proxy# @NP))

instance HIndices NS where
  hindices :: forall xs. SListI xs => Proxy# NS -> NP (Index xs) xs
  hindices _ = case sList @xs of
    SNil  -> Nil
    SCons -> IZ :* hmap IS (hindices (proxy# @NP))

instance HIndices POP where
  hindices :: forall xss. SListI2 xss => Proxy# POP -> POP (Indexx xss) xss
  hindices _ = case sList @xss of
    SNil -> POP Nil
    SCons @yss @xs -> case hindices @_ @_ @_ @yss (proxy# @POP) of
      POP rest -> POP $ hmap IZZ (hindices @_ @_ @_ @xs (proxy# @NP)) :* hcmap (Proxy @SListI) (hmap ISS) rest

type HIPure :: ((k -> Type) -> l -> Type) -> Constraint
class HIPure h where
  hcipure :: (HAp h, SListIN h l, AllN h c l) => Proxy# c -> (forall x. c x => Ind h l x -> f x) -> h f l

instance HIPure NP where
  hcipure (_ :: Proxy# c) f = hcpure (Proxy @c) (fn f) `hap` hindices (proxy# @NP)

instance HIPure POP where
  hcipure (_ :: Proxy# c) f = hcpure (Proxy @c) (fn f) `hap` hindices (proxy# @POP)

{-------------------------------------------------------------------------------
  Prefix actions
-------------------------------------------------------------------------------}

hprefixActions :: forall h f l m.
     (AllN h (Compose (PrefixAction m) f) l, HAp h, Prod h ~ h)
  => h (K (m ())) l -> h f l -> h f l
hprefixActions xs ys = hcliftA2 (Proxy @(Compose (PrefixAction m) f)) f xs ys
  where
    f :: forall x. PrefixAction m (f x) => K (m ()) x -> f x -> f x
    f (K action) x = prefixAction action x

class PrefixAction m a where
  prefixAction :: m b -> a -> a

instance PrefixAction m FsErrorPath where
  prefixAction _ x = x

instance PrefixAction m b => PrefixAction m (a -> b) where
  prefixAction action f = \x -> (prefixAction action (f x))

instance Monad m => PrefixAction m (m a) where
  prefixAction action1 action2 = action1 >> action2

{-------------------------------------------------------------------------------
  Peek
-------------------------------------------------------------------------------}

hpeek ::
     forall f h l. (HAp h, AllN (Prod h) (Compose Peek f) l)
  => h f l -> h (WrapPeekItem :.: f) l
hpeek xs = hcmap (Proxy @(Compose Peek f)) f xs
  where
    f :: forall x. Peek (f x) => f x -> (WrapPeekItem :.: f) x
    f x = Comp $ WrapPeekItem $ peek x

newtype WrapPeekItem a = WrapPeekItem {unwrapPeekItem :: PeekItem a }

class Peek a where
  type PeekItem a :: Type
  peek :: a -> PeekItem a

instance Peek (Stream a) where
  type instance PeekItem (Stream a) = Maybe a
  peek (UnsafeStream _ xs) = case xs of
      []    -> Nothing
      (x:_) -> x

instance Peek a => Peek (I a) where
  type instance PeekItem (I a) = PeekItem a
  peek (I x) = peek x

{-------------------------------------------------------------------------------
  Prefix actions
-------------------------------------------------------------------------------}

newtype Peeker m a = Peeker (MVar m [PeekItem a])

newPeeker :: MonadMVar m => m ((Peeker m :.: f) a)
newPeeker = Comp . Peeker <$> newMVar []

newPeekers :: (HPure h, SListIN h xs, HSequence h, MonadMVar m) => m (h (Peeker m :.: f) xs)
newPeekers = hsequence' $ (hpure (Comp newPeeker))

pushPeeker :: MonadMVar m => Peeker m a -> PeekItem a -> m ()
pushPeeker (Peeker var) x = modifyMVar_ var (pure . (x:))

barrr ::
     forall m h f g l. (SListIN h l, HAp h
     , MonadSTM m
     , MonadMVar m
     , AllN h (Compose (PrefixAction m) f) l
     , HIPure h
     , Prod h ~ h
     , AllN h Top l
     )
  => h (Peeker m :.: g) l -> m (h g l) -> h f l -> h f l
barrr peekers var =
    hcliftA3 (Proxy @(Compose (PrefixAction m) f)) f peekers (fooo var)
  where
    f :: (PrefixAction m (f x), Peek (g x)) => (Peeker m :.: g) x -> (m :.: g) x -> f x -> f x
    f (Comp peeker) (Comp getStream) action =
        flip prefixAction action $ do
          s <- getStream
          let x = peek s
          pushPeeker peeker x


{- barrr :: StrictTVar m (h f xs) -> h (Peeker m) xs -> h I xs -> h I xs
barrr var peekers hfs = hcliftA2 (Proxy @Top) f peekers hfs
  where
    f peeker prim = undefined -}

fooo ::
     forall m h f l. (
       SListIN h l, HAp h
     , MonadSTM m
     , HIPure h
     , Prod h ~ h
     , AllN h Top l
     )
  => m (h f l)
  -> h (m :.: f) l
fooo var = hcipure (proxy# @Top) f
  where
    f :: forall x. Ind h l x -> (m :.: f) x
    f ind = Comp $ do
      xs <- var
      pure (proj (proxy# @h) ind xs)

proj :: Proxy# h -> Ind h l x -> Prod h f l -> f x
proj = undefined


-- >>> foo
-- (0,3)
foo :: IO (Int, Int)
foo = do
    let hfs = ioHasFS @IO (MountPoint "")
    cs <- newCounters
    x <- case cs of
      POP ((K c :* _) :* Nil) -> readPrimVar c
    let hfs' = to (temp2 (Proxy @IO) (Proxy @(HasFS IO HandleIO)) cs (from hfs))
    _ <- dumpState hfs'
    _ <- dumpState hfs'
    _ <- dumpState hfs'
    y <- case cs of
       POP ((K c :* _) :* Nil) -> readPrimVar c
    pure (x, y)


newCounters :: (All SListI xs, PrimMonad m) => m (POP (K (PrimVar (PrimState m) Int)) xs)
newCounters = hsequenceK (hpure (K (newPrimVar 0)))

incrCounter :: PrimMonad m => PrimVar (PrimState m) Int -> m ()
incrCounter var = void $ fetchAddInt var 1



temp2 :: forall a m. (
       PrimMonad m
     , All (All (PrefixAction m)) (Code a)
     )
  => Proxy m -> Proxy a -> POP (K (PrimVar (PrimState m) Int)) (Code a) -> Rep a -> Rep a
temp2 _ _ counters rep = hcliftA2 (Proxy @(PrefixAction m)) f counters rep
  where
    f :: forall x. PrefixAction m x => K (PrimVar (PrimState m) Int) x -> I x -> I x
    f (K var) (I x) = I (prefixAction (incrCounter @m var) x)



type HKD m h f = SOP f (Code (HasFS m h))

pattern HKD ::
     f (DumpState m h)
  -> f (HOpen m h)
  -> f (HClose m h)
  -> f (HIsOpen m h)
  -> f (HSeek m h)
  -> f (HGetSome m h)
  -> f (HGetSomeAt m h)
  -> f (HPutSome m h)
  -> f (HTruncate m h)
  -> f (HGetSize m h)
  -> f (CreateDirectory m h)
  -> f (CreateDirectoryIfMissing m h)
  -> f (ListDirectory m h)
  -> f (DoesDirectoryExist m h)
  -> f (DoesFileExist m h)
  -> f (RemoveDirectoryRecursive m h)
  -> f (RemoveFile m h)
  -> f (RenameFile m h)
  -> f (MkFsErrorPath m h)
  -> f (UnsafeToFilePath m h)
  -> f (HGetBufSome m h)
  -> f (HGetBufSomeAt m h)
  -> f (HPutBufSome m h)
  -> f (HPutBufSomeAt m h)
  -> HKD m h f
pattern HKD {
    dumpStateHKD
    -- file operation
  , hOpenHKD, hCloseHKD, hIsOpenHKD, hSeekHKD, hGetSomeHKD, hGetSomeAtHKD
  , hPutSomeHKD, hTruncateHKD, hGetSizeHKD
    -- directory operations
  , createDirectoryHKD, createDirectoryIfMissingHKD, listDirectoryHKD
  , doesDirectoryExistHKD,doesFileExistHKD, removeDirectoryRecursiveHKD
  , removeFileHKD, renameFileHKD, mkFsErrorPathHKD, unsafeToFilePathHKD
    -- file I\/O with user-supplied buffers
  , hGetBufSomeHKD, hGetBufSomeAtHKD, hPutBufSomeHKD, hPutBufSomeAtHKD
  } = SOP (Z (
         dumpStateHKD
        -- file operations
      :* hOpenHKD
      :* hCloseHKD
      :* hIsOpenHKD
      :* hSeekHKD
      :* hGetSomeHKD
      :* hGetSomeAtHKD
      :* hPutSomeHKD
      :* hTruncateHKD
      :* hGetSizeHKD
        -- directory operations
      :* createDirectoryHKD
      :* createDirectoryIfMissingHKD
      :* listDirectoryHKD
      :* doesDirectoryExistHKD
      :* doesFileExistHKD
      :* removeDirectoryRecursiveHKD
      :* removeFileHKD
      :* renameFileHKD
      :* mkFsErrorPathHKD
      :* unsafeToFilePathHKD
        -- file I\/O with user-supplied buffers
      :* hGetBufSomeHKD
      :* hGetBufSomeAtHKD
      :* hPutBufSomeHKD
      :* hPutBufSomeAtHKD
      :* Nil))


-- | Introduce possibility of errors
simErrorHasFS2 ::
     forall m. (MonadSTM m, MonadThrow m, PrimMonad m)
  => StrictTMVar m MockFS
  -> StrictTVar m Errors
  -> m (HasFS m HandleMock, HKD m HandleMock (Peeker m))
simErrorHasFS2 fsVar errorsVar = do
    let hfs = simErrorHasFS fsVar errorsVar

    ps <- newPeekers
    let var = productTypeFrom <$> readTVarIO errorsVar
        hfs' = productTypeFrom hfs
        hfs'' = barrr ps var hfs'
    pure $ productTypeTo hfs''



{-
data HKD m h f = HKD {
    dumpStateHKD              :: f (DumpState m h)
    -- file operations
  , hOpenHKD                    :: f (HOpen m h)
  , hCloseHKD                  :: f (HClose m h)
  , hIsOpenHKD                 :: f (HIsOpen m h)
  , hSeekHKD                   :: f (HSeek m h)
  , hGetSomeHKD                :: f (HGetSome m h)
  , hGetSomeAtHKD              :: f (HGetSomeAt m h)
  , hPutSomeHKD                :: f (HPutSome m h)
  , hTruncateHKD               :: f (HTruncate m h)
  , hGetSizeHKD                :: f (HGetSize m h)
    -- directory operations
  , createDirectoryHKD         :: f (CreateDirectory m h)
  , createDirectoryIfMissingHKD:: f (CreateDirectoryIfMissing m h)
  , listDirectoryHKD           :: f (ListDirectory m h)
  , doesDirectoryExistHKD      :: f (DoesDirectoryExist m h)
  , doesFileExistHKD           :: f (DoesFileExist m h)
  , removeDirectoryRecursiveHKD:: f (RemoveDirectoryRecursive m h)
  , removeFileHKD              :: f (RemoveFile m h)
  , renameFileHKD              :: f (RenameFile m h)
  , mkFsErrorPathHKD           :: f (MkFsErrorPath m h)
  , unsafeToFilePathHKD        :: f (UnsafeToFilePath m h)
  , hGetBufSomeHKD             :: f (HGetBufSome m h)
  , hGetBufSomeAtHKD           :: f (HGetBufSomeAt m h)
  , hPutBufSomeHKD             :: f (HPutBufSome m h)
  , hPutBufSomeAtHKD           :: f (HPutBufSomeAt m h)
  } -}


{- withEntryCounters ::
     MonadSTM m
  => HKD m h (StrictTVar m)
  -> HasFS m h
  -> HasFS m h
withEntryCounters hkd HasFS{..} = HasFS {
      dumpState = incrTVar dumpStateC >> dumpState
      -- file operatoins
    , hOpen = \a b -> incrTVar hOpenC >> hOpen a b
    , hClose = \a -> incrTVar hCloseC >> hClose a
    , hIsOpen = \a -> incrTVar hIsOpenC >> hIsOpen a
    , hSeek = \a b c -> incrTVar hSeekC >> hSeek a b c
    , hGetSome = \a b -> incrTVar hGetSomeC >> hGetSome a b
    , hGetSomeAt = \a b c -> incrTVar hGetSomeAtC >> hGetSomeAt a b c
    , hPutSome = \a b -> incrTVar hPutSomeC >> hPutSome a b
    , hTruncate = \a b -> incrTVar hTruncateC >> hTruncate a b
    , hGetSize = \a -> incrTVar hGetSizeC >> hGetSize a
      -- directory operations
    , createDirectory = \a -> incrTVar createDirectoryC >> createDirectory a
    , createDirectoryIfMissing = \a b -> incrTVar createDirectoryIfMissingC >> createDirectoryIfMissing a b
    , listDirectory = \a -> incrTVar listDirectoryC >> listDirectory a
    , doesDirectoryExist = \a -> incrTVar doesDirectoryExistC >> doesDirectoryExist a
    , doesFileExist = \a -> incrTVar doesFileExistC >> doesFileExist a
    , removeDirectoryRecursive = \a -> incrTVar removeDirectoryRecursiveC >> removeDirectoryRecursive a
    , removeFile = \a -> incrTVar removeFileC >> removeFile a
    , renameFile = \a b -> incrTVar renameFileC >> renameFile a b
    , mkFsErrorPath = mkFsErrorPath
    , unsafeToFilePath = unsafeToFilePath
      -- file I\/O with user-supplied buffers
    , hGetBufSome = \a b c d -> incrTVar hGetBufSomeC >> hGetBufSome a b c d
    , hGetBufSomeAt = \a b c d e -> incrTVar hGetBufSomeAtC >> hGetBufSomeAt a b c d e
    , hPutBufSome = \a b c d -> incrTVar hPutBufSomeC >> hPutBufSome a b c d
    , hPutBufSomeAt = \a b c d e -> incrTVar hPutBufSomeAtC >> hPutBufSomeAt a b c d e
    } -}
