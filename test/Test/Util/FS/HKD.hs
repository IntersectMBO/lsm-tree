
{-# LANGUAGE CPP                   #-}
{-# LANGUAGE DataKinds             #-}
{-# LANGUAGE PatternSynonyms       #-}
{-# LANGUAGE QuantifiedConstraints #-}

{-# OPTIONS_GHC -Wno-unused-top-binds #-}
{-# OPTIONS_GHC -Wno-orphans #-}
{-# LANGUAGE TypeFamilies          #-}

module Test.Util.FS.HKD (

  ) where

import           Control.Monad (void)
import           Control.Monad.Primitive
import qualified Data.ByteString as BS
import           Data.Int
import           Data.Primitive.ByteArray
import           Data.Primitive.PrimVar
import           Data.Set
import           Data.SOP
import           Data.Word
import           Generics.SOP
import qualified GHC.Generics as GHC
import           System.FS.API
import           System.FS.IO
import           System.FS.Sim.Error
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

pattern CodeFS ::
     (IsProductType (HasFS m h) (InnerCode m h), All Top (InnerCode m h))
  => f (DumpState m h)
  -> f (HOpen m h)
  -> g f (Code (HasFS m h))
pattern CodeFS{cfsDumpState, cfsHOpen} <- (productTypeFrom -> I cfsDumpState :* I cfsHOpen :* _)

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

newCounters :: (All SListI xs, PrimMonad m) => m (POP (K (PrimVar (PrimState m) Int)) xs)
newCounters = hsequenceK (hpure (K (newPrimVar 0)))

incrCounter :: PrimMonad m => PrimVar (PrimState m) Int -> m ()
incrCounter var = void $ fetchAddInt var 1

class PrefixAction m a where
  prefixAction :: m b -> a -> a

instance PrefixAction m FsErrorPath where
  prefixAction _ x = x

instance PrefixAction m b => PrefixAction m (a -> b) where
  prefixAction action f = \x -> (prefixAction action (f x))

instance Monad m => PrefixAction m (m a) where
  prefixAction action1 action2 = action1 >> action2


temp2 :: forall a m. (
       PrimMonad m
     , All (All (PrefixAction m)) (Code a)
     )
  => Proxy m -> Proxy a -> POP (K (PrimVar (PrimState m) Int)) (Code a) -> Rep a -> Rep a
temp2 _ _ counters rep = hcliftA2 (Proxy @(PrefixAction m)) f counters rep
  where
    f :: forall x. PrefixAction m x => K (PrimVar (PrimState m) Int) x -> I x -> I x
    f (K var) (I x) = I (prefixAction (incrCounter @m var) x)

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

-- pattern FSHKD :: HKD m h f
-- pattern FSHKD {dumpStateHKD,hOpenHKD,hCloseHKD,hIsOpenHKD,hSeekHKD,hGetSomeHKD,hGetSomeAtHKD,hPutSomeHKD
--   ,hTruncateHKD,hGetSizeHKD,createDirectoryHKD,createDirectoryIfMissingHKD,listDirectoryHKD,doesDirectoryExistHKD} =
--         I dumpStateHKD
--         -- file operatoins
--       :* I hOpenHKD
--       :* I hCloseHKD
--       :* I hIsOpenHKD
--       :* I hSeekHKD
--       :* I hGetSomeHKD
--       :* I hGetSomeAtHKD
--       :* I hPutSomeHKD
--       :* I hTruncateHKD
--       :* I hGetSizeHKD
--         -- directory operations
--       :* I createDirectoryHKD
--       :* I createDirectoryIfMissingHKD
--       :* I listDirectoryHKD
--       :* I doesDirectoryExistHKD
--       :* I doesFileExistHKD
--       :* I removeDirectoryRecursiveHKD
--       :* I removeFileHKD
--       :* I renameFileHKD
--       :* I mkFsErrorPathHKD
--       :* I unsafeToFilePathHKD
--         -- file I\/O with user-supplied buffers
--       :* I hGetBufSomeHKD
--       :* I hGetBufSomeAtHKD
--       :* I hPutBufSomeHKD
--       :* I hPutBufSomeAtHKD
--       :* Nil


--   -- | Introduce possibility of errors
-- simErrorHasFS ::
--      forall m. (MonadSTM m, MonadThrow m, PrimMonad m)
--   => StrictTMVar m MockFS
--   -> StrictTVar m Errors
--   -> HasFS m HandleMock
-- simErrorHasFS fsVar errorsVar =
--     -- TODO: Lenses would be nice for the setters
--     case Sim.simHasFS fsVar of
--       hfs@HasFS{..} -> HasFS{
--           dumpState =
--             withErr errorsVar (mkFsPath ["<dumpState>"]) dumpState "dumpState"
--               dumpStateE (\e es -> es { dumpStateE = e })
--         , hOpen      = \p m ->
--             withErr errorsVar p (hOpen p m) "hOpen"
--             hOpenE (\e es -> es { hOpenE = e })
--         , hClose     = \h ->
--             withErr' errorsVar h (hClose h) "hClose"
--             hCloseE (\e es -> es { hCloseE = e })
--         , hIsOpen    = hIsOpen
--         , hSeek      = \h m n ->
--             withErr' errorsVar h (hSeek h m n) "hSeek"
--             hSeekE (\e es -> es { hSeekE = e })
--         , hGetSome   = hGetSome' errorsVar hGetSome
--         , hGetSomeAt = hGetSomeAt' errorsVar hGetSomeAt
--         , hPutSome   = hPutSome' errorsVar hPutSome
--         , hTruncate  = \h w ->
--             withErr' errorsVar h (hTruncate h w) "hTruncate"
--             hTruncateE (\e es -> es { hTruncateE = e })
--         , hGetSize   =  \h ->
--             withErr' errorsVar h (hGetSize h) "hGetSize"
--             hGetSizeE (\e es -> es { hGetSizeE = e })

--         , createDirectory          = \p ->
--             withErr errorsVar p (createDirectory p) "createDirectory"
--             createDirectoryE (\e es -> es { createDirectoryE = e })
--         , createDirectoryIfMissing = \b p ->
--             withErr errorsVar p (createDirectoryIfMissing b p) "createDirectoryIfMissing"
--             createDirectoryIfMissingE (\e es -> es { createDirectoryIfMissingE = e })
--         , listDirectory            = \p ->
--             withErr errorsVar p (listDirectory p) "listDirectory"
--             listDirectoryE (\e es -> es { listDirectoryE = e })
--         , doesDirectoryExist       = \p ->
--             withErr errorsVar p (doesDirectoryExist p) "doesDirectoryExist"
--             doesDirectoryExistE (\e es -> es { doesDirectoryExistE = e })
--         , doesFileExist            = \p ->
--             withErr errorsVar p (doesFileExist p) "doesFileExist"
--             doesFileExistE (\e es -> es { doesFileExistE = e })
--         , removeDirectoryRecursive = \p ->
--             withErr errorsVar p (removeDirectoryRecursive p) "removeFile"
--             removeDirectoryRecursiveE (\e es -> es { removeDirectoryRecursiveE = e })
--         , removeFile               = \p ->
--             withErr errorsVar p (removeFile p) "removeFile"
--             removeFileE (\e es -> es { removeFileE = e })
--         , renameFile               = \p1 p2 ->
--             withErr errorsVar p1 (renameFile p1 p2) "renameFile"
--             renameFileE (\e es -> es { renameFileE = e })
--         , mkFsErrorPath = fsToFsErrorPathUnmounted
--         , unsafeToFilePath = error "simErrorHasFS:unsafeToFilePath"
--           -- File I\/O with user-supplied buffers
--         , hGetBufSome   = hGetBufSomeWithErr   errorsVar hfs
--         , hGetBufSomeAt = hGetBufSomeAtWithErr errorsVar hfs
--         , hPutBufSome   = hPutBufSomeWithErr   errorsVar hfs
--         , hPutBufSomeAt = hPutBufSomeAtWithErr errorsVar hfs
--         }


pattern Foo :: f Int -> f Char -> NP f [Int, Char]
pattern Foo {foo1, foo2} = foo1 :* foo2 :* Nil

#define types(binOp) \
        (DumpState m h) \
  binOp (HOpen m h)

bar :: types (->)
bar = undefined


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
