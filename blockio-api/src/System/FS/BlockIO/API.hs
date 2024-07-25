{-# LANGUAGE DerivingVia                #-}
{-# LANGUAGE GeneralisedNewtypeDeriving #-}
{-# LANGUAGE MultiParamTypeClasses      #-}
{-# LANGUAGE RankNTypes                 #-}
{-# LANGUAGE StandaloneDeriving         #-}
{-# LANGUAGE TypeFamilies               #-}
{-# LANGUAGE UnboxedTuples              #-}

module System.FS.BlockIO.API (
    HasBlockIO (..)
  , IOCtxParams (..)
  , defaultIOCtxParams
  , mkClosedError
  , IOOp (..)
  , ioopHandle
  , ioopFileOffset
  , ioopBuffer
  , ioopBufferOffset
  , ioopByteCount
  , IOResult (..)
    -- * Advice
  , Advice (..)
  , hAdviseAll
  , hDropCacheAll
    -- * Re-exports
  , ByteCount
  , FileOffset
  ) where

import           Control.DeepSeq
import           Control.Monad.Primitive (PrimMonad (PrimState))
import           Data.Primitive.ByteArray (MutableByteArray)
import qualified Data.Vector as V
import qualified Data.Vector.Generic as VG
import qualified Data.Vector.Generic.Mutable as VGM
import qualified Data.Vector.Primitive as VP
import qualified Data.Vector.Unboxed as VU
import qualified Data.Vector.Unboxed.Mutable as VUM
import           GHC.IO.Exception (IOErrorType (ResourceVanished))
import           GHC.Stack (HasCallStack)
import           System.FS.API
import           System.IO.Error (ioeSetErrorString, mkIOError)
import           System.Posix.Types (ByteCount, FileOffset)

-- | Abstract interface for submitting large batches of I\/O operations.
data HasBlockIO m h = HasBlockIO {
    -- | (Idempotent) close the interface.
    --
    -- Using 'submitIO' after 'close' should thrown an 'FsError' exception. See
    -- 'mkClosedError'.
    close    :: HasCallStack => m ()
    -- | Submit a batch of I\/O operations and wait for the result.
    --
    -- Results correspond to input 'IOOp's in a pair-wise manner, i.e., one can
    -- match 'IOOp's with 'IOResult's by indexing into both vectors at the same
    -- position.
    --
    -- If any of the I\/O operations fails, an 'FsError' exception will be thrown.
  , submitIO :: HasCallStack => V.Vector (IOOp (PrimState m) h) -> m (VU.Vector IOResult)
    -- | Set the file data caching mode for a file handle.
    --
    -- This has different effects on different distributions.
    -- * [Linux]: set the @O_DIRECT@ flag.
    -- * [MacOS]: set the @F_NOCACHE@ flag.
    -- * [Windows]: no-op.
    --
    -- TODO: subsequent reads/writes with misaligned byte arrays should fail
    -- both in simulation and real implementation.
  , hSetNoCache :: Handle h -> Bool -> m ()
    -- | Predeclare an access pattern for file data.
    --
    -- This has different effects on different distributions.
    -- * [Linux]: perform @posix_fadvise(2).
    -- * [MacOS]: no-op.
    -- * [Windows]: no-op.
  , hAdvise :: Handle h -> FileOffset -> FileOffset -> Advice -> m ()
    -- | Allocate file space.
    --
    -- This has different effects on different distributions.
    -- * [Linux]: perform @posix_fallocate(2).
    -- * [MacOS]: no-op.
    -- * [Windows]: no-op.
  , hAllocate :: Handle h -> FileOffset -> FileOffset -> m ()
  }

instance NFData (HasBlockIO m h) where
  rnf (HasBlockIO a b c d e) =
      rwhnf a `seq` rwhnf b `seq` rnf c `seq`
      rwhnf d `seq` rwhnf e

-- | Concurrency parameters for initialising a 'HasBlockIO. Can be ignored by
-- serial implementations.
data IOCtxParams = IOCtxParams {
                     ioctxBatchSizeLimit   :: !Int,
                     ioctxConcurrencyLimit :: !Int
                   }

instance NFData IOCtxParams where
  rnf (IOCtxParams x y) = rnf x `seq` rnf y

defaultIOCtxParams :: IOCtxParams
defaultIOCtxParams = IOCtxParams {
      ioctxBatchSizeLimit   = 64,
      ioctxConcurrencyLimit = 64 * 3
    }

mkClosedError :: HasCallStack => SomeHasFS m -> String -> FsError
mkClosedError (SomeHasFS hasFS) loc = ioToFsError (mkFsErrorPath hasFS (mkFsPath [])) ioerr
  where ioerr =
          ioeSetErrorString
            (mkIOError ResourceVanished loc Nothing Nothing)
            ("HasBlockIO closed: " <> loc)


data IOOp s h =
    IOOpRead  !(Handle h) !FileOffset !(MutableByteArray s) !BufferOffset !ByteCount
  | IOOpWrite !(Handle h) !FileOffset !(MutableByteArray s) !BufferOffset !ByteCount

instance NFData (IOOp s h) where
  rnf = rwhnf

ioopHandle :: IOOp s h -> Handle h
ioopHandle (IOOpRead h _ _ _ _)  = h
ioopHandle (IOOpWrite h _ _ _ _) = h

ioopFileOffset :: IOOp s h -> FileOffset
ioopFileOffset (IOOpRead _ off _ _ _)  = off
ioopFileOffset (IOOpWrite _ off _ _ _) = off

ioopBuffer :: IOOp s h -> MutableByteArray s
ioopBuffer (IOOpRead _ _ buf _ _)  = buf
ioopBuffer (IOOpWrite _ _ buf _ _) = buf

ioopBufferOffset :: IOOp s h -> BufferOffset
ioopBufferOffset (IOOpRead _ _ _ bufOff _)  = bufOff
ioopBufferOffset (IOOpWrite _ _ _ bufOff _) = bufOff

ioopByteCount :: IOOp s h -> ByteCount
ioopByteCount (IOOpRead _ _ _ _ c)  = c
ioopByteCount (IOOpWrite _ _ _ _ c) = c

-- | Number of read/written bytes.
newtype IOResult = IOResult ByteCount
  deriving newtype VP.Prim

newtype instance VUM.MVector s IOResult = MV_IOResult (VP.MVector s IOResult)
newtype instance VU.Vector     IOResult = V_IOResult  (VP.Vector    IOResult)

deriving via (VU.UnboxViaPrim IOResult) instance VGM.MVector VU.MVector IOResult
deriving via (VU.UnboxViaPrim IOResult) instance VG.Vector   VU.Vector  IOResult

instance VUM.Unbox IOResult

-- | Basically "System.Posix.Fcntl.Advice" from the @unix@ package
data Advice =
    AdviceNormal
  | AdviceRandom
  | AdviceSequential
  | AdviceWillNeed
  | AdviceDontNeed
  | AdviceNoReuse
  deriving stock (Show, Eq)

-- | Apply 'Advice' to all bytes of a file, which is referenced by a 'Handle'.
hAdviseAll :: HasBlockIO m h -> Handle h -> Advice -> m ()
hAdviseAll hbio h advice = hAdvise hbio h 0 0 advice -- len=0 implies until the end of file

-- | Drop the full file referenced by a 'Handle' from the OS page cache, if
-- present.
hDropCacheAll :: HasBlockIO m h -> Handle h -> m ()
hDropCacheAll hbio h = hAdviseAll hbio h AdviceDontNeed
