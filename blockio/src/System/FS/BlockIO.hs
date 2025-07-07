module System.FS.BlockIO (
    -- * Description
    -- $description

    -- * Re-exports
    module System.FS.BlockIO.API
  , module System.FS.BlockIO.IO

    -- * Example
    -- $example
) where

import           System.FS.BlockIO.API
import           System.FS.BlockIO.IO

{-------------------------------------------------------------------------------
  Examples
-------------------------------------------------------------------------------}

{- $description

  The 'HasBlockIO' record type defines an /abstract interface/. A value of a
  'HasBlockIO' type is what we call an /instance/ of the abstract interface, and
  an instance is produced by a function that we call an /implementation/. In
  principle, we can have multiple instances of the same implementation.

  There are currently two known implementations of the interface:

  * An implementation using the real file system, which can be found in the
    "System.FS.BlockIO.IO" module. This implementation is platform-dependent,
    but has largely similar observable behaviour.

  * An implementation using a simulated file system, which can be found in the
    @System.FS.BlockIO.Sim@ module of the @blockio:sim@ sublibrary. This
    implementation is uniform across platforms.

  The 'HasBlockIO' abstract interface is an extension of the 'HasFS' abstract
  interface that is provided by the
  [@fs-api@](https://hackage.haskell.org/package/fs-api) package. Whereas
  'HasFS' defines many primitive functions, for example for opening a file, the
  main feature of 'HasBlockIO' is to define a function for performing batched
  I\/O. As such, users of @blockio@ will more often than not need to pass both a
  'HasFS' and a 'HasBlockIO' instance to their functions.
-}

{- $example

  >>> import Control.Monad
  >>> import Control.Monad.Primitive
  >>> import Data.Primitive.ByteArray
  >>> import Data.Vector qualified as V
  >>> import Data.Word
  >>> import Debug.Trace
  >>> import System.FS.API as FS
  >>> import System.FS.BlockIO.IO
  >>> import System.FS.BlockIO.API
  >>> import System.FS.IO

  The main feature of the 'HasBlockIO' abstract interface is that it provides a
  function for performing batched I\/O using 'submitIO'. Depending on the
  implementation of the interface, performing I\/O in batches concurrently using
  'submitIO' can be much faster than performing each I\/O operation in a
  sequential order. We will not go into detail about this performance
  consideration here, but more information can be found in the
  "System.FS.BlockIO.IO" module. Instead, we will show an example of how
  'submitIO' can be used in your own projects.

  We aim to build an example that writes some contents to a file using
  'submitIO', and then reads the contents out again using 'submitIO'. The file
  contents will simply be bytes.

  >>> type Byte = Word8

  The first part of the example is to write out bytes to a given file path using
  'submitIO'. We define a @writeFile@ function that does just that. The file is
  assumed to not exist already.

  The bytes, which are provided as separate bytes, are written into a buffer (a
  mutable byte array). Note that the buffer should be /pinned/ memory to prevent
  pointer aliasing. In the case of write operations, this buffer is used to
  communicate to the backend what the bytes are that should be written to disk.
  For simplicity, we create a separate 'IOOpWrite' instruction for each byte.
  This instruction requires information about the write operation. In order of
  appearence these are: the file handle to write bytes to, the offset into that
  file, the buffer, the offset into that buffer, and the number of bytes to
  write. Finally, all instructions are batched together and submitted in one go
  using 'submitIO'. For each instruction, an 'IOResult' is returned, which
  describes in this case the number of written bytes. If any of the instructions
  failed to be performed, an error is thrown. We print the 'IOResult's to
  standard output.

  Note that in real scenarios it would be much more performant to aggregate the
  bytes into larger chunks, and to create an instruction for each of those
  chunks. A sensible size for those chunks would be the disk page size (4Kb for
  example), or a multiple of that disk page size. The disk page size is
  typically the smallest chunk of memory that can be written to or read from the
  disk. In some cases it is also desirable or even required that the buffers are
  aligned to the disk page size. For example, alignment is required when using
  direct I\/O.

  >>> :{
  writeFile ::
       HasFS IO HandleIO
    -> HasBlockIO IO HandleIO
    -> FsPath
    -> [Byte]
    -> IO ()
  writeFile hasFS hasBlockIO file bytes = do
      let numBytes = length bytes
      FS.withFile hasFS file (FS.WriteMode FS.MustBeNew) $ \h -> do
        buffer <- newPinnedByteArray numBytes
        forM_ (zip [0..] bytes) $ \(i, byte) ->
          let bufferOffset = fromIntegral i
          in  writeByteArray @Byte buffer bufferOffset byte
        results <- submitIO hasBlockIO $ V.fromList [
            IOOpWrite h fileOffset buffer bufferOffset 1
          | i <- take numBytes [0..]
          , let fileOffset = fromIntegral i
                bufferOffset = FS.BufferOffset i
          ]
        print results
  :}

  The second part of the example is to read a given number of bytes from a given
  file path using 'submitIO'. We define a @readFile@ function that follows the
  same general structure and behaviour as @writeFile@, but @readFile@ is of
  course reading bytes instead of writing bytes.

  >>> :{
  readFile ::
       HasFS IO HandleIO
    -> HasBlockIO IO HandleIO
    -> FsPath
    -> Int
    -> IO [Byte]
  readFile hasFS hasBlockIO file numBytes = do
      FS.withFile hasFS file FS.ReadMode $ \h -> do
        buffer <- newPinnedByteArray numBytes
        results <- submitIO hasBlockIO $ V.fromList [
            IOOpRead h fileOffset buffer bufferOffset numBytes
          | i <- [0..3]
          , let fileOffset = fromIntegral i
                bufferOffset = FS.BufferOffset i
                numBytes = 1
          ]
        print results
        forM (take numBytes [0..]) $ \i ->
          let bufferOffset = i
          in  readByteArray @Byte buffer i
  :}

  Now we can combine @writeFile@ and @readFile@ into a very small example called
  @writeReadFile@, which does what we set out to do: write a few bytes to a
  (temporary) file and read them out again using 'submitIO'. We also print the
  bytes that were written and the bytes that were read, so that the user can
  check by hand whether the bytes match.

  >>> :{
  writeReadFile :: HasFS IO HandleIO -> HasBlockIO IO HandleIO -> IO ()
  writeReadFile hasFS hasBlockIO = do
      let file = FS.mkFsPath ["simple_example.txt"]
      let bytesIn = [1,2,3,4]
      print bytesIn
      writeFile hasFS hasBlockIO file bytesIn
      bytesOut <- readFile hasFS hasBlockIO file 4
      print bytesOut
      FS.removeFile hasFS file
  :}

  In order to run @writeReadFile@, we will need 'HasFS' and 'HasBlockIO'
  instances. This is where the separation between interface and implementation
  shines: @writeReadFile@ is agnostic to the implementations of the the abstract
  interfaces, so we could pick any implementations and slot them in. For this
  example we will use the /real/ implementation from "System.FS.BlockIO.IO", but
  we could have used the /simulated/ implementation from the @blockio:sim@
  sub-library just as well. We define the @example@ function, which uses
  'withIOHasBlockIO' to instantiate both a 'HasFS' and 'HasBlockIO' instance,
  which we pass to 'writeReadFile'.

  >>> :{
    example :: IO ()
    example =
      withIOHasBlockIO (MountPoint "") defaultIOCtxParams $ \hasFS hasBlockIO ->
        writeReadFile hasFS hasBlockIO
  :}

  Finally, we can run the example to produce some output. As we can see, the
  input bytes match the output bytes.

  >>> example
  [1,2,3,4]
  [IOResult 1,IOResult 1,IOResult 1,IOResult 1]
  [IOResult 1,IOResult 1,IOResult 1,IOResult 1]
  [1,2,3,4]
-}
