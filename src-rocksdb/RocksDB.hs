{-# LANGUAGE CPP #-}
module RocksDB (
    -- * Options
    Options,
    withOptions,
    optionsSetCreateIfMissing,
    optionsSetMaxOpenFiles,
    optionsSetCompression,
    optionsIncreaseParallelism,
    optionsOptimizeLevelStyleCompaction,
    -- * Read Options
    ReadOptions,
    withReadOptions,
    -- * Write options
    WriteOptions,
    withWriteOptions,
    writeOptionsDisableWAL,
    -- * DB operations
    RocksDB,
    withRocksDB,
    put,
    get,
    multiGet,
    delete,
    write,
    checkpoint,
    -- * Write batch
    WriteBatch,
    withWriteBatch,
    writeBatchPut,
    writeBatchDelete,
    -- * Block based table options
    BlockTableOptions,
    withBlockTableOptions,
    blockBasedOptionsSetFilterPolicy,
    optionsSetBlockBasedTableFactory,
    -- * Filter policy
    FilterPolicy,
    withFilterPolicyBloom,
) where

import           Control.Exception (bracket)
import           Control.Monad (forM)
import           Data.ByteString (ByteString)
import           Data.ByteString.Unsafe (unsafePackMallocCStringLen,
                     unsafeUseAsCStringLen)
import           Data.Foldable.WithIndex (ifor_)
import           Data.Word (Word64)
import           Foreign.C.String (peekCString, withCString)
import           Foreign.C.Types (CInt, CSize)
import           Foreign.Marshal.Alloc (alloca, free)
import           Foreign.Marshal.Array (allocaArray)
import           Foreign.Marshal.Utils (withMany)
import           Foreign.Ptr (Ptr, nullPtr)
import           Foreign.Storable (peek, peekElemOff, poke, pokeElemOff)

import           RocksDB.FFI

#if MIN_VERSION_base(4,18,0)
import           Foreign.C.ConstPtr (ConstPtr (..))
#endif

-------------------------------------------------------------------------------
-- constptr
-------------------------------------------------------------------------------

#if MIN_VERSION_base(4,18,0)
constPtr :: Ptr a -> ConstPtr a
constPtr = ConstPtr
#else
constPtr :: Ptr a -> Ptr a
constPtr = id
#endif

-------------------------------------------------------------------------------
-- error
-------------------------------------------------------------------------------

withErrPtr :: (ErrPtr -> IO r) -> IO r
withErrPtr kont = alloca $ \ptr -> do
    poke ptr nullPtr
    x <- kont ptr
    ptr' <- peek ptr
    if ptr' == nullPtr
    then return ()
    else free ptr'
    return x

assertErrPtr :: String -> ErrPtr -> IO ()
assertErrPtr fun ptr = do
    ptr' <- peek ptr
    if ptr' == nullPtr
    then return ()
    else do
        msg <- peekCString ptr'
        fail $ fun ++ ": " ++ msg

-------------------------------------------------------------------------------
-- options
-------------------------------------------------------------------------------

newtype Options = Options (Ptr OPTIONS)

withOptions :: (Options -> IO r) -> IO r
withOptions kont = bracket
    rocksdb_options_create
    rocksdb_options_destroy
    (\ptr -> kont (Options ptr))

optionsSetCreateIfMissing :: Options -> Bool -> IO ()
optionsSetCreateIfMissing (Options ptr) v =
    rocksdb_options_set_create_if_missing ptr (if v then 1 else 0)

optionsIncreaseParallelism :: Options -> Int -> IO ()
optionsIncreaseParallelism (Options ptr) v =
    rocksdb_options_increase_parallelism ptr (intToCInt v)

optionsSetMaxOpenFiles :: Options -> Int -> IO ()
optionsSetMaxOpenFiles (Options ptr) v =
    rocksdb_options_set_max_open_files ptr (intToCInt v)

optionsOptimizeLevelStyleCompaction :: Options -> Word64 -> IO ()
optionsOptimizeLevelStyleCompaction (Options ptr) v =
    rocksdb_options_optimize_level_style_compaction ptr v

optionsSetCompression :: Options -> Int -> IO ()
optionsSetCompression (Options ptr) v =
    rocksdb_options_set_compression ptr (intToCInt v)

-------------------------------------------------------------------------------
-- read options
-------------------------------------------------------------------------------

newtype ReadOptions = ReadOptions (Ptr READOPTIONS)

withReadOptions :: (ReadOptions -> IO r) -> IO r
withReadOptions kont = bracket
    rocksdb_readoptions_create
    rocksdb_readoptions_destroy
    (\ptr -> kont (ReadOptions ptr))

-------------------------------------------------------------------------------
-- write options
-------------------------------------------------------------------------------

newtype WriteOptions = WriteOptions (Ptr WRITEOPTIONS)

withWriteOptions :: (WriteOptions -> IO r) -> IO r
withWriteOptions kont = bracket
    rocksdb_writeoptions_create
    rocksdb_writeoptions_destroy
    (\ptr -> kont (WriteOptions ptr))

writeOptionsDisableWAL :: WriteOptions -> Bool {- ^ Disable -} -> IO ()
writeOptionsDisableWAL (WriteOptions opts) disable =
    rocksdb_writeoptions_disable_WAL opts (if disable then 1 else 0)

-------------------------------------------------------------------------------
-- db operations
-------------------------------------------------------------------------------

data RocksDB = RocksDB (Ptr DB) ErrPtr

withRocksDB :: Options -> FilePath -> (RocksDB -> IO r) -> IO r
withRocksDB (Options opt) path kont =
    withCString path $ \path' ->
    withErrPtr $ \errptr ->
    bracket (rocksdb_open' path' errptr) rocksdb_close $ \ptr ->
    kont (RocksDB ptr errptr)
  where
    rocksdb_open' path' errptr = do
        ptr <- rocksdb_open opt path' errptr
        assertErrPtr "rocksdb_open" errptr
        return ptr

put :: RocksDB -> WriteOptions -> ByteString -> ByteString -> IO ()
put (RocksDB dbPtr errPtr) (WriteOptions opts) key val =
    unsafeUseAsCStringLen key $ \(kp, kl) ->
    unsafeUseAsCStringLen val $ \(vp, vl) -> do
        rocksdb_put dbPtr opts kp (intToCSize kl) vp (intToCSize vl) errPtr
        assertErrPtr "rocksdb_put" errPtr

get :: RocksDB -> ReadOptions -> ByteString -> IO (Maybe ByteString)
get (RocksDB dbPtr errPtr) (ReadOptions opts) key =
    unsafeUseAsCStringLen key $ \(kp, kl) ->
    alloca $ \vlPtr -> do
        vp <- rocksdb_get dbPtr opts kp (intToCSize kl) vlPtr errPtr
        assertErrPtr "rocksdb_get" errPtr -- TODO: may leak
        vl <- peek vlPtr
        if vp == nullPtr
        then return Nothing
        else Just <$> unsafePackMallocCStringLen (vp, csizeToInt vl)

multiGet :: RocksDB -> ReadOptions -> [ByteString] -> IO [Maybe ByteString]
multiGet (RocksDB db _errPtr) (ReadOptions opts) keys =
    allocaArray n $ \kps ->
    allocaArray n $ \kls ->
    allocaArray n $ \vps ->
    allocaArray n $ \vls ->
    allocaArray n $ \errs ->
    withMany unsafeUseAsCStringLen keys $ \keys' -> do
        -- populate keys
        ifor_ keys' $ \i (kp, kl) -> do
            pokeElemOff kps i (constPtr kp)
            pokeElemOff kls i (intToCSize kl)

        -- multi get
        rocksdb_multi_get db opts (intToCSize n) kps kls vps vls errs

        -- read keys
        forM [ 0 .. n - 1 ] $ \i -> do
            vp <- peekElemOff vps i
            vl <- peekElemOff vls i

            -- TODO: we don't check errs here. we should.

            if vp == nullPtr
            then return Nothing
            else Just <$> unsafePackMallocCStringLen (vp, csizeToInt vl)

  where
    n = length keys

delete :: RocksDB -> WriteOptions -> ByteString -> IO ()
delete (RocksDB dbPtr errPtr) (WriteOptions opts) key =
    unsafeUseAsCStringLen key $ \(kp, kl) -> do
        rocksdb_delete dbPtr opts kp (intToCSize kl) errPtr
        assertErrPtr "rocksdb_delete" errPtr

write :: RocksDB -> WriteOptions -> WriteBatch -> IO ()
write (RocksDB dbPtr errPtr) (WriteOptions opts) (WriteBatch batch) = do
    rocksdb_write dbPtr opts batch errPtr
    assertErrPtr "rocksdb_write" errPtr

checkpoint :: RocksDB -> FilePath -> IO ()
checkpoint (RocksDB dbPtr errPtr) path =
    withCString path $ \path' ->
    bracket rocksdb_checkpoint_object_create' rocksdb_checkpoint_object_destroy $ \cp -> do
        rocksdb_checkpoint_create cp path' 0 errPtr
        assertErrPtr "rocksdb_checkpoint_create" errPtr
  where
    rocksdb_checkpoint_object_create' = do
        cp <- rocksdb_checkpoint_object_create dbPtr errPtr
        assertErrPtr "rocksdb_checkpoint_object_create" errPtr
        return cp

-------------------------------------------------------------------------------
-- write batch
-------------------------------------------------------------------------------

newtype WriteBatch = WriteBatch (Ptr WRITEBATCH)

withWriteBatch :: (WriteBatch -> IO r) -> IO r
withWriteBatch kont = bracket
    rocksdb_writebatch_create
    rocksdb_writebatch_destroy
    (\ptr -> kont (WriteBatch ptr))

writeBatchPut :: WriteBatch -> ByteString -> ByteString -> IO ()
writeBatchPut (WriteBatch batch) key val =
    unsafeUseAsCStringLen key $ \(kp, kl) ->
    unsafeUseAsCStringLen val $ \(vp, vl) ->
        rocksdb_writebatch_put batch kp (intToCSize kl) vp (intToCSize vl)

writeBatchDelete :: WriteBatch -> ByteString -> IO ()
writeBatchDelete (WriteBatch batch) key =
    unsafeUseAsCStringLen key $ \(kp, kl) ->
        rocksdb_writebatch_delete batch kp (intToCSize kl)

-------------------------------------------------------------------------------
-- block based table options
-------------------------------------------------------------------------------

newtype BlockTableOptions = BlockTableOptions (Ptr BLOCKTABLEOPTIONS)

withBlockTableOptions :: (BlockTableOptions -> IO r) -> IO r
withBlockTableOptions kont = bracket
    rocksdb_block_based_options_create
    rocksdb_block_based_options_destroy
    (\ptr -> kont (BlockTableOptions ptr))

blockBasedOptionsSetFilterPolicy :: BlockTableOptions -> FilterPolicy -> IO ()
blockBasedOptionsSetFilterPolicy (BlockTableOptions opts) (FilterPolicy policy) =
    rocksdb_block_based_options_set_filter_policy opts policy

optionsSetBlockBasedTableFactory :: Options -> BlockTableOptions -> IO ()
optionsSetBlockBasedTableFactory (Options opts) (BlockTableOptions ptr) =
    rocksdb_options_set_block_based_table_factory opts ptr

-------------------------------------------------------------------------------
-- filter policy
-------------------------------------------------------------------------------

newtype FilterPolicy = FilterPolicy (Ptr FILTERPOLICY)

withFilterPolicyBloom :: Int -> (FilterPolicy -> IO r) -> IO r
withFilterPolicyBloom bits_per_key kont = bracket
    (rocksdb_filterpolicy_create_bloom (intToCInt bits_per_key))
    (\_ -> return ()) -- rocksdb_filterpolicy_destroy, causes segfault if we free it.
    (\ptr -> kont (FilterPolicy ptr))

-------------------------------------------------------------------------------
-- utils
-------------------------------------------------------------------------------

intToCSize :: Int -> CSize
intToCSize = fromIntegral

csizeToInt :: CSize -> Int
csizeToInt = fromIntegral

intToCInt :: Int -> CInt
intToCInt = fromIntegral
