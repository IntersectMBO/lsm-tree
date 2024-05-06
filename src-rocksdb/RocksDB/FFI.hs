{-# LANGUAGE CApiFFI #-}
{-# LANGUAGE CPP     #-}
module RocksDB.FFI (
  -- * options
  OPTIONS,
  rocksdb_options_create,
  rocksdb_options_destroy,
  rocksdb_options_set_create_if_missing,
  rocksdb_options_set_max_open_files,
  rocksdb_options_increase_parallelism,
  rocksdb_options_optimize_level_style_compaction,
  rocksdb_options_set_compression,
  -- * read options
  READOPTIONS,
  rocksdb_readoptions_create,
  rocksdb_readoptions_destroy,
  -- * write options
  WRITEOPTIONS,
  rocksdb_writeoptions_create,
  rocksdb_writeoptions_destroy,
  rocksdb_writeoptions_disable_WAL,
  -- * db operations
  DB,
  rocksdb_open,
  rocksdb_close,
  rocksdb_get,
  rocksdb_multi_get,
  rocksdb_put,
  rocksdb_delete,
  rocksdb_write,
  -- ** checkpoints
  CHECKPOINT,
  rocksdb_checkpoint_object_create,
  rocksdb_checkpoint_object_destroy,
  rocksdb_checkpoint_create,
  -- * write batch
  WRITEBATCH,
  rocksdb_writebatch_create,
  rocksdb_writebatch_destroy,
  rocksdb_writebatch_put,
  rocksdb_writebatch_delete,
  -- * block table options
  BLOCKTABLEOPTIONS,
  rocksdb_block_based_options_create,
  rocksdb_block_based_options_destroy,
  rocksdb_block_based_options_set_filter_policy,
  rocksdb_options_set_block_based_table_factory,
  -- * filter policy
  FILTERPOLICY,
  rocksdb_filterpolicy_destroy,
  rocksdb_filterpolicy_create_bloom,
  -- * error
  ErrPtr,
) where

import           Data.Word (Word64)
import           Foreign.C.String (CString)
import           Foreign.C.Types (CChar, CInt (..), CSize (..), CUChar (..))
import           Foreign.Ptr (Ptr)

#if MIN_VERSION_base(4,18,0)
import           Foreign.C.ConstPtr (ConstPtr (..))
#endif

-------------------------------------------------------------------------------
-- error
-------------------------------------------------------------------------------

type ErrPtr = Ptr CString

-------------------------------------------------------------------------------
-- options
-------------------------------------------------------------------------------

data {-# CTYPE "rocksdb/c.h" "rocksdb_options_t" #-} OPTIONS

foreign import capi "rocksdb/c.h rocksdb_options_create"
  rocksdb_options_create :: IO (Ptr OPTIONS)

foreign import capi "rocksdb/c.h rocksdb_options_destroy"
  rocksdb_options_destroy :: Ptr OPTIONS -> IO ()

foreign import capi "rocksdb/c.h rocksdb_options_increase_parallelism"
  rocksdb_options_increase_parallelism :: Ptr OPTIONS -> CInt -> IO ()

foreign import capi "rocksdb/c.h rocksdb_options_optimize_level_style_compaction"
  rocksdb_options_optimize_level_style_compaction :: Ptr OPTIONS -> Word64 -> IO ()

foreign import capi "rocksdb/c.h rocksdb_options_set_create_if_missing"
  rocksdb_options_set_create_if_missing :: Ptr OPTIONS -> CUChar -> IO ()

foreign import capi "rocksdb/c.h rocksdb_options_set_max_open_files"
  rocksdb_options_set_max_open_files :: Ptr OPTIONS -> CInt -> IO ()

foreign import capi "rocksdb/c.h rocksdb_options_set_compression"
  rocksdb_options_set_compression :: Ptr OPTIONS -> CInt -> IO ()

-------------------------------------------------------------------------------
-- read options
-------------------------------------------------------------------------------

data {-# CTYPE "rocksdb/c.h" "rocksdb_readoptions_t" #-} READOPTIONS

foreign import capi "rocksdb/c.h rocksdb_readoptions_create"
  rocksdb_readoptions_create :: IO (Ptr READOPTIONS)

foreign import capi "rocksdb/c.h rocksdb_readoptions_destroy"
  rocksdb_readoptions_destroy :: Ptr READOPTIONS -> IO ()

-------------------------------------------------------------------------------
-- write options
-------------------------------------------------------------------------------

data {-# CTYPE "rocksdb/c.h" "rocksdb_writeoptions_t" #-} WRITEOPTIONS

foreign import capi "rocksdb/c.h rocksdb_writeoptions_create"
  rocksdb_writeoptions_create :: IO (Ptr WRITEOPTIONS)

foreign import capi "rocksdb/c.h rocksdb_writeoptions_destroy"
  rocksdb_writeoptions_destroy :: Ptr WRITEOPTIONS -> IO ()

foreign import capi "rocksdb/c.h rocksdb_writeoptions_disable_WAL"
  rocksdb_writeoptions_disable_WAL :: Ptr WRITEOPTIONS -> CInt -> IO ()

-------------------------------------------------------------------------------
-- db operations
-------------------------------------------------------------------------------

data {-# CTYPE "rocksdb/c.h" "rocksdb_t" #-} DB

foreign import capi "rocksdb/c.h rocksdb_open"
  rocksdb_open :: Ptr OPTIONS -> CString -> ErrPtr -> IO (Ptr DB)

foreign import capi "rocksdb/c.h rocksdb_close"
  rocksdb_close :: Ptr DB -> IO ()

-- | Returns NULL if not found.  A malloc()ed array otherwise.
-- Stores the length of the array in *vallen.
foreign import capi "rocksdb/c.h rocksdb_get"
  rocksdb_get :: Ptr DB -> Ptr READOPTIONS
    -> CString -> CSize
    -> Ptr CSize
    -> ErrPtr
    -> IO CString

#if MIN_VERSION_base(4,18,0)
foreign import capi "rocksdb/c.h rocksdb_multi_get"
  rocksdb_multi_get :: Ptr DB -> Ptr READOPTIONS
    -> CSize
    -> Ptr (ConstPtr CChar) -> Ptr CSize
    -> Ptr CString -> Ptr CSize
    -> Ptr CString
    -> IO ()
#else
foreign import ccall "rocksdb/c.h rocksdb_multi_get"
  rocksdb_multi_get :: Ptr DB -> Ptr READOPTIONS
    -> CSize
    -> Ptr (Ptr CChar) -> Ptr CSize
    -> Ptr CString -> Ptr CSize
    -> Ptr CString
    -> IO ()
#endif

foreign import capi "rocksdb/c.h rocksdb_put"
  rocksdb_put :: Ptr DB -> Ptr WRITEOPTIONS
    -> CString -> CSize
    -> CString -> CSize
    -> ErrPtr
    -> IO ()

foreign import capi "rocksdb/c.h rocksdb_delete"
  rocksdb_delete :: Ptr DB -> Ptr WRITEOPTIONS
    -> CString -> CSize
    -> ErrPtr
    -> IO ()

foreign import capi "rocksdb/c.h rocksdb_write"
  rocksdb_write :: Ptr DB -> Ptr WRITEOPTIONS
    -> Ptr WRITEBATCH
    -> ErrPtr
    -> IO ()

data {-# CTYPE "rocksdb/c.h" "rocksdb_checkpoint_t" #-} CHECKPOINT

foreign import capi "rocksdb/c.h rocksdb_checkpoint_object_create"
  rocksdb_checkpoint_object_create :: Ptr DB -> ErrPtr -> IO (Ptr CHECKPOINT)

foreign import capi "rocksdb/c.h rocksdb_checkpoint_object_destroy"
  rocksdb_checkpoint_object_destroy :: Ptr CHECKPOINT -> IO ()

foreign import capi "rocksdb/c.h rocksdb_checkpoint_create"
  rocksdb_checkpoint_create :: Ptr CHECKPOINT -> CString -> Word64 -> ErrPtr -> IO ()

-------------------------------------------------------------------------------
-- write batch
-------------------------------------------------------------------------------

data {-# CTYPE "rocksdb/c.h" "rocksdb_writebatch_t" #-} WRITEBATCH

foreign import capi "rocksdb/c.h rocksdb_writebatch_create"
  rocksdb_writebatch_create :: IO (Ptr WRITEBATCH)

foreign import capi "rocksdb/c.h rocksdb_writebatch_destroy"
  rocksdb_writebatch_destroy :: Ptr WRITEBATCH -> IO ()

foreign import capi "rocksdb/c.h rocksdb_writebatch_put"
  rocksdb_writebatch_put :: Ptr WRITEBATCH
    -> CString -> CSize
    -> CString -> CSize
    -> IO ()

foreign import capi "rocksdb/c.h rocksdb_writebatch_delete"
  rocksdb_writebatch_delete :: Ptr WRITEBATCH
    -> CString -> CSize
    -> IO ()

-------------------------------------------------------------------------------
-- block based table options
-------------------------------------------------------------------------------

data {-# CTYPE "rocksdb/c.h" "rocksdb_block_based_table_options_t" #-} BLOCKTABLEOPTIONS

foreign import capi "rocksdb/c.h rocksdb_block_based_options_create"
  rocksdb_block_based_options_create :: IO (Ptr BLOCKTABLEOPTIONS)

foreign import capi "rocksdb/c.h rocksdb_block_based_options_destroy"
  rocksdb_block_based_options_destroy :: Ptr BLOCKTABLEOPTIONS -> IO ()

foreign import capi "rocksdb/c.h rocksdb_block_based_options_set_filter_policy"
  rocksdb_block_based_options_set_filter_policy :: Ptr BLOCKTABLEOPTIONS -> Ptr FILTERPOLICY -> IO ()

foreign import capi "rocksdb/c.h rocksdb_options_set_block_based_table_factory"
  rocksdb_options_set_block_based_table_factory :: Ptr OPTIONS -> Ptr BLOCKTABLEOPTIONS -> IO ()

-------------------------------------------------------------------------------
-- filter policy
-------------------------------------------------------------------------------

data {-# CTYPE "rocksdb/c.h" "rocksdb_filterpolicy_t" #-} FILTERPOLICY

foreign import capi "rocksdb/c.h rocksdb_filterpolicy_destroy"
  rocksdb_filterpolicy_destroy :: Ptr FILTERPOLICY -> IO ()

foreign import capi "rocksdb/c.h rocksdb_filterpolicy_create_bloom"
  rocksdb_filterpolicy_create_bloom :: CInt -> IO (Ptr FILTERPOLICY)
