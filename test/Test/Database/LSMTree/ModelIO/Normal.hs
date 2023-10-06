{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE TypeApplications    #-}
module Test.Database.LSMTree.ModelIO.Normal (tests) where

import qualified Data.ByteString as BS
import           Data.Functor (void)
import           Data.Proxy (Proxy (..))
import           Database.LSMTree.ModelIO.Normal (LookupResult (..),
                     TableHandle, Update (..))
import           Test.Database.LSMTree.ModelIO.Class
import           Test.Tasty (TestTree, testGroup)
import           Test.Tasty.QuickCheck
import           Test.Util.Orphans ()

tests :: TestTree
tests = testGroup "Database.LSMTree.ModelIO.Normal"
    [ testProperty "lookup-insert" $ prop_lookupInsert tbl
    ]
  where
    tbl = Proxy :: Proxy TableHandle

type Key = BS.ByteString
type Value = BS.ByteString
type Blob = BS.ByteString

-------------------------------------------------------------------------------
-- implement classic QC tests for basic k/v properties
-------------------------------------------------------------------------------

-- | You can lookup what you inserted.
prop_lookupInsert ::
     forall h. IsTableHandle h
  => Proxy h
  -> Key  -> Value -> [(Key, Update Value Blob)] -> Property
prop_lookupInsert h k v ups = ioProperty $ do
    -- create session, table handle, and populate it with some data.
    s <- newSession
    hdl <- new @h s (testTableConfig h)
    updates ups hdl

    -- the main dish
    inserts [(k, v, Nothing)] hdl
    res <- lookups [k] hdl

    -- void makes blobrefs into ()
    return $ fmap void res === [Found k v]

-- TODO: blobrefs.
