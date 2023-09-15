{-# LANGUAGE RoleAnnotations     #-}
{-# LANGUAGE ScopedTypeVariables #-}
{-# LANGUAGE StandaloneDeriving  #-}
{-# LANGUAGE TupleSections       #-}
{-# LANGUAGE TypeApplications    #-}
{-# LANGUAGE TypeFamilies        #-}
module Database.LSMTree.Model.Common (
    SomeSerialisationConstraint (..)
  , SomeUpdateConstraint (..)
  ) where

import qualified Data.ByteString as BS

class SomeSerialisationConstraint a where
    serialise :: a -> BS.ByteString

    -- Note: cannot fail.
    deserialise :: BS.ByteString -> a

instance SomeSerialisationConstraint BS.ByteString where
    serialise = id
    deserialise = id

class SomeUpdateConstraint a where
    merge :: a -> a -> a

instance SomeUpdateConstraint BS.ByteString where
    merge = (<>)
