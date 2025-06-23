-- | This is an internal module that has to be exposed for technical reasons. Do
-- not use it.
module System.FS.BlockIO.Serial.Internal (
    -- We have to re-export from somewhere so that the @blockio:sim@ sub-library
    -- can use it. Unfortunately, this makes the function part of the public API
    -- even though we'd prefer to keep it truly hidden. There are ways around
    -- this, for example using a new private sub-library that contains the
    -- "System.FS.BlockIO.Serial" module, but it's a lot of boilerplate.
    serialHasBlockIO
  ) where

import           System.FS.BlockIO.Serial (serialHasBlockIO)
