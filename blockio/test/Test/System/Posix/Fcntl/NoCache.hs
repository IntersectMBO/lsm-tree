{-# LANGUAGE CPP #-}
module Test.System.Posix.Fcntl.NoCache (tests) where

import           Test.Tasty (TestTree, testGroup)

#ifdef mingw32_HOST_OS
#else

import qualified GHC.IO.FD as GHC (FD (..))
import qualified GHC.IO.Handle.FD as GHC (handleToFd)
import           System.IO (IOMode (..), withFile)
import           System.Posix.Fcntl.NoCache (readFcntlNoCache,
                     writeFcntlNoCache)
import           System.Posix.Types (Fd (..))
import           Test.Tasty.HUnit (testCaseSteps, (@?=))

#endif

tests :: TestTree

#ifdef mingw32_HOST_OS

tests = testGroup "Test.System.Posix.Fcntl.NoCache" []

#else

tests = testGroup "Test.System.Posix.Fcntl.NoCache"
    [ testCaseSteps "basic-read" $ \info -> do
        withFile "blockio.cabal" ReadMode $ \hdl -> do
            GHC.FD {GHC.fdFD = fd} <- GHC.handleToFd hdl
            noCache <- readFcntlNoCache (Fd fd)
            info $ show noCache
            noCache @?= False

    , testCaseSteps "write-read" $ \info -> do
        withFile "blockio.cabal" ReadMode $ \hdl -> do
            GHC.FD {GHC.fdFD = fd} <- GHC.handleToFd hdl
            writeFcntlNoCache (Fd fd) True
            noCache <- readFcntlNoCache (Fd fd)
            info $ show noCache
#ifdef darwin_HOST_OS
            -- on darwin the readFcntlNoCache is always returning False
            noCache @?= False
#else
            noCache @?= True
#endif
    ]

#endif
