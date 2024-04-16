{-# LANGUAGE BangPatterns        #-}
{-# LANGUAGE NoFieldSelectors    #-}
{-# LANGUAGE OverloadedRecordDot #-}
{-# LANGUAGE OverloadedStrings   #-}
module Main (main) where

import           Control.Applicative ((<**>))
import qualified Crypto.Hash.SHA256 as SHA256
import qualified Data.Binary as B
import qualified Data.ByteString as BS
import           Data.Foldable (foldl')
import qualified Data.Primitive as P
import           Data.Word (Word64)
import qualified Database.LSMTree.Internal.Normal as N
import qualified Database.LSMTree.Internal.Run as Run
import qualified Database.LSMTree.Internal.WriteBuffer as WB
import           Database.LSMTree.Orphans ()
import qualified Options.Applicative as O
import qualified System.Clock as Clock
import qualified System.FS.API as FS
import qualified System.FS.IO as FsIO
import           System.IO.Temp (withTempDirectory)
import           System.Mem (performMajorGC)
import           Text.Printf (printf)

-------------------------------------------------------------------------------
-- Setup
-------------------------------------------------------------------------------

-- Using primitive makes serialisation overhead as low as possible
type K = BS.ByteString
type V = BS.ByteString
type B = P.ByteArray

-------------------------------------------------------------------------------
-- Main
-------------------------------------------------------------------------------

main :: IO ()
main = do
    opts <- O.execParser optsP'
    print opts
    withSessionDir $ \sessionRoot -> do
        print sessionRoot
        performMajorGC
        ((), seconds) <- timed (benchRun opts sessionRoot)
        printf "time %f s\n" seconds

  where
    optsP' = O.info (optsP <**> O.helper) O.fullDesc

-------------------------------------------------------------------------------
-- Bench
-------------------------------------------------------------------------------

theValue :: V
theValue = BS.replicate 60 120 -- 'x'
{-# NOINLINE theValue #-}

-- We generate keys by hashing a word64 and adding two "random" bytes.
-- This way we can ensure that keys are distinct.
--
-- I think this approach of generating keys should match UTxO quite well.
-- This is purely CPU bound operation, and we should be able to push IO
-- when doing these in between.
makeKey :: Word64 -> K
makeKey w64 = SHA256.hashlazy (B.encode w64) <> "=="

benchRun :: Opts -> FilePath -> IO ()
benchRun opts sessionRoot = do
    let fs = FsIO.ioHasFS (FS.MountPoint sessionRoot)
    -- flush write buffer
    let wb :: WB.WriteBuffer K V B
        !wb = foldl'
            (\wb' i -> WB.addEntryNormal (makeKey i) (N.Insert theValue Nothing) wb')
            WB.empty
            [ 1 .. fromIntegral (opts.size) :: Word64 ]

    _run <- Run.fromWriteBuffer fs (Run.RunFsPaths 42) wb
    return ()

-------------------------------------------------------------------------------
-- Session
-------------------------------------------------------------------------------

withSessionDir :: (FilePath -> IO a) -> IO a
withSessionDir = withTempDirectory "" "session"

-------------------------------------------------------------------------------
-- Opts
-------------------------------------------------------------------------------

data Opts = Opts
    { size :: Int
    -- TODO: do we need a flag to not remove temporary directory?
    }
  deriving Show

optsP :: O.Parser Opts
optsP = pure Opts
    <*> O.option O.auto (O.long "size" <> O.value 500000 <> O.help "Size of initial run")

-------------------------------------------------------------------------------
-- clock
-------------------------------------------------------------------------------

timed :: IO a -> IO (a, Double)
timed action = do
    t1 <- Clock.getTime Clock.Monotonic
    x  <- action
    t2 <- Clock.getTime Clock.Monotonic
    return (x, fromIntegral (Clock.toNanoSecs (Clock.diffTimeSpec t2 t1)) * 1e-9)
