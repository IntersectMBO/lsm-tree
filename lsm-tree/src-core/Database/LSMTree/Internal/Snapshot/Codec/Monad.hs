{-# OPTIONS_HADDOCK not-home #-}

-- | Decoder monad for snapshot metadata
module Database.LSMTree.Internal.Snapshot.Codec.Monad (
    Dec (..)
  , Env (..)
  , runDec
  , liftDecoder
  , liftST
  ) where

import           Codec.CBOR.Decoding (Decoder)
import qualified Codec.CBOR.Decoding as CBOR
import           Control.Monad.Reader (MonadReader, MonadTrans (lift),
                     ReaderT (..))
import           Control.Monad.ST (ST)

newtype Dec s a = Dec { unwrap :: ReaderT Env (Decoder s) a }
  deriving newtype (Functor, Applicative, Monad)

deriving newtype instance MonadReader Env (Dec s)
deriving newtype instance MonadFail (Dec s)

data Env = Env {
    v3Assert :: Bool
  }

runDec :: Dec s a -> Env -> Decoder s a
runDec dec env = runReaderT dec.unwrap env

liftDecoder :: Decoder s a -> Dec s a
liftDecoder = Dec . lift

liftST :: ST s a -> Dec s a
liftST = Dec . lift . CBOR.liftST
