{-# LANGUAGE RecordWildCards #-}
{-# OPTIONS_HADDOCK not-home #-}

-- | Incremental (in-memory portion of) run construction
--
module Database.LSMTree.Internal.BloomFilterAcc (
    -- * Bloom filter allocation
    RunBloomFilterAlloc (..)
    -- * Incremental, in-memory bloom filter construction
  , newMBloom
  , bloomInserts
  ) where

import           Control.DeepSeq (NFData (..))
import           Control.Monad.ST.Strict
import qualified Data.BloomFilter.Blocked as Bloom
import           Database.LSMTree.Internal.BloomFilter (MBloom)
import           Database.LSMTree.Internal.Entry (NumEntries (..))
import           Database.LSMTree.Internal.PageAcc (PageAcc)
import qualified Database.LSMTree.Internal.PageAcc as PageAcc
import           Database.LSMTree.Internal.Serialise (SerialisedKey)

{-------------------------------------------------------------------------------
  Bloom filter allocation
-------------------------------------------------------------------------------}

-- | See 'Database.LSMTree.Internal.Config.BloomFilterAlloc'
data RunBloomFilterAlloc =
    -- | Bits per element in a filter
    RunAllocFixed      !Double
  | RunAllocRequestFPR !Double
  deriving stock (Show, Eq)

instance NFData RunBloomFilterAlloc where
    rnf (RunAllocFixed a)      = rnf a
    rnf (RunAllocRequestFPR a) = rnf a

{-------------------------------------------------------------------------------
  Incremental, in-memory bloom filter construction
-------------------------------------------------------------------------------}

newMBloom :: NumEntries -> RunBloomFilterAlloc -> Bloom.Salt -> ST s (MBloom s a)
newMBloom (NumEntries nentries) alloc salt =
    Bloom.new (Bloom.sizeForPolicy (policy alloc) nentries) salt
  where
    --TODO: it'd be possible to turn the RunBloomFilterAlloc into a BloomPolicy
    -- without the NumEntries, and cache the policy, avoiding recalculating the
    -- policy every time.
    policy (RunAllocFixed bitsPerEntry) = Bloom.policyForBits bitsPerEntry
    policy (RunAllocRequestFPR fpr)     = Bloom.policyForFPR fpr

-- An instance of insertMany specialised to SerialisedKey and indexKeyPageAcc.
-- This is a performance-sensitive function. It is marked NOINLINE so we can
-- easily inspect the core and check all the specialisations worked as expected.
{-# NOINLINE bloomInserts #-}
bloomInserts :: MBloom s SerialisedKey -> PageAcc s -> Int -> ST s ()
bloomInserts !mbloom !mpageacc !nkeys =
    Bloom.insertMany mbloom (PageAcc.indexKeyPageAcc mpageacc) nkeys
