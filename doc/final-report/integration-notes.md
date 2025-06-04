# Storing the Cardano ledger state on disk: integration notes for high-performance backend

Authors: Joris Dral, Wolfgang Jeltsch
Date: May 2025

## Sessions

Creating new empty tables or opening tables from snapshots requires a `Session`.
The session can be created using `openSession`, which has to be done in the
consensus layer. The session should be shared between all tables. Sharing
between a table and its duplicates, which are created using `duplicate`, is
automatic. Once the session is created, it could be stored in the `LedgerDB`.
When the `LedgerDB` is closed, all tables and the session should be closed.
Closing the session will automatically close all tables, but this is only
intended to be a backup functionality: ideally the user closes all tables
manually.

## The compact index

The compact index is a memory-efficient data structure that maintains serialised
keys. Rather than storing full keys, it only stores the first 64 bits of each
key.

The compact index only works properly if in most cases it can determine the
order of two serialised keys by looking at their 64-bit prefixes. This is the
case, for example, when the keys are hashes: the probability that two hashes
have the same 64-bit prefixes is $\frac{1}{2}^{64}$ and thus very small. If the
hashes are 256 bits in size, then the compact index uses 4 times less memory
than if it would store the full keys.

There is a backup mechanism in place for the case when the 64-bit prefixes of
keys are not sufficient to make a comparison. This backup mechanism is less
memory-efficient and less performant. That said, if the probability of prefix
clashes is very small, like in the example above, then in practice the backup
mechanism will never be used.

UTXO keys are *almost* uniformly distributed. Each UTXO key consist of a 32-byte
hash and a 2-byte index. While the distribution of hashes is uniform, the
distribution of indexes is not, as indexes are counters that always start at 0.
A typical transaction has two inputs and two outputs and thus requires storing
two UTXO keys that have the same hash part, albeit not the same index part. If
we serialise UTXO keys naively, putting the hash part before the index part,
then the 64-bit prefixes will often not be sufficient to make comparisons
between keys. As a result, the backup mechanism will kick in way too often,
which will severely hamper performance.

The solution is to change the serialisation of UTXO keys such that the first
64 bits of a serialised key comprise the 2-byte index and just 48 bits of the
hash. This way, comparisons of keys with equal hashes will succeed, as the
indexes will be taken into account. On the other hand, it becomes more likely
that the covered bits of hashes are not enough to distinguish between different
hashes, but the propability of this should still be so low that the backup
mechanism will not kick in in practice.

Importantly, range lookups and cursor reads return key–value pairs in the order
of their *serialised* keys. With the described change to UTXO key serialisation,
the ordering of serialised keys no longer matches the ordering of actual,
unserialised keys. This is fine for `lsm-tree`, for which any total ordering of
keys is as good as any other total ordering. However, the consensus layer will
face the situation where a range lookup or a cursor read returns key–value pairs
slightly out of order. Currently, we do not expect this to cause problems.

## Snapshots

Snapshots currently require support for hard links. This means that on Windows
the library only works when using NTFS. Support for other file systems could be
added by providing an alternative snapshotting method, but such a method would
likely involve copying file contents, which is slower than hard-linking.

Creating a snapshot outside the session directory while still using hard links
should be possible as long as the directory for the snapshot is on the same disk
volume as the session directory, but this feature is currently not implemented.
Hard-linking across different volumes is generally not possible; therefore,
placing a snapshot on a volume that does not hold the associated session
directory requires a different snapshotting implementation, which would probably
also rely on copying file contents.

A copying snapshotting implementation would probably kill two birds with one
stone by removing the two current limitations just discussed.

Presumably, `cardano-node` will eventually be required to support storing
snapshots on a different volume than where the session is placed, for example on
a cheaper non-SSD drive. This feature was unfortunately not anticipated in the
project specification and so is not currently included. As discussed above, it
could be added with some additional work.

## Value resolving

When instantiating the `ResolveValue` class, it is usually advisable to
implement `resolveValue` such that it works directly on the serialised values.
This is typically cheaper than having `resolveValue` deserialise the values,
composing them, and then serialising the result. For example, when the resolve
function is intended to work like `(+)`, then `resolveValue` could add the raw
bytes of the serialised values and would likely achieve better performance this
way.

## `io-classes` incompatibility

At the time of writing, various packages in the `cardano-node` stack depend on
`io-classes-1.5` and the 1.5-versions of its daughter packages, like
`strict-stm`. For example, the build dependencies in `ouroboros-consensus.cabal`
contain the following:

* `io-classes ^>= 1.5`
* `strict-stm ^>= 1.5`

However, `lsm-tree` needs `io-classes-1.6` or `io-classes-1.7`, and this leads
to a dependency conflict. One would hope that a package could have loose enough
bounds that it could be built with `io-classes-1.5`, `io-classes-1.6`, and
`io-classes-1.7`. Unfortunately, this is not the case, because, starting with
the `io-classes-1.6` release, daughter packages like `strict-stm` are
sublibraries of `io-classes`. For example, the build dependencies in
`lsm-tree.cabal` contain the following:

* `io-classes ^>= 1.6 || ^>= 1.7`
* `io-classes:strict-stm`

Sadly, there is currently no way to express both sets of build dependencies
within a single `build-depends` field, as Cabal’s support for conditional
expressions is not powerful enough for this.

It is known to us that the `ouroboros-consensus` stack has not been updated to
`io-classes-1.7` due to a bug related to Nix. For more information, see
https://github.com/IntersectMBO/ouroboros-network/pull/4951. We would advise to
fix this Nix-related bug rather than downgrading `lsm-tree`’s dependency on
`io-classes` to version 1.5.
