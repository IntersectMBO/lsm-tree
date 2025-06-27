<style>
r { color: Red }
o { color: Orange }
g { color: Green }
</style>

# Storing the Cardano ledger state on disk: final report for high-performance backend

Authors: Duncan Coutts, Joris Dral, Wolfgang Jeltsch
Date: June 2025

As part of the project to reduce `cardano-node`’s memory use (colloquially known
as UTXO-HD), a high-performance disk backend has been developed as an
arm’s-length project by Well-Typed LLP on behalf of Input Output Global, Inc.
(IOG) and later Intersect MBO. The intent is for the backend to be integrated
into the consensus layer of `cardano-node`, specifically to be used for storing
large parts of the Cardano ledger state. The backend is now feature-complete and
should satisfy all functional requirements, and it has favourable results
regarding the performance requirements.

The backend is implemented as a Haskell library called `lsm-tree`[^2], which
provides efficient on-disk key–value storage using log-structured merge-trees,
or LSM-trees for short. An LSM-tree is a data structure for key–value mappings
that is optimized for large tables with a high insertion volume, such as the
UTXO set and other stake-related data. The library has a number of custom
features that are primarily tailored towards use cases of the consensus layer,
but it should be useful for the broader Haskell community as well.

Currently, a UTXO-HD `cardano-node` already exists, but it is an MVP that uses
off-the-shelf database software (LMDB) to store parts of the ledger state on
disk. Though the LMDB-based solution is suitable for the current state of the
Cardano blockchain, it is not suitable to achieve Cardano’s long-term business
requirements, such as high throughput with limited system resources. The goal of
`lsm-tree` is to pave the way for achieving said business requirements,
providing the necessary foundation on which technologies like Ouroboros Leios
can build.

Prior to development, an analysis was conducted, leading to a comprehensive
requirements document[^1] outlining the functional and non-functional
(performance) requirements for the `lsm-tree` library. The requirements document
includes recommendations for the disk backend to meet the following criteria:

1. It helps achieve the higher performance targets.
2. It supports the new and improved operations required by the ledger to be able
   to move the stake-related tables to disk.
3. It is able to be tested in the context of the integrated consensus layer
   using the I/O fault testing framework.

This report aims to set out the requirements for the `lsm-tree` library as
described in the original requirements document and to analyse how and to what
extent these requirements are met by the implementation. The intent of this
report is not to substantiate how the requirements came to be; the full context
including technical details is available in the original requirements document
for the interested reader.

It should be noted that the requirements of the `lsm-tree` component were
specified in isolation from the consensus layer and `cardano-node`, but these
requirements were of course chosen with the larger system in mind. This report
only reviews the development of `lsm-tree` as a standalone component, while
integration notes are provided in an accompanying document[^7]. Integration of
`lsm-tree` with the consensus layer will happen as a separate phase of the
UTXO-HD project.

Readers are advised to familiarise themselves with the API of the library by
reading through the Haddock documentation of the public API. A version of the
Haddock documentation that tracks the `main` branch of the repository is hosted
using GitHub Pages[^6]. There are two modules that make up the public API: the
`Database.LSMTree` module contains the full-featured public API, whereas the
`Database.LSMTree.Simple` module offers a simplified version that is aimed at
new users and use cases that do not require advanced features. Additional
documentation can be found in the package description[^8]. This and the simple
module should be good places to start at before moving on to the full-featured
module.

The version of the library that is used as the basis for this report is tagged
`alpha` in the `lsm-tree` Git repository. It can be checked out using the
following commands:

```sh
git clone git@github.com:IntersectMBO/lsm-tree.git
cd lsm-tree
git checkout alpha
```

## Development history

Before the `lsm-tree` project was officially commissioned, we performed some
prototyping and design work. This preparatory work was important to de-risk key
aspects of the design, such as achieving high SSD throughput using a Haskell
program. The initial results from the prototypes were positive, which
contributed to the decision to move forward with the project.

Development of `lsm-tree` officially started in early autumn 2023. The first
stages focused mostly on additional prototyping and design but also included
testing. Among the prototyping and design artefacts are the following:

* A prototype for the incremental merge algorithm[^3]
* A library for high SSD throughput using asynchronous I/O[^4]
* Specifications of the formats of on-disk files and directories[^5]

In the spirit of test-driven development, we created a reference implementation
for the library, modelling each of the basic and advanced features that the
library would eventually have. Additionally, we implemented tests targeting the
reference implementation. We designed these tests in such a way that they could
later be reused by the actual implementation of the library, but initially they
served as sanity checks for the reference implementation.

After the initial phase, we worked towards an MVP for the `lsm-tree` library.
During this period, we wrote most of the core code of the library. We aimed at
making the MVP a full implementation of a key–value store, covering the basic
operations, namely insertion, deletion and lookup, but not necessarily advanced
or custom features.

Over the course of time, we wrote tests and micro-benchmarks for the library
internals to sanity-check our progress. In particular, we created
micro-benchmarks to catch performance defects or regressions early.

We focused on defining the in-memory structures and algorithms of the MVP first
before moving on to I/O code. This separation proved fruitful: the purely
functional code could be tested and benchmarked before dealing with the more
tricky aspects of impure code.

Towards completion of the MVP, we created a macro-benchmark that simulates the
workload when handling the UTXO set – the largest component of the ledger state
that UTXO-HD moves to disk and likely the most performance-sensitive dataset.
This benchmark serves in particular as the basis for assessing the performance
requirements, which will be discussed further in the [*Performance
requirements*](#performance-requirements) section. Results of running the
macro-benchmark on the MVP showed promising results.

With the core of the library implemented, we turned to adding more features to
the library, beyond the basic key–value operations, including some special
features that are not provided by off-the-shelf database software. The following
features are worth mentioning:

* Monoidal updates
* Separate blob retrieval
* Range lookups and cursors
* Incremental merges
* Multiple writeable table references
* Snapshots
* Table unions
* I/O fault testing

Most of these features will also be touched upon in the [*Functional
requirements*](#functional-requirements) section.

Along the way, we optimised and refactored code to improve the quality of the
library and its tests. For each completed feature, we enabled the corresponding
tests that we had written for the reference implementation to check the real
implementation as well. The passing of these tests served as a rite of passage
for each completed feature.

In the final stages, we reviewed and improved the public API, tests, benchmarks,
documentation and library packaging. We constructed the final deliverables, such
as this report and additional integration notes[^7], which should guide the
integration of `lsm-tree` with the consensus layer. In June 2025, we reached
the final milestone.

## Functional requirements

This section outlines the functional requirements for the `lsm-tree` library and
how they are satisfied. The requirements are described in the original
requirements document[^1], Section 18.2.

Several requirements specify that an appropriate test should demonstrate the
desired functionality. Though the internals of the library are extensively
tested directly, the tests that should be of most interest to the reader are
those that are written against the public API. These tests can be found in the
modules listed below, whose source code is under the `test` directory. Where it
aids the report in the analysis of the functional requirements, we will
reference specific tests. Beyond this, the reader is encouraged to peruse and
review the listed modules.

| Module                                 | Test style    |
|----------------------------------------|---------------|
|`Test.Database.LSMTree.Class`           | Classic       |
|`Test.Database.LSMTree.StateMachine`    | State machine |
|`Test.Database.LSMTree.StateMachine.DL` | State machine |
|`Test.Database.LSMTree.UnitTests`       | Unit          |

The tests are written in three styles:

* *Classic* QuickCheck-style property tests check for particular properties of
  the library’s features. Many of these properties are tailored towards specific
  functional requirements, and their tests are usually grouped according to the
  requirements they refer to.

* *State machine* tests compare the library against a model (the reference
  implementation) for a broad spectrum of interleaved actions that are performed
  in lock-step by the library and its model. The state machine tests generate a
  variety of scenarios, some of which are similar to scenarios that the classic
  property tests comprise. A clear benefit of the state machine tests is that
  they are more likely than classic property tests to catch subtle edge-cases
  and interactions.

* *Unit* tests are used to exercise behaviour that is interesting in its own
  right but not suitable for inclusion in the state machine tests because it
  would negatively affect their coverage. For example, performing operations on
  closed tables will always throw the same exception; so not including this
  behaviour in the state machine tests means that these tests can do interesting
  things with *open* tables more often.

### Requirement 1

> It should have an interface that is capable of implementing the existing
> interface used by the existing consensus layer for its on-disk backends.

For the analysis of this functional requirement, we use a fixed version of the
`ouroboros-consensus` repository[^10]. This version can be checked out using the
following commands:

```sh
git clone git@github.com:IntersectMBO/ouroboros-consensus.git
cd ouroboros-consensus
git checkout 9d41590555954c511d5f81682ccf7bc963659708
```

The consensus interface that has to be implemented using `lsm-tree` is given by
the `LedgerTablesHandle` record type[^11]. This type provides an abstract view
on the table storage, so that the rest of the consensus layer does not have to
concern itself with the concrete implementation of that storage, be it based on
`lsm-tree` or not; `ouroboros-consensus` can freely pick any particular record
as long as it constitutes a faithful implementation of the storage interface.
This has advantages for initial functional integration because the integration
effort is confined to the implementation of the record. To take full advantage
of all of `lsm-tree`’s features, further integration efforts would be needed
because changes to the interface and the rest of the consensus layer would be
required. However, this is considered out of scope for the current phase of the
UTXO-HD project.

Currently, the consensus layer has one implementation of table storage for the
ledger, which stores all data in main memory[^12]. This implementation preserves
much of the behaviour of a pre-UTXO-HD node. A closer look at it shows that
there are two pieces of implementation-specific functionality that are not
covered by the `LedgerTablesHandle` record: creating a fresh such record and
producing such a record from an on-disk snapshot. It makes sense that these are
standalone functions, as they produce the records in the first place.

All in all, we are left with the following API to implement in the integration
phase:

```haskell
newLSMTreeLedgerTablesHandle ::
  _ -> m (LedgerTablesHandle m l) -- newTable


openLSMTreeLedgerTablesHandleSnapshot ::
  _ -> m (LedgerTablesHandle m l) -- openTableFromSnapshot


data LedgerTablesHandle m l = LedgerTablesHandle {
    close              :: _ -- closeTable
  , duplicate          :: _ -- duplicate
  , read               :: _ -- lookups
  , readRange          :: _ -- rangeLookup or Cursor functions
  , readAll            :: _ -- rangeLookup or Cursor functions
  , pushDiffs          :: _ -- updates
  , takeHandleSnapshot :: _ -- saveSnapshot
  , tablesSize         :: _ -- return Nothing
  }
```

For the sake of brevity, we have elided the types of the different functions,
replacing them by underscores. To give a sense of how the `lsm-tree` library’s
interface fits the interface used in `ouroboros-consensus`, each of the
functions above has a comment that lists the corresponding function or functions
from `lsm-tree`’s public API that should be used to implement it.

Note that `tablesSize` should always return `Nothing` in the case of the
`lsm-tree`-based implementation. While the in-memory implementation can
determine the number of entries in a table in constant time, given that this
value is cached, the `lsm-tree`-based implementation would have to read all the
*physical* entries from disk in order to compute the number of *logical* entries
because it would have to ensure that key duplicates are ignored. We already
anticipated this when we defined the `LedgerTablesHandle` type, and consequently
accounted for it by using `Maybe Int` as the return type of `tablesSize`.

The analysis above offers a simplified view on how the `lsm-tree` and consensus
interfaces fit together; so this report is accompanied by integration notes[^7]
that provide further guidance. These notes include, for example, an explanation
of the need to store a session context in the ledger database. However,
implementation details like these are not considered to be blockers for the
integration efforts, as there are clear paths forward.

### Requirement 2

> The basic properties of being a key value store should be demonstrated using
> an appropriate test or tests.

`lsm-tree` has been supporting the basic operations `insert`, `delete` and
`lookup` as well as their bulk versions `inserts`, `deletes` and `lookups` since
the MVP was finished. It also provides operations `update` and `updates`,
realised as combinations of inserts and deletes.

We generally advise to prefer the bulk operations over the elementary ones. On
Linux systems, lookups in particular will better utilise the storage bandwidth
when the bulk version is used, especially in a concurrent setting. This is due
to the method used to perform batches of I/O, which employs the `blockio-uring`
package[^4]: submitting many batches of I/O concurrently will lead to many I/O
requests being in flight at once, so that the SSD bandwidth can be saturated.
This is particularly relevant for the consensus layer, which will have to employ
concurrent batching to meet higher performance targets, for example by using a
pipelining design.

It is not part of the requirements but does deserve to be mentioned that there
is specific support for storing blobs (binary large objects). Morally, blobs are
part of values, but they are stored and retrieved separately. The reasoning
behind this feature is that there are use cases where not all of the data in a
value is needed by the user. For example, when a Cardano node is replaying the
current chain, the consensus layer skips expensive checks like script
validation. Scripts are part of UTXO values, and we could skip reading them
during replay if we stored them as blobs. Note that the performance improvements
from using blob storage are only with lookups; *updates* involving blobs are
about as expensive as if the blobs’ contents were included in the values.

A naive implementation of updates entails latency spikes due to table merging,
but the `lsm-tree` library can avoid such spikes by spreading out I/O over time,
using an incremental merge algorithm: the algorithm that we prototyped at the
start of the `lsm-tree` project[^3]. Avoiding latency spikes is essential for
`cardano-node` because `cardano-node` is a real-time system, which has to
respond to input promptly. The use of the incremental merge algorithm does not
improve the time complexity of updates as such, but it turns the *amortised*
time complexity of the naive solution into a *worst-case* time complexity.

### Requirement 3

> It should have an extended interface that supports key-range lookups, and this
> should be demonstrated using an appropriate test or tests.

The library offers two alternatives for key–range lookups:

* *The `rangeLookup` function* requires the user to specify a range of keys via
  a lower and an upper bound and reads the corresponding entries from a table.

* A *cursor* can be used to read consecutive segments of a table. The user can
  position a cursor at a specific key and then read database entries from there
  either one by one or in bulk, with bulk reading being the recommended method.
  A cursor offers a stable view of a table much like an independently writeable
  reference (see functional requirement 5), meaning that updates to the original
  table are not visible through the cursor. Currently, reading from a cursor
  simultaneously advances the cursor position.

Internally, `rangeLookup` is defined in terms of the cursor interface but is
provided for convenience nonetheless. A range lookup always returns as many
entries as there are in the specified table segment, while reading through a
cursor returns as many entries as requested, unless the end of the table is hit.

### Requirement 4

> It should have an extended interface that supports a ‘monoidal update’
> operation in addition to the normal insert, delete and lookup. The choice of
> monoid should be per table/mapping (not per operation). The behaviour of this
> operation should be demonstrated using an appropriate test.

The terminology related to monoidal updates in `lsm-tree` is different from the
terminology used in the original functional requirement. From this point
onwards, we will say ‘upsert’ instead of ‘monoidal update’ and ‘resolve
function’ instead of ‘monoid’.

Where `lsm-tree`’s `insert` and `delete` behave like the functions `insert` and
`delete` from `Data.Map`, `upsert` behaves like `insertWith`: if the table
contains no value for the given key, the given value is inserted; otherwise, the
given value is combined with the one in the table to form the new value. The
combining of values is done with a user-supplied resolve function. It is
required that this function is associative. Examples of associative functions
are `(+)` and `(*)` but also `const` and `max`. If the resolve function is
`const`, an upsert behaves like an insert. The choice of resolve function is per
table, and this is enforced using a `ResolveValue` class constraint. The user
can implement the resolve function on unserialised values, serialised values or
both. It is advisable to at least implement it on serialised values because this
way serialisation and deserialisation of values can be avoided.

Like inserts and deletes, upserts can be submitted either one by one or in bulk.
There is also an `updates` function that allows submitting a mix of upserts,
inserts and deletes.

### Requirement 5

> It should have an extended interface that exposes the ability to support
> multiple independently writable references to different versions of the
> datastore. The behaviour of this functionality should be demonstrated using an
> appropriate test. For further details see Section 17.

Multiple independently writable references can be created using `duplicate`.
`duplicate` is relatively cheap because we do not need to copy data around:
Haskell provides us with data structure persistence, sharing and garbage
collection for in-memory data, and we simulate these features for on-disk data.
However, it is important to make a distinction between *immutable* and
*mutable* data. We will focus on immutable data first.

Immutable in-memory data can be shared between tables and their duplicates ‘for
free’ by relying on Haskell’s persistent data structures, and the garbage
collector will free memory ‘for free’ when it becomes unused. What is inherent
to LSM-trees is that on-disk data is immutable once created, which means we can
also share immutable on-disk data ‘for free’. Garbage collection for on-disk
data does not come for free however. Therefore, internally all shared disk
resources (like files and file handles) are reference-counted using a custom
reference counting scheme. When the last reference to a disk resource is
released, the disk resource is deleted. Our reference counting scheme is quite
elaborate: it can be run in a debug mode that checks, amongst other things, that
all references are ultimately released.

The story for mutable data is slightly more tricky. Most importantly,
incremental merges (see functional requirement 2) are shared between tables and
their duplicates. Continuing the analogy with Haskell’s persistent data
structures and sharing, we can view an incremental merge as similar to a thunk.
Incremental merges only progress, which does not involve modifying data in
place, and each table and its duplicates share the same progress. Special care
is taken to ensure that incremental merges can progress concurrently without
threads waiting much on other threads. The design here is to only perform actual
I/O in batches, while tracking in memory how much I/O should be performed and
when. The in-memory tracking data can be updated concurrently and relatively
quickly, and ideally only one of the threads will do I/O from time to time.
Garbage collection for shared incremental merges uses the same reference
counting approach as garbage collection for immutable on-disk data.

### Requirement 6

> It should have an extended interface that supports taking snapshots of tables.
> This should have $O(\log n)$ time complexity. The behaviour of this operation
> should be demonstrated using an appropriate test. For further details see
> Sections 7.5 and 7.6.

Snapshots can be created using `saveSnapshot` and later be opened using
`openTableFromSnapshot`. Non-snapshotted contents of a table are lost when the
table is closed (and thus in particular when its session is closed). Therefore,
it is necessary to take a snapshot if one wants to drop access to a table and
later regain it. When a snapshot is created, the library ensures durability of
the snapshot by flushing to disk the contents and file system metadata of files
and directories involved in the snapshot.

Creating a snapshot is relatively cheap: hard links to all the table-specific
files are put in a dedicated snapshot directory, together with some metadata and
a serialised write buffer. This way, most files are shared between active tables
and snapshots, but this is okay since these files are immutable by design. Hard
links are supported by all POSIX systems, and Windows supports them for NTFS
file systems. The number of files in a table scales logarithmically with the
number of physical key–value entries in the table. Hence, if the number of
entries in a table is $n$, then taking a snapshot requires $O(\log n)$ disk
operations.

Opening a snapshot is more costly because the contents of all files involved in
the snapshot are validated (see functional requirement 8). Moreover, the state
of ongoing merges is not stored in a snapshot but recomputed when the snapshot
is opened, which takes additional time. Another downside of not storing merge
state is that any sharing of in-memory data is lost when creating a snapshot and
opening it later. A more sophisticated design could conceivably restore sharing,
but we chose the current design for the sake of simplicity.

### Requirement 7

> It should have an extended interface that supports merging monoidal tables.
> This should have $O(\log n)$ time complexity at the point it is used, on the
> assumption that it is used at most every $n$ steps. The behaviour of this
> operation should be demonstrated using an appropriate test. For further
> details see Section 7.7.

Since the term ‘merge’ is already part of the LSM-tree terminology, we chose to
call this operation a table *union* instead. Moreover, ‘union’ is a more fitting
name, since the behavior of table union is similar to that of
`Data.Map.unionWith`: all logical key–value pairs with unique keys are
preserved, but pairs that have the same key are combined using the resolve
function that is also used for upserts (see functional requirement 4). When the
resolve function is `const`, table union behaves like `Data.Map.union`, which
computes a left-biased union.

We make a distinction between *immediate* and *incremental* unions:

* *Immediate unions* (`union`, `unions`) perform all necessary computation
  immediately. This can be costly; in general, it requires that all key–value
  pairs of the involved tables are read from disk, combined using the resolve
  function and then written back to disk.

* *Incremental unions* (`incrementalUnion`, `incrementalUnions`) offer a way to
  spread out computation cost over time. Initially, an incremental union just
  combines the internal trees of the input tables to form a new tree for the
  output table. This is relatively cheap, as it requires mostly in-memory
  operations and induces only a constant amount of disk I/O. Afterwards, the
  user can repeatedly request a next computation batch to be performed until the
  whole computation is completed (`supplyUnionCredits`). It is up to the user to
  decide on the sizes of these batches, and this decision can be made based on
  the amount of work still to be done, which can be queried
  (`remainingUnionDebt`).

  The whole computation requires a linear amount of disk I/O, regardless of the
  batching strategy. However, usually not every read or write of a key–value
  pair requires I/O, since in most cases multiple key–value pairs fit into a
  single disk page and only a read or write of a disk page induces I/O.

Internally, immediate unions are implemented in terms of incremental unions.

Initially, performing a lookup in an incremental union table will cost roughly
the same as performing a lookup in each of the input tables separately. However,
as the computation of the union progresses, the internal tree is progressively
merged into a single run, and finally a lookup induces only a constant amount of
I/O. Since immediate unions are completed immediately, lookups in their results
are immediately cheap.

In Cardano, unions should be used for combining tables of staking rewards at
epoch boundaries. Because Cardano is a real-time system, latency spikes during
normal operation must be prevented; so Cardano should use incremental unions for
combining staking reward tables. When doing so, the computation of each union
can be spread out over the course of the following epoch and be finished right
before the next incremental union starts.

### Requirement 8

> It should be able to run within the `io-sim` simulator, and do all disk I/O
> operations via the `fs-sim` simulation API (which may be extended as needed).
> It should be able to detect file data corruption upon startup/restoration.
> Detection of corruption during startup should be reported by an appropriate
> mechanism. During normal operation any I/O exceptions should be reported by an
> appropriate mechanism, but it need not detect ‘silent’ data corruption. The
> behaviour of this corrupted detection should be demonstrated using an
> appropriate test. For further details see Section 14.2.

The `lsm-tree` library supports both real I/O and I/O simulation, including file
system simulation. Its public functions work with arbitrary `IO`-like monads and
can therefore be used in `IO` as well as `IOSim` computations. When opening a
session (`openSession`), the user provides a value of the `HasFS` record type
from the `fs-api` package to specify the file system implementation to use. This
way, the user can choose between the real file system and the file system
simulation provided by the `fs-sim` package.

We made some smaller changes to `fs-api` and `fs-sim` to facilitate the
development of `lsm-tree`. Furthermore, we created an extension to `HasFS`
called `HasBlockIO`. It captures both the submission of batches of I/O, for
example using `blockio-uring`[^4], and some functionality unrelated to batching
that is nonetheless useful for `lsm-tree`. The latter could eventually be
included in `fs-api` and `fs-sim`.

In the context of `lsm-tree`, startup or restoration means opening a table from
a table snapshot. In the consensus layer, table snapshots would be part of the
ledger state snapshots that are created periodically. When a Cardano node starts
up, the most recent, uncorrupted ledger state snapshot has to be restored, which
requires checking table snapshots for corruption.

The technique for detecting whether a table snapshot is corrupted consists of
two parts:

* Each snapshot includes a metadata file that captures the structure of the
  table’s internal tree and stores the table’s configuration options. Any
  mismatch between the metadata file and other files in the snapshot directory
  will result in an exception.

* All files in the snapshot, including the metadata file, have associated
  checksum files. Checksums for run files are computed incrementally; so their
  computation does not induce latency spikes. Like the other table files, also
  checksum files are not copied but referenced via hard links when a snapshot is
  created. When a snapshot is opened, the checksum for each file is recomputed
  and checked against the stored checksum. Any checksum mismatch will be
  communicated to the user using an exception.

Note that the success of this technique is independent of the nature of the I/O
faults that caused a corruption: corruption from both noisy and silent I/O
faults is detected.

Corruption detection upon snapshot opening is tested by the state machine tests
using their random interleaving of table operations, which include an operation
that creates a snapshot and corrupts it. However, there is also a dedicated test
for corruption detection, which can be run using the following command:

```sh
cabal run lsm-tree-test -- -p prop_flipSnapshotBit
```

This test creates a snapshot of a randomly populated table and flips a random
bit in one of the snapshot’s files. Afterwards, it opens the snapshot and
verifies that an exception is raised.

In addition to performing the corruption detection described above, the library
does not further attempt to detect silent I/O faults, but it raises exceptions
for all noisy I/O faults, that is, faults that are detected by the underlying
software or hardware. To verify that this reporting of noisy I/O faults works
properly, we implemented a variant of the state machine tests, which can be run
using the following command:

```sh
cabal run lsm-tree-test -- -p prop_noSwallowedExceptions
```

This variant aggressively injects I/O faults through the `fs-sim` file system
simulation and checks whether it can observe an exception for each injected
fault.

A noble goal for the library would be to be fully exception-safe, ensuring that
tables are left in a consistent state even after noisy I/O faults (assuming
proper masking by the user where required). This was not a functional
requirement, however, since in case of an exception the consensus layer will
shut down anyway and thus drop all table data outside of snapshots. Nonetheless,
we took some steps to ensure exception safety for the core library, but did not
achieve it fully. We also extended the state machine tests with machinery that
injects noisy I/O faults and checks whether the tables are left in a consistent
state. If there should be a desire in the future to make the library fully
exception-safe, then this machinery should help achieve this goal.

## Performance requirements

<r>TODO</r>



## Sources

<!-- TODO(joris): I haven’t found a way (yet) to do proper citations in Markdown, so I’m abusing footnotes. We might port this text to a LateX document later, so this should do for now -->

[^1]: Storing the Cardano ledger state on disk: requirements for a high performance backend

[^2]: Link: https://github.com/IntersectMBO/lsm-tree/tree/alpha

[^3]: Link: https://github.com/IntersectMBO/lsm-tree/blob/alpha/prototypes/ScheduledMerges.hs

[^4]: Link: https://github.com/well-typed/blockio-uring

[^5]: Link: https://github.com/IntersectMBO/lsm-tree/tree/alpha/doc

[^6]: Link: https://intersectmbo.github.io/lsm-tree/index.html

[^7]: Storing the Cardano ledger state on disk: integration notes for high performance backend

[^8]: Link: https://github.com/IntersectMBO/lsm-tree/blob/main/lsm-tree.cabal



[^10]: Link: https://github.com/IntersectMBO/ouroboros-consensus/commit/9d41590555954c511d5f81682ccf7bc963659708

[^11]: Link: https://github.com/IntersectMBO/ouroboros-consensus/blob/9d41590555954c511d5f81682ccf7bc963659708/ouroboros-consensus/src/ouroboros-consensus/Ouroboros/Consensus/Storage/LedgerDB/V2/LedgerSeq.hs#L72-L96

[^12]: Link: https://github.com/IntersectMBO/ouroboros-consensus/blob/9d41590555954c511d5f81682ccf7bc963659708/ouroboros-consensus/src/ouroboros-consensus/Ouroboros/Consensus/Storage/LedgerDB/V2/InMemory.hs
