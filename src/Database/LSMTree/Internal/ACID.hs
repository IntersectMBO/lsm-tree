module Database.LSMTree.Internal.ACID where


{-
  Note [ACID and exception safety]
~~~~~~~~~~~~~~

  This text copies liberally from https://en.wikipedia.org/wiki/ACID and related
  wiki pages.

  Atomicity, consistency, isolation, and durability (ACID) are important
  properties of database transactions. They guarantee data validity despite
  errors, power failures, and other mishaps. A /transaction/ is a sequence of
  database operations that satisfy the ACID properties.

  This note describes what constitutes a transaction in lsm-tree, and how we
  satisfy each of the ACID properties (and to what extent). Along the way, we
  also describe how we approach (a-)synchronous exception safety.

  Recall: an lsm-tree database comprises a single session, containing possibly
  many tables and cursors, which can be dependent (shared internal state) or
  completely independent (no shared internal state).



  ### Transactions

  lsm-tree does not have transactions in the typical sense that many relational
  databases have, where transactions can be built from smaller
  components/actions, e.g., reads and writes of individual cells. Instead,
  lsm-tree's public API only exposes functions that individually form a
  transaction; there are no smaller building blocks. An example of such a
  transaction is 'updates'.

  An lsm-tree transaction still perform multiple actions /internally/, but
  transactions themselves are not composable into larger transactions, so it
  should be expected that table contents can change between transactions. If a
  consistent view of a table /is/ required so that independent transactions have
  access to their own version of the database state, then a read-write view can
  be obtained using 'duplicate', and a read-only view can be obtained using
  'newCursor'.

  We make a distinction between three types of transactions. These types of
  transactions each have different usage requirements, depending on whether they
  create, use or release /stateful handles/.

  DEFINITION [Stateful handle]: a stateful handle is an in-memory structure that
    governs any number of system resources. These system resources include
    amongst others: file locks, open file handles.

  DEFINITION [Acquisition transaction]: an acquisition transaction is a
    transaction that acquires a new stateful handle. These transactions include:

    * openSession
    * new
    * newCursor
    * newCursorAtOffset
    * openSnapshot
    * duplicate
    * union

  DEFINITION [Usage transaction]: a usage transaction is a transaction that uses
    a stateful handle. This does /not/ include transactions that close stateful
    handles. These transactions do include:

    * lookups
    * rangeLookup
    * readCursor
    * inserts
    * deletes
    * mupserts
    * updates
    * retrieveBlobs (TODO: is this is a special type of transaction? Explain why
        blob references are not stateful handles in the strict sense)
    * createSnapshot
    * deleteSnapshot
    * listSnapshots

  DEFINITION [Release transaction]: A release transaction is a transaction that
    releases a stateful handle. These transactions include:

    * closeSession
    * close
    * closeCursor

  The management of stateful handles is up to the user, and so they must be
  properly managed in the presence of (a-)synchronous exceptions. The
  requirements are that (i) each acquisition transaction should be paired with
  the proper release transaction in such a way that the release transaction is
  guaranteed to run even in the presence of (a-)synchronous exceptions, and that
  (ii) acquisition and release transactions should be run with exceptions
  masked. Bracket-style functions such as 'withSession' and 'withTable' are
  provided by lsm-tree. If those helper functions are not sufficient, then it is
  the responsibility of the user to guarantee exception safety when using
  acquisition and release transactions directly.

  As a fallback, sessions will keep track of open tables and cursors, such that
  they can be closed once the session is closed. Of course, if the session is
  not properly closed, then the fallback will not trigger either. Moreover, this
  fallback does not provide prompt cleanup of stateful handles, which would be a
  problem for long-lived programs.

  TODO: can we really call 'openSession' and 'closeSession' transactions?
  TODO: reconsider names



  ### Atomicity

  DEFINITION [Atomicity]: atomicity guarantees that each transaction is treated
    as a single "unit", which either succeeds completely or fails completely: if
    any of the statements constituting a transaction fails to complete, the
    entire transaction fails and the /database/ is left unchanged.

  In our case, the term /database/ is ambiguous. It can refer to the /pyshical
  database/ or the /logical database/. For LSM trees in general, different
  states of a physical database can correspond to a single state of the logical
  database. LSM trees may run a compaction algorithm at any point, which reduces
  the number of on-disk files, but it does not modify the logical contents of
  the database. This means we can be more lenient in our definition of
  atomicity. This definition also increases opportunities for concurrency,
  because we can sometimes run compaction without worrying that the logical
  database contents change.

  DEFINITION [Atomicity revisited]: ... and the logical /database/ is left
    unchanged.

  A further simplifying factor is that lsm-tree only exposes complete
  transactions. Internally, the building blocks for transactions form a known
  set of /actions/, and each transaction is a known composition of a known
  subset of these actions. Our design for atomicity can therefore be simpler
  than the design for most relational databases.

  Moreover, LSM trees are log-structured, which means that on-disk data is not
  modified in place. Instead, files are created once and they are immutable
  until they are no longer referenced, at which point they become garbage
  collected.


  We facilitate atomicity by (i) recording side effects in an ActionLog so that
  side effects can be rolled. Some side effects are




-}





-- $concurrency
-- Table are mutable objects and as such applications should restrict their
-- concurrent use to avoid races.
--
-- It is a reasonable mental model to think of a 'Table' as being like a
-- @IORef (Map k v)@ (though without any equivalent of @atomicModifyIORef@).
--
-- The rules are:
--
-- * It is a race to read and modify the same table concurrently.
-- * It is a race to modify and modify the same table concurrently.
-- * No concurrent table operations create a /happens-before/ relation.
-- * All synchronisation needs to occur using other concurrency constructs.
--
-- * It is /not/ a race to read and read the same table concurrently.
-- * It is /not/ a race to read or modify /separate/ tables concurrently.
--
-- We can classify all table operations as \"read\" or \"modify\" for the
-- purpose of the rules above. The read operations are:
--
-- * 'lookups'
-- * 'rangeLookup'
-- * 'retrieveBlobs'
-- * 'createSnapshot'
-- * 'duplicate'
--
-- The write operations are:
--
-- * 'inserts'
-- * 'deletes'
-- * 'updates'
-- * 'close'
--
-- In particular it is possible to read a stable view of a table while
-- concurrently modifying it: 'duplicate' the table first and then perform reads
-- on the duplicate, while modifying the original table. Note however that it
-- would still be a race to 'duplicate' concurrently with modifications: the
-- duplication must /happen before/ subsequent modifications.
--
-- Similarly, a cursor constitutes a stable view of a table and can safely be
-- read while modifying the original table.
-- However, reading from a cursor will take a lock, so concurrent reads on the
-- same cursor will block until the first one completes. This is due to the
-- cursor position being updated as entries are read.
--
-- It safe to read a table (using 'lookups' or 'rangeLookup') concurrently, and
-- doing so can take advantage of CPU and I\/O parallelism, and thus may
-- improve throughput.
