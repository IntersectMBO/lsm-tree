% LSM specification: directory and file layouts
% Duncan Coutts

# Scope

This document is intend to cover the overall LSM directory format, including:

 * persistent snapshots
 * live handles
 * sufficient checksum/consistency info to detect directory corruption
   (not just individual file content corruption)
 * high-level configuration options persisted to files (if necessary)

This document is part of a group of documents that cover the formats:
 * `format-directory.md`: covering the overall LSM directory format
 * `format-run.md`: covering the LSM file-run format
 * `format-page.md`: covering the LSM file-run _page_ format


# Top level layout

The filesystem representation for an LSM session lives under a single directory
structure. We will describe all files relative to this `${session}` root.

The top level directory contains

 * `${session}/lock` file
 * `${session}/active/` directory
 * `${session}/snapshots/` directory

The top level lock file is used with an OS file lock API to (cooperatively)
ensure exclusive use of a session. It is otherwise empty.

# Active directory layout

The ephemeral directory contains all of the files for all the LSM runs that are
in use by any open LSM handle in the session.

 * `${session}/active/${n}.keyops`
 * `${session}/active/${n}.filter`
 * `${session}/active/${n}.index`
 * `${session}/active/${n}.checksums`

The LSM runs are numbered using a session-wide counter that is initialised from
0 each time a session is opened.

Note that there are no metadata files in the active directory (as there are for
snapshots). This is because the metadata is held in memory for open LSM handles.

# Snapshots directory layout

Each snapshot has a name in a session-wide namespace, and gets a corresponding
subdirectory:

 * `${session}/snapshots/${name}/` directory

Thus the limitations on portable file/directory names applies to snapshot names.

Each snapshot contains a metadata file for the snapshot overall and a checksum:

 * `${session}/snapshots/${name}/snapshot`
 * `${session}/snapshots/${name}/snapshot.checksum`

plus the five files for each LSM run in the snapshot:

 * `${session}/snapshots/${name}/${n}.keyops`
 * `${session}/snapshots/${name}/${n}.blobs`
 * `${session}/snapshots/${name}/${n}.filter`
 * `${session}/snapshots/${name}/${n}.index`
 * `${session}/snapshots/${name}/${n}.checksum`

In this case the LSM run files are numbered from 0 within each snapshot.

## Snapshot metadata

The snapshot metadata file contains the following information:
 * A format version/tag identifier
 * The stored parameters for the LSM table (specified when the table was
   originally created), including
   - LSM tuning parameters
   - Page size for all the key/operations files
   - Index type and parameters
 * Key/value type information for dynamic-type sanity checking
 * The shape of the LSM tree overall (runs within levels etc), referencing
   the numbered LSM runs.

There is a separate `snapshot.checksum` file containing the checksum of the
metadata file.

In particular, the snapshot metadata file tells us exactly which other LSM run
files to expect. This is important for detecting filesystem corruption.

# LSM run file layout

Each completed LSM run has five files:

 1. `${n}.keyops`: the sorted run of key/operation pairs
 2. `${n}.blobs`:  the blob values associated with the key/operations
 3. `${n}.filter`: a Bloom filter of all the keys in the run
 4. `${n}.index`:  an index from keys to disk page numbers
 5. `${n}.checksum`: a file listing the crc32c checksums of the other files

The format of the main four files is covered in `format-run.md`.

Each in-progress LSM run has only the main files, but no checksum file, and the
main files will be in some intermediate state of construction.

# Checksum files

Each `.checksum` file lists the CRC-32C (Castagnoli) of other files.

The file uses the BSD-style checksum format (e.g. as produced by tools like
`md5sum --tag`), with the algorithm name "CRC32C". This format is text,
one line per file, using hexedecimal for the 32bit output.

Checksum files are used for each LSM run, and for the snapshot metadata.
```
CRC32C (keyops) = fd040004
CRC32C (blobs) = 5a3b820c
CRC32C (filter) = 6653e178
CRC32C (index) = f4ec6724
```
```
CRC32C (snapshot) = 87972d7f
```
Note that the checksum file for LSM runs _does not_ include the full name of
each file, such as `0.keyops`. It excludes the number from the file name. The
number is instead implicit from the name of the checksum file, e.g.
`0.checksum` contains checksums for `0.*` files.

The reason for this kind of "relative" naming, is because when the LSM runs
are moved between active and snapshot, the number of the LSM run will change,
and by using relative names, we avoid having to rewrite the checksum files,
and can instead just hard link them.

# Verifying snapshots

The checksum of the snapshot metadata must be verified.

The snapshot metadata file then determines the number of LSM runs within the
snapshot. Each numbered run within the snapshot must have a checksum file,
which must list the other files.

The checksum of each LSM run file must be verified.

If verfification passes, this ensures all the files are present and have their
expected content. The use of CRCs protects against accidental corruption, not
deliberate corruption.

# Saving and restoring snapshots using hard links

Saving a snapshot involves a few steps:

 1. creating a _new_ directory for the snapshot
 2. writing out the snapshot metadata file and checksum
 3. hard linking all the LSM run files into the snapshot directory
 4. for durability, `fsync`:
   - all the LSM run files, including checksum files
   - metadata file and metadata checksum file
   - the snapshot directory itself

All five files for each LSM (including the checksum file) are hard-linked into
the snapshot directory under new number. The number of the file changes
because the LSM run numbers within a snapshot are counted from 0 within the
snapshot, whereas the run numbers in the active directory are shared across all
open LSM handles. For this reason, the checksum file content uses relative
file names, without mentioning the number.

Restoring a snapshot involves:

 1. verifying the snapshot (optional, depending on paranoia)
 2. loading the snapshot metadata
 3. hard linking all the LSM run files from the snapshot into the active
    directory

All five files for each LSM (including the checksum file) are hard-linked into
the active directory under a new number. The LSM runs in the active directory
are numbered sequentially across all open LSM handles.
