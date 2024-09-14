% LSM specification: LSM file-run format
% Duncan Coutts

# Scope

This document is intended to cover the format of the files used to represent an
individual LSM run, including:

* any header for metadata
* bloom filters
* indexes
* key/operation pages (but the page format is covered in `format-page.md`)
* blob files for large additional values (blobs can vary in size, but we can
  design performance for a range, e.g. 1-16kb, and can impose an upper bound)
* separate files for: k/ops, blobs, and index & filter
* checksums for all files

This document is part of a group of documents that cover the formats:
 * `format-directory.md`: covering the overall LSM directory format
 * `format-run.md`: covering the LSM file-run format
 * `format-page.md`: covering the LSM file-run _page_ format

# LSM runs

Each LSM run consists of four components:

1. the sorted run of key/operation pairs
2. the blobs associated with the key/operation pairs, if any
3. a Bloom filter of all the keys in the run
4. an index from keys to disk page numbers

Each one is represented on-disk in a separate file. Thus there are four files
per run.

## Represented in memory or on-disk

The bloom filter and index are loaded into memory when the LSM run is in use.
The key/operations and blob files remain primarily on disk and pages are loaded
as needed for lookup and merge operations.

## Inherited metadata

Metadata is kept at the LSM handle level, not per-run. This is because we
expect all metadata to be the same for all runs in an LSM tree.

There are some bits of metadata from the LSM handle that we rely on for
interpreting or creating the LSM run files:

* Page size: the key/operations file is structured as pages for efficient I/O.
  These pages will typically be 4k, but the format could support 8k, 16k, 32k
  or 64k pages.
* Index type: in principle we could support multiple index representations,
  to suit different kinds and properties of key (e.g. fixed size, uniformly
  distributed). Initially we will support a compact index type suited to
  uniformly distributed keys of variable size but with at least 64bits.

## Key/operations file

This file contains all the key/operation/blobref entries for the LSM run,
sorted by key and arranged into pages. The individual page format is detailed
in `format-page.md`.

The overall file format consists exclusively of these pages, with no additional
metadata or file headers. The page size will typically be 4k but can be any
power of 2 up to 64k.

The run is sorted by key, based on the usual key byte string representation.
The number of each disk page within the file is significant, as this page
number is what is stored within the LSM run index, to enable easily finding the
right disk page(s) to read for a lookup operation. Pages are numbered from 0.

The key/operations are organised into pages in the following way:

* All the keys in a page must have the same "range finder" bits, if a compact
  index is in use. For code uniformity, if a compact index is not used then
  this is equivalent to using 0 range finder bits. This restriction is needed
  to support the compact index representation.

  When packing key/operations into pages linearly, the initial key bits can be
  tracked and when they change from one key to the next, then a page boundary
  must be used, at the expense of wasting any remaining space in the page. The
  range finder bits are chosen to not cause excessive waste.

* Otherwise, key/operation/blobrefs are packed into pages so that as many fit
  as possible, given the size limit of the page format. The details of the size
  of pages is given in `format-page.md`.

* For the special case of N=1, the value can be large and span multiple pages.

* In all other cases, N>1, all entries must fit within a single page.

### Maximum key and value sizes

The maximum key size is limited by what can fit within a page, thus the
maximum key size depends on the page size. The page size overhead (derived in
`format-page.md`) is 44 bytes. We can however round to more "sensible" looking
numbers.

* 4k pages:  2^12-44, rounded: 4,000
* 8k pages,  2^13-44, rounded: 8,000
* 16k pages, 2^14-44, rounded: 16,000
* 32k pages, 2^15-44, rounded: 32,000
* 64k pages, 2^16-44, rounded: 64,000

The maximum value size is 2^32-1 minus the page overhead, and conservatively
assuming a maximum sized key with maximum page size. This is approximately
2^32 - 2^16 - 1 = 4294901759, though this limit could also be rounded down in
a public API.

## Blobs file

Each key/operation in the key/operations file optionally contains a blob
reference. A blob reference is of course a reference to a blob value, which
is simply a sequence of bytes. These blob values are stored in the blobs file.

The representation of a blob reference is a 64bit file offset (in bytes) and a
32bit length (in bytes). This identifies a span of bytes within this blob file.

The file format is simple: the concatenation of all the blob values, in the
same order as the key/operations that refer to the blobs (which is sorted by
key). There is no additional padding or alignment.

This simple format supports writing out the LSM run incrementally, as entries
are added sequentially in key order. Blobs are retrieved simply by reading the
blob span into memory.

To keep things simple and uniform, there is no special case for the empty
sequence of blobs: it is represented by an empty blobs file.

## Bloom filter file

The format of this file consists of a header, followed by the bloom filter bit
vector.

The first 4 bytes (32bit) are a format identifier / format version. This
determines the format of the rest of the header and file.
The format identifier also acts as endianess marker.
It (and remaining fields) are serialised in native byte order.

The remainder of the header for format 1 consists of:
 1. The hash function count (32bit)
 2. The bit size of the filter (64bit)

The fields of the header are serialized in native byte order.

The maximum filter size is 2^48 bits, corresponding to 32 Terabytes.
The family of hash functions to use is implied by the format version.

The filter bit vector itself is organised as a whole number of 64 bit words.
It follows immediately after the header. The endianness from the header
indicates the endianness of the 64 bit words.

## Index file

The index maps keys to the number of the disk page(s) in the key/operation file
that _could_ contain this key.

That is, the index says where the key could possibly be, and thus what pages
to load from disk to search for the key. The index does not say whether a key
exists or not (the bloom filter does this, albeit probabilistically, with false
positives).

For the case of large values that require multiple pages, the index must report
the span of disk pages that the key/operation may be on. This allows the whole
range of pages to be read from disk in one go. (An alternative design would be
for the first page to indicate that it is a multi-page value, and then require
a second round of disk I/O to read in the remaining pages.)

The design is intended to be somewhat extensible by allowing for different
choices of index representation, to suit different kinds of key and key
properties. In particular initially we will support a compact index type.
For more general purpose applications one can foresee the need for a more
ordinary index type.

The type of index in use is metadata that must be known for the whole LSM table
handle. It does not vary per run.

Independently of the concrete index serialization format, the type and the
version of the format are stored at the beginning as a 32-bit number called the
type–version indicator. A type–version indicator has the following shape, where
bit numbers refer to significance:

* Bits 0–7 contain the version number.
* Bits 8–15 contain a number denoting the type of the index.
* Bits 16–31 are zero.

As a result, a type–version indicator can be written in hexadecimal with only
4-digits of which the first two denote the type and the last two constitute the
version number. Version numbers are assigned independently for each particular
type. They must not be zero, which ensures that from the way the type–version
indicator is stored one can determine whether the concrete serialised index
stores numbers in little-endian or big-endian.

### Compact index

The compact index type is designed to work with keys that are large
cryptographic hashes, e.g. 32 bytes. In particular it requires:

* keys must be uniformly distributed
* keys must be at least 8 bytes (64bits), but can otherwise be variable length

For this important special case, we can do significantly better than storing a
whole key per page: we can typically store just 8 bytes (64bits) per page. This
is a factor of 4 saving for 32 byte keys.

The type of compact indexes is denoted by the number 0. The type–version
indicator in a serialised compact index is padded to 64 bits.

For version 1, the representation after the type–version indicator consists of
1. a primary array of 64bit words, one entry per page in the index
2. a clash indicator bit vector, one bit per page in the index
3. a larger-than-page indicator bit vector, one bit per page in the index
4. a clash map, mapping each page with a clash indicator to the full minimum
   key for the page
5. a footer

The footer consists of the last 16 bytes of the file
1. the number of pages in the primary array (64bit)
2. the number of keys in the corresponding key/ops file (64bit)

The file format consists of each part, sequentially within the file. This
format can in-part be written out incrementally as the index is constructed.
The primary array is the largest component, and this is the part that can be
written out to disk incrementally. All the remaining parts can only be written
upon the completion of the index. These parts must be kept in memory while the
index is constructed and can be flushed upon completion.

The rationale for the footer being at the end of the file is that it
means they are at a known offset relative to the end of the file, and can be
read first. This helps with pre-allocating the memory needed for the
in-memory representation of the other main components, and knowing how much
data to read from disk for each component.

The clash map is expected to be very small, so its file format is mainly
designed to be simple, not compact.

The alignment of the components is arranged such that it would be possible (if
desired) to mmap the whole file and access almost all of the components with
natural alignment. The clash map however is not a simple flat array, so it is
is expected to be decoded, rather than to be accessed in-place.

|     |                 | elements   | size  | alignment | trailing padding to |
|-----|-----------------|------------|-------|-----------|---------------------|
| 0   | type, version   | 1          | 32bit | 32bit     | 64bit (at the end)  |
| 1   | primary array   | n          | 64bit | 64bit     |                     |
| 2   | clash indicator | ceil(n/64) | 64bit | 64bit     |                     |
| 3   | LTP indicator   | ceil(n/64) | 64bit | 64bit     |                     |
| 4.1 | clash map size  | 1          | 64bit | 64bit     |                     |
| 4.2 | clash map       | s          |       | 64bit     | 64bit (each entry)  |
| 5.1 | number pages    | 1          | 64bit | 64bit     |                     |
| 5.2 | number keys     | 1          | 64bit | 64bit     |                     |

For the clash map, after its size s, each pair of key and page number is
serialised in the following order:
1. the page number (32bit)
2. the length of the key in bytes (32bit)
3. the key, with trailing padding to 64 bit alignment

### Ordinary index

The ordinary index type is intended to cover a general use case and not have any
special constraints on the key. It supports serialised keys of variable length
and makes worst case assumptions about key distribution.

An ordinary index contains for each page the key stored last in this page or, if
the page is an overflow page, the key of the corresponding key–value pair. From
this, the following constraints on the sequence of stored keys follow:
* The sequence must be non-empty.
* The elements of the sequence must be non-decreasing.

The type of compact indexes is denoted by the number 1. The type–version
indicator in a serialised ordinary index is not padded.

For version 1, the file contents after the type–version indicator consist of the
following:
1. A sequence of keys, where each key is stored as a 16 bit number stating the
   length of its serialised form followed by the serialised form itself
2. A footer, which just consists of a 64 bit number that states the number of
   keys in the corresponding run

The sequence of keys can be written out incrementally, while the footer can only
be written upon completion of the index.

No provisions regarding alignment are made. All parts of the index are stored
sequentially without any gaps.

|   |                | elements   | size   |
|---|----------------|------------|--------|
| 0 | type, version  | 1          | 32 bit |
| 1 | key sequence   | n          |        |
| 2 | number of keys | 1          | 64 bit |

For the key sequence, each key is represented by the following components:
1. The length of the serialised key (16 bit)
2. The serialised key
