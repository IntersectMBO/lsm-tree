% LSM specification: LSM file-run page format
% Duncan Coutts

# Scope

This document is intended to cover the format of the pages within the
key/operation file of each LSM run, including:

* page header for metadata
* "normal" pages of k/op pairs
* encoding of insert/delete/update operations
* encoding of monoidal update operations
* encoding of references to blobs

This document is part of a group of documents that cover the formats:
 * `format-directory.md`: covering the overall LSM directory format
 * `format-run.md`: covering the LSM file-run format
 * `format-page.md`: covering the LSM file-run _page_ format

# Page format considerations

The key/operations file encodes the main part of the LSM key value mapping.
Logically, a page consists of a value of type
```
type Page = [(Key, Operation, Maybe BlobRef)]
```
where the operation can be insert (with a value), delete (with no value) or
upsert (with a value). The entries are sorted by key.

The file is structured in a page-oriented way to support efficient I/O using
random page-sized reads. By page-oriented we mean that the information is
structured to be read in units of whole pages, one or more.

The literature suggests that 4k pages are the best default choice for databases
for current hardware for random lookups, and as such the page size used in the
file is 4k. Typically we expect a single 4k page to contain several
key/operation pairs, but there is support for "overflow" pages, where a single
value is so large that it needs multiple pages. Keys however are limited in
size, and cannot exceed a single 4k page.

The page format should be designed to support the LSM lookup operation
efficiently. Lookup works by loading the 4k page on which the key (probably)
exists, and then searching for the key/operation pair within the page. Thus an
efficient design should minimise any decoding overhead, and enable efficient
search. The approach we take is to design the page format to require no copying
or allocation with minimal computation for decoding, and to keep the keys
together in a sorted dense list.

We assume that both keys and values are byte strings of variable size.

For the special case of a single value that spans multiple pages, the number of
pages will be discovered during the lookup operation from the index and so all
the required pages will be read in at once. The pages ready in do not need to
be contiguous in memory.

## Representation sizes

The concrete representation is chosen to balance a couple factors: speed of
access (for lookup operations) and size. We choose to make all values be
naturally aligned relative to the page, e.g. 2-byte values at 2-byte offsets.
This means that if a page from disk is loaded into a 4k aligned page in memory
(as is required anyway for `O_DIRECT`) then all memory access will be naturally
aligned, and thus it will be unnecessary to use unaligned memory operations.

For offsets within the page, we can use 16 bits. Since 4k is 2^12, this leaves
4 bits spare. This could be used to allow for larger pages, up to 64k.

# Page format representation

A page consists of:

1. A directory of the other components
   - 8-byte size
2. An array of 1-bit blob reference indicators
   - a packed array (i.e. a bitmap), 1 element per key
   - padded to 8-byte size
3. An array of 2-bit operation types
   - a packed array, 1 element per key
   - padded to 8-byte size
4. A pair of arrays of blob references
   - each blob reference is a 64bit offset and 32bit length
   - an array of 64bit offset values
   - an array of 32bit length values
5. An array of key offsets
   - 16bit offsets (to start of each key), 1 element per key
6. An array of value offsets
   - 16bit offsets (to start of each value), 1 element per key (not value)
   - +1 last offset for offset 1 past last value
   - special case when the page contains just 1 key/op pair: the last offset
     is 32bit, not 16bit. So overall in this case there are two offsets, a
     16bit start and 32bit end offset, each naturally aligned
7. The concatenation of all keys
   - the byte span for individual keys is derived from the offsets array
8. The concatenation of all values
   - the byte span for individual value is derived from the offsets array
   - for the 1-element case, this value may span multiple overflow pages

The somewhat odd order of the entries is due to representation issues:

* we want to allow for natural memory address alignment of all data
* we also want compact representation without too much space wasted to padding
* we want to take advantage of cache line loads
* we want to keep offset computation fast but not have to store lots of offsets

## Page directory

The page directory stores enough information to compute the offsets of all
components of the page structure. Specifically it stores

1. N: the number of key/op pairs in this page
2. B: the number of blobrefs in this page
3. KO: offset to the array of key offsets (component 5)
4. unused/spare

Each of these is represented as a 16bit value, for a total of 64bits.

The size of each component (in bytes) is as follows:
1. 8
2. (N + 63) `shiftR` 3 .&. complement 0x7   (N bits rounded up to 64)
3. (2*N + 63) `shiftR` 3 .&. complement 0x7 (2N bits rounded up to 64)
4. 12*B                                     (where B is number of blobrefs)
5. 2*N
6. 2*(N+1) when N>1
   6       when N=1
7. sum (map length keys)
8. sum (map length values)

The page offset of each component can be computed as follows:

1. 0
2. 8
3. 8 + ((N + 63) `shiftR` 3 .&. complement 0x7)
4. (stored offset KO) - 12*B
5. (stored offset KO)
6. (stored offset KO) + 2*N
7. entry 0 in array of key offsets
8. entry 0 in array of value offsets

## Page size tracking

When packing key/operation/blobref tuples into pages, we need to be able to
accurately predict the size of the serialised representation to make sure that
we do not overflow a page.

The size can be tracked incrementally as follows:

Start with N = 0, B = 0, size = 10 bytes.

When adding an element (k, op(v), b):

* Increment N by 1
* Increment B by 1 if a blobref is present
* Increment size by the sum of:
   + 8 if N mod 64 == 0
   + 8 if N mod 32 == 0
   + 12 if blobref is present
   + 2
   + 4 if N==0, 0 if N==1 and 2 otherwise
   + len k
   + len v  (or 0 if op is delete)

## Page size overheads

The biggest key that can fit into a page is determined by the overheads for the
page representation.

We can calculate this. If conservatively assume a blobref is present.

The page size formula above gives us the sum of:
 + 10
 + 8
 + 8
 + 12
 + 2
 + 4

Which is 44 bytes, not including the key itself. Thus the maximum key size can
be the page size minus 44 bytes.

## Blob reference indicators

This is a bitmap with a bit for each key/operation pair, indicating whether
there is an associated blob reference or not.

The bitmap is aligned and padded to 64 bits so that the bitmap operation can
use 64 bit word operations. The bitmap is arranged in words of 64 bits, in
little endian format.

The bitmap is indexed by taking the query index, shifting right 6 bits to
obtain which 64bit word to inspect, and then within the 64bit word, selecting
bit i where i is the lower 6 bits of the query index. The selected bit
indicates whether a blob reference is present for this key.

## Operation types

This is a bitmap with 2 bits for each key/operation pair, indicating the type of
operation for that key/operation pair.

The bitmap is aligned and padded to 64 bits. The bitmap is arranged in words of
64 bits, in little-endian format.

The bitmap is indexed by taking the query index, shifting right 5 bits to obtain
which 64-bit word to expect, and then within the 64bit word, selecting bit (2 \*
i) and bit (2 \* i + 1) where i is the lower 5 bits of the query index. The
selected bits indicate the operation type for this key.

For each 2-bit pair in little-endian format, the mapping is:

```
00 -> Insert
10 -> Mupsert
01 -> Delete
```

## Blob reference arrays

Each blob reference is a 64bit byte offset and a 32bit byte length. This
identifies a span of bytes within the LSM run blob file associated with this
key/operations file.

The pair of blob reference arrays gives the actual blob reference values. They
only contain entries for keys that have blob references. It is organised as a
pair of arrays, rather than an array of pairs. This is because the two
components of a blob reference are 64bit and 32bit and using a pair of arrays
representation gives natural alignment without any padding.

If the blob reference bitmap indicates that there is a blob reference for key i,
then the index in the blob reference array can be found by counting the number
of 1 bits for all bitmap indexes stricly less than i. This can be done
relatively efficiently using the popcount CPU instruction. For example, pages
with up to 64 keys, the bitmap fits into a single 64bit word and thus a single
popcount instruction, (and some bit shifting and masking) is needed to find the
index of the blob reference.

## Key offsets

This is an array of 16bit values. There are N entries. Entry i (indexed from 0)
gives the offset within the page of the start of the key byte string. Entry i+1
therefore gives the offset one past each key. This provides a span for each key:
each one represented by an inclusive lower offset and an exclusive upper offset.
For the final key, key N-1, we can still use N-1 and N, even though there are
only N entries, because we arrange that the value offset array comes
immediately after the key offset array, and the first entry of the value offset
array will be one past the final key (because the value byte strings themselves
come immediately after the key byte strings).

## Value offsets

Except for the special case of N=1, this is an array of 16bit values, with N+1
entries. In the usual case, we can find the value byte string span from index
i and i+1. As above, the spans are represented by an inclusive lower offset and
an exclusive upper offset.

For the special case of N=1, the representation is two offsets, but the second
offset is 32bit rather than 16bit. This is to allow for very large values that
take more than one page (or indeed more than 64k). As with all other spans, it
uses inclusive lower and exclusive upper representation.

Note that the 32bit value will still be naturally aligned, because the key
offset array is aligned to 32bit, and for N=1, the value offset array will be
at a 16bit but not 32bit alignment, and therefore after the 16bit entry in the
value offset array, we get back to 32bit alignment.

This combination of using only N rather than N+1 key offsets, and having the
N=1 special case use 16 + 32bit is a bit of a cunning hack, but it means we
need no extra padding and get natural alignment. This makes page size
calculations simpler and saves space. There is only a modest complexity cost
to the N=1 special case.

## Key and value byte strings

This is the concatenation of all the key byte strings, followed by all the value
byte strings. The spans are given by the key and value offset arrays.

Note that the concatenation of keys must fit within the first page, but for the
special case of N=1, the value may span multiple pages.

This restriction allows the key search to be simple by not having to deal with
non-contiguous memory pages. For the special case of N=1 where multiple pages
need to be read in from disk, the pages do not need to be contiguous in memory.
This allows the I/O buffer strategy to be simple by only having to deal with
single pages. Then the only code that has to deal with page boundaries is
copying the value byte string.

