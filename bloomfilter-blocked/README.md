# bloomfilter-blocked

`bloomfilter-blocked` is a Haskell library providing multiple fast and efficient
implementations of [bloom filters](https://en.wikipedia.org/wiki/Bloom_filter).
It is a full rewrite of the
[`bloomfilter`](https://hackage.haskell.org/package/bloomfilter) package,
originally authored by Bryan O'Sullivan <bos@serpentine.com>.

A bloom filter is a space-efficient data structure representing a set that can
be probablistically queried for set membership. The set membership query returns
no false negatives, but it might return false positives. That is, if an element
was added to a bloom filter, then a subsequent query definitely returns `True`.
If an element was *not* added to a filter, then a subsequent query may still
return `True` if `False` would be the correct answer. The probabiliy of false
positives -- the false positive rate (FPR) -- is configurable.

The library includes two implementations of bloom filters: classic, and blocked.

* **Classic** bloom filters, found in the `Data.BloomFilter.Classic` module: a
  default implementation that is faithful to the canonical description of a
  bloom filter data structure.

* **Blocked** floom filters, found in the `Data.BloomFilter.Blocked` module: an
  implementation that optimises the memory layout of a classic bloom filter for
  speed (cheaper CPU cache reads), at the cost of a slightly higher FPR for the
  same amount of assigned memory.

The FPR scales inversely with how much memory is assigned to the filter. It also
scales inversely with how many elements are added to the set. The user can
configure how much memory is asisgned to a filter, and the user also controls
how many elements are added to a set. Each implementation comes with helper
functions, like `sizeForFPR` and `sizeForBits`, that the user can leverage to
configure filters.

Both immutable (`Bloom`) and mutable (`MBloom`) bloom filters, including
functions to convert between the two, are provided for each implementation. Note
however that a (mutable) bloom filter can not be resized once created, and that
elements can not be deleted once inserted.

For more information about the library and examples of how to use it, see the
Haddock documentation of the different modules.

# Usage notes

User should take into account the following:

* This package is not supported on 32bit systems.

# Differences from the `bloomfilter` package

The library is a full rewrite of the
[`bloomfilter`](https://hackage.haskell.org/package/bloomfilter) package,
originally authored by Bryan O'Sullivan <bos@serpentine.com>. The main
differences are:

* `bloomfilter-blocked` supports both classic and blocked bloom filters, whereas
  `bloomfilter` only supports the former.
* `bloomfilter-blocked` supports bloom filters of arbitrary sizes, whereas
  `bloomfilter` limits the sizes to powers of two.
* `bloomfilter-blocked` supports sizes up to `2^48` for classic bloom filters
  and up to `2^41` for blocked bloom filters, instead of `2^32`.
* In `bloomfilter-blocked`, the `Bloom` and `MBloom` types are parameterised
  over a `Hashable` type class, instead of having a `a -> [Hash]` typed field.
  This separation in `bloomfilter-blocked` allows clean (de-)serialisation of
  filters as the hashing scheme is static.
* `bloomfilter-blocked` uses [`XXH3`](https://xxhash.com/) for hashing instead
  of [Jenkins'
  `lookup3`](https://en.wikipedia.org/wiki/Jenkins_hash_function#lookup3), which
  `bloomfilter` uses.
* The user can configure hash salts for improved security in
  `bloomfilter-blocked`, whereas this is not supported in `bloomfilter`.
