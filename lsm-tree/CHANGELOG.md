# Revision history for `lsm-tree`

## Unreleased

### Breaking changes

None

### New features

None

### Minor changes

* Support `io-classes ^>=1.9` and `^>=1.10`. See PR
  [#819](https://github.com/IntersectMBO/lsm-tree/pull/819).

### Bug fixes

* Fix a bug where `lookups` with a large number of input keys would sometimes
  return incorrect lookup results. See [PR
  #841](https://github.com/IntersectMBO/lsm-tree/pull/841).

## 1.0.0.1 -- 2025-12-03

* PATCH: support `filepath-1.4`. See PR
  [#804](https://github.com/IntersectMBO/lsm-tree/pull/804).

## 1.0.0.0 -- 2025-08-06

* First released version.
