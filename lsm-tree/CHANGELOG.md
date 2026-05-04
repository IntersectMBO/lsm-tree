# Revision history for `lsm-tree`

## Unreleased

### Breaking changes

* Move to snapshot version v2 due to changes to the internal table
  representation. While this change is backwards compatible (i.e. new code
  is still able to read old snapshots), be aware that v2 snapshots cannot be
  read by older versions of `lsm-tree`.
  See [PR #834](https://github.com/IntersectMBO/lsm-tree/pull/834).

### New features

### Full API

* Add a new `importSnapshot` function for importing snapshots from outside a
  session into that session. See [PR
  #835](https://github.com/IntersectMBO/lsm-tree/pull/835).
* Add a new `SnapshotImportDirDoesNotExistError` exception, which can currently
  only be thrown by thrown by `importSnapshot`. See [PR
  #835](https://github.com/IntersectMBO/lsm-tree/pull/835).
* Add a new `exportSnapshot` function for exporting snapshots from inside a
  session to outside that session. See [PR
  #835](https://github.com/IntersectMBO/lsm-tree/pull/835).
* Add a new `SnapshotExportDirExistsError` exception, which can currently only
  be thrown by thrown by `exportSnapshot`. See [PR
  #835](https://github.com/IntersectMBO/lsm-tree/pull/835).
* Add a new `withOpenMountedSessionIO` function, a variant of
  `withOpenSessionIO` with more general control over where snapshots can be
  exported to and imported from. See [PR
  #835](https://github.com/IntersectMBO/lsm-tree/pull/835).

### Minor changes

* Update to `fs-sim ^>=0.5`. See PR
  [#845](https://github.com/IntersectMBO/lsm-tree/pull/845).

### Full API

* Deprecate `withOpenSessionIO` in favour of `withOpenMountedSessionIO`.
  `withOpenSessionIO` is generally unsafe to use with the new `importSnapshot`
  and `exportSnapshot` functions. See [PR
  #835](https://github.com/IntersectMBO/lsm-tree/pull/835).

### Bug fixes

None

## 1.0.0.2 -- 2026-04-24

### Breaking changes

None

### New features

None

### Minor changes

* Support `io-classes ^>=1.9` and `^>=1.10`. See [PR
  #819](https://github.com/IntersectMBO/lsm-tree/pull/819).
* Support `ghc-9.14`. See [PR
  #836](https://github.com/IntersectMBO/lsm-tree/pull/836).
* Support `containers-0.8`. See [PR
  #836](https://github.com/IntersectMBO/lsm-tree/pull/836).

### Bug fixes

* Fix a bug where `lookups` with a large number of input keys would sometimes
  return incorrect lookup results. See [PR
  #841](https://github.com/IntersectMBO/lsm-tree/pull/841).

## 1.0.0.1 -- 2025-12-03

* PATCH: support `filepath-1.4`. See PR
  [#804](https://github.com/IntersectMBO/lsm-tree/pull/804).

## 1.0.0.0 -- 2025-08-06

* First released version.
