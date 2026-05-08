# Revision history for blockio

## Next version -- unreleased

### Breaking changes

* Update to `fs-sim ^>=0.5`. See [PR
  #845](https://github.com/IntersectMBO/lsm-tree/pull/845).
* `IOCtxParams` has a new record field for configuring the use of IOWAIT
  metrics. Like the other configuration options, this option only applies on
  Linux platforms. See [PR
  #846](https://github.com/IntersectMBO/lsm-tree/pull/846). For more information
  about the new option, see the documentation for `blockio-uring`.

### New features

None

### Minor changes

* Support `blockio-uring ^>= 0.2`, and drop support for `blockio-uring ^>= 0.1`.
  See [PR #846](https://github.com/IntersectMBO/lsm-tree/pull/846)

### Bug fixes

None

## 0.1.1.2 -- 2026-04-24

### Breaking changes

None

### New features

None

### Minor changes

* Support `io-classes ^>=1.9` and `^>=1.10`. See [PR
  #819](https://github.com/IntersectMBO/lsm-tree/pull/819).
* Support `ghc-9.14`. See [PR
  #836](https://github.com/IntersectMBO/lsm-tree/pull/836).

### Bug fixes

None

## 0.1.1.1 -- 2025-12-10

* PATCH: Support `unix-2.8.4`. See PR
  [#807](https://github.com/IntersectMBO/lsm-tree/pull/807).

## 0.1.1.0 -- 2025-12-03

* NON-BREAKING: Support `unix-2.8.6` by vendoring code related to file caching
  from the `unix` package. See PR
  [#805](https://github.com/IntersectMBO/lsm-tree/pull/805).

## 0.1.0.1 -- 2025-07-15

* PATCH: Include the README in the distributed files. See PR
  [#787](https://github.com/IntersectMBO/lsm-tree/pull/787).

## 0.1.0.0 -- 2025-07-15

* First version. Released on an unsuspecting world.
