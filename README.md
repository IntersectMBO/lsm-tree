# lsm-tree

[![handbook](https://img.shields.io/badge/policy-Cardano%20Engineering%20Handbook-informational)](https://input-output-hk.github.io/cardano-engineering-handbook)
[![Haskell CI](https://img.shields.io/github/actions/workflow/status/IntersectMBO/lsm-tree/haskell.yml?label=Build)](https://github.com/IntersectMBO/lsm-tree/actions/workflows/haskell.yml)
[![Documentation CI](https://img.shields.io/github/actions/workflow/status/IntersectMBO/lsm-tree/documentation.yml?label=Documentation%20build)](https://github.com/IntersectMBO/lsm-tree/actions/workflows/documentation.yml)
[![Haddocks](https://img.shields.io/badge/documentation-Haddocks-purple)](https://IntersectMBO.github.io/lsm-tree/)

> :warning: **This library is in active development**: there is currently no
> release schedule!

`lsm-tree` is a project that is developed by Well-Typed LLP on behalf of Input
Output Global Inc (IOG). The main contributors are Duncan Coutts, Joris Dral,
Matthias Heinzel, Wolfgang Jeltsch, Wen Kokke, and Alex Washburn.

The `lsm-tree` library is a Haskell implementation of [log-structured
merge-trees](https://en.wikipedia.org/wiki/Log-structured_merge-tree), based on
[Monkey](https://dl.acm.org/doi/abs/10.1145/3035918.3064054), that provides an
on-disk key-value store.

It has a number of custom features that are primarily tailored towards
performant disk IO in the Haskell implementation of the Ouroboros-family of
consensus algorithms, which can be found in [the `ouroboros-consensus`
repository](https://github.com/IntersectMBO/ouroboros-consensus). Nevertheless,
this library should be generally useful for projects that require an on-disk
key-value store.

## System requirements

This library only supports 64-bit, little-endian systems. On Windows, the
library only works probably on drives with NTFS.

Provide the -threaded flag to executables, test suites and benchmark suites if
you use this library on Linux systems.
