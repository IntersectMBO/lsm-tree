# Contributing

## Installation requirements

Different OS distributions have different installation requirements.

### Linux:
We recommend installing the `pkgconfig` and `liburing` systems packages, though
they are not required. However, one would not get the performance benefits of
performing I/O asynchronously.

* Ubuntu:
  ```
  apt-get install pkg-config liburing-dev
  ```

If these packages are not installed, then the `serialblockio` cabal package flag
has to be enabled, either by setting the flag in
`cabal.project`/`cabal.project.local`, or by passing the flag to the `cabal`
executable using `--flag=+serialblockio`.

> :warning: **When enabling `serialblockio`, disable the
> `cabal.project.blockio-uring` import in `cabal.project`!** Unfortunately, this
> line has to be removed/commented out manually (for now), or the project won't
> build.

Installing `rocksdb` is entirely optional, and only required if one wants to
build or run the `rocksdb-bench-wp8` comparison macro-benchmark.

* Ubuntu:
  ```
  apt-get install librocksdb-dev
  ```

If this package is not installed, then the `rocksdb` cabal package flag has to
be disabled, either by setting the flag in
`cabal.project`/`cabal.project.local`, or by passing the flag to the cabal
executable using `--flag=-rocksdb`

### MacOS

There are no installation requirements.

### Windows

There are no installation requirements.

## Building

The project is built using `ghc` and `cabal`.

```
cabal update
cabal build all
```

## Testing

Tests are run using `cabal`.

```
cabal build all
cabal test all
```

## Code style

There is no strict code style, but try to keep the code style consistent
throughout the repository and favour readability. Code should be well-documented
and well-tested.

## Formatting

We use `stylish-haskell` to format Haskell files, and we use `cabal-fmt` to
format `*.cabal` files. We also use `cabal check` to sanity check our cabal
files. See the helpful scripts in the [scripts folder](./scripts/), and the
[`stylish-haskell` configuration file](./.stylish-haskell.yaml).

To perform a pre-commit code formatting pass, run one of the following:

  *  If you prefer `fd` and have it installed on your system:
     ```
     ./format-stylish-fd.sh
     ./format-cabal-fd.sh
     ./check-cabal.sh
     ./haddocks.sh
     ```

  *  Otherwise using Unix `find`:
     ```
     ./format-stylish-find.sh
     ./format-cabal-find.sh
     ./check-cabal.sh
     ./haddocks.sh

## Pull requests

The following are requirements for merging a PR into `main`:
* Each commit should be small and should preferably address one thing. Commit
  messages should be useful.
* Document and test your changes.
* The PR should have a useful description, and it should link issues that it
  resolves (if any).
* Changes introduced by the PR should be recorded in the relevant changelog
  files.
* PRs should not bundle many unrelated changes.
* PRs should be approved by at least 1 code owner.
* The PR should pass all CI checks.

## Releases

Releases follow the [Haskell Package Versioning
Policy](https://pvp.haskell.org/). We use version numbers consisting of 4 parts,
like `A.B.C.D`.
* `A.B` is the *major* version number. A bump indicates a breaking change.
* `C` is the *minor* version number. A bump indicates a non-breaking change.
* `D` is the *patch* version number. A bump indicates a small, non-breaking
  patch.
