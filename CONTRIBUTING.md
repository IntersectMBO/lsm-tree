# Contributing

## Installation requirements

Different OS distributions have different installation requirements. We recommend
using [GHCup](https://www.haskell.org/ghcup/) to manage your Haskell toolchain
(GHC, Cabal, etc.).

### Linux:
We recommend installing the `pkg-config` and `liburing` systems packages. These
are required for the high-performance asynchronous I/O backend.

* Ubuntu:
  ```bash
  sudo apt-get install pkg-config liburing-dev
  ```

If these packages are not installed, then the `serialblockio` cabal package flag
has to be enabled, either by setting the flag in
`cabal.project`/`cabal.project.local`, or by passing the flag to the `cabal`
executable using `--flag=+serialblockio`.

Installing `rocksdb` is entirely optional, and only required if one wants to
build or run the `utxo-rocksdb-bench` comparison macro-benchmark.

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

## Formatting and Linting

We use various tools to maintain code quality and consistency. See the helpful
scripts in the [scripts folder](./scripts/).

### Haskell Code
We use `stylish-haskell` for formatting and `hlint` for linting.
* Format: `./scripts/format-stylish-haskell.sh`
* Lint: `./scripts/lint-hlint.sh`

### Cabal Files
We use `cabal-fmt` for formatting and `cabal check` for sanity checks.
* Format: `./scripts/format-cabal-fmt.sh`
* Check: `./scripts/lint-cabal.sh`

### Other Linters
* Shell scripts: `./scripts/lint-shellcheck.sh`
* GitHub Actions: `./scripts/lint-actionlint.sh`

### Documentation
* Check Haddocks: `./scripts/generate-haddock.sh`

## Pull requests

The following are requirements for merging a PR into `main`:
* Each commit should be small and should preferably address one thing. Commit
  messages should be useful.
* Document and test your changes.
* The PR should have a useful description, and it should link issues that it
  resolves (if any).
* Changes introduced by the PR should be recorded in the relevant changelog
  files. Ideally, each changelog entry should link to the PR that introduced the
  changes, and it should have a `BREAKING`, `NON-BREAKING`, or `PATCH` level.
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

To publish a release for a package, follow the steps below:

* Changelog checks (`CHANGELOG.md`):
  * Check that all user-facing changes have been recorded.
  * Check that each changelog entry is in one of these sections: `Breaking
    changes`, `New features`, `Minor changes`, or `Bug fixes`.
  * Check that each changelog entry links to a PR, if applicable.
  * Add or update the changelog's section header with the package version that
    is going to be released, and the date of the release. The version should be
    picked based on our package versioning policy.

* Cabal file checks (`*.cabal`):
  * Update the `version` field.
  * Update the `tag` field of the `source-repository this` stanza.

* Cabal project file checks (`cabal.project*`):
  * Update the `index-state` in the `cabal.project.release` file to the current
    date-time, or the closest valid date-time to the current date-time, so that
    CI builds and tests the libraries with the newest versions of dependencies.
