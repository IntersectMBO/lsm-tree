# Contributing

## Building

The project is built using `ghc` and `cabal`, and does not require any external
dependencies.

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