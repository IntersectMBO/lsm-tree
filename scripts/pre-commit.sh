#!/bin/sh

# To install as a Git pre-commit hook, run:
#
# > ln scripts/pre-commit.sh .git/hooks/pre-commit.sh
#

# POSIX compliant method for 'pipefail':
fail=$(mktemp)

# Check for unstaged changes in Haskell files
unstaged_haskell_files="$(git ls-files --exclude-standard --no-deleted --deduplicate --modified '*.hs' || echo > "$fail")"
if [ ! "${unstaged_haskell_files}" = "" ]; then
    echo "Found unstaged Haskell files"
    echo "${unstaged_haskell_files}"
fi

# Check for unstaged changes in Cabal files
unstaged_cabal_files="$(git ls-files --exclude-standard --no-deleted --deduplicate --modified '*.cabal' || echo > "$fail")"
if [ ! "${unstaged_cabal_files}" = "" ]; then
    echo "Found unstaged Cabal files"
    echo "${unstaged_cabal_files}"
fi

# Lint GitHub Actions workflows with actionlint
./scripts/lint-actionlint.sh || echo > "$fail"
echo

# Format Cabal files with cabal-fmt
./scripts/format-cabal-fmt.sh || echo > "$fail"
echo

# Format Haskell files with stylish-haskell
./scripts/format-stylish-haskell.sh || echo > "$fail"
echo

# Lint GitHub Actions workflows with actionlint
./scripts/lint-actionlint.sh || echo > "$fail"
echo

# Lint Cabal files with cabal
./scripts/lint-cabal.sh || echo > "$fail"
echo

# Lint Haskell files files with HLint
./scripts/lint-hlint.sh || echo > "$fail"
echo

# Generate README.md from package description
./scripts/generate-readme.hs || echo > "$fail"
echo

# Check whether or not any subcommand failed:
if [ -s "$fail" ]; then
    rm "$fail"
    exit 1
else
    rm "$fail"
    exit 0
fi
