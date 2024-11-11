#!/bin/sh

# To install as a Git pre-commit hook, run:
#
# > ln scripts/pre-commit.sh .git/hooks/pre-commit.sh
#

# Check for unstaged changes in Haskell files
unstaged_haskell_files="$(git ls-files --exclude-standard --no-deleted --deduplicate --modified '*.hs')"
if [ ! "${unstaged_haskell_files}" = "" ]; then
    echo "Found unstaged Haskell files"
    echo "${unstaged_haskell_files}"
    exit 1
fi

# Check for unstaged changes in Cabal files
unstaged_cabal_files="$(git ls-files --exclude-standard --no-deleted --deduplicate --modified '*.cabal')"
if [ ! "${unstaged_cabal_files}" = "" ]; then
    echo "Found unstaged Cabal files"
    echo "${unstaged_cabal_files}"
    exit 1
fi

# Run various checks and formatters
./scripts/check-cabal.sh || exit 1
./scripts/format-cabal.sh || exit 1
./scripts/format-stylish.sh || exit 1
