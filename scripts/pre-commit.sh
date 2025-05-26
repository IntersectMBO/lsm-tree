#!/bin/sh

# To install as a Git pre-commit hook, run:
#
# > ln scripts/pre-commit.sh .git/hooks/pre-commit.sh
#

# POSIX compliant method for 'pipefail':
warn=$(mktemp)

# Check for unstaged changes in Haskell files
unstaged_haskell_files="$(git ls-files --exclude-standard --no-deleted --deduplicate --modified '*.hs' || echo > "$warn")"
if [ ! "${unstaged_haskell_files}" = "" ]; then
    echo "Found unstaged Haskell files"
    echo "${unstaged_haskell_files}"
fi

# Check for unstaged changes in Cabal files
unstaged_cabal_files="$(git ls-files --exclude-standard --no-deleted --deduplicate --modified '*.cabal' || echo > "$warn")"
if [ ! "${unstaged_cabal_files}" = "" ]; then
    echo "Found unstaged Cabal files"
    echo "${unstaged_cabal_files}"
fi

# Lint GitHub Actions workflows with actionlint
./scripts/lint-actionlint.sh || echo > "$warn"
echo

# Format Cabal files with cabal-fmt
./scripts/format-cabal-fmt.sh || echo > "$warn"
echo

# Format Haskell files with stylish-haskell
./scripts/format-stylish-haskell.sh || echo > "$warn"
echo

# Lint GitHub Actions workflows with actionlint
./scripts/lint-actionlint.sh || echo > "$warn"
echo

# Lint Cabal files with cabal
./scripts/lint-cabal.sh || echo > "$warn"
echo

# Lint Haskell files files with HLint
./scripts/lint-hlint.sh || echo > "$warn"
echo

# Lint shell scripts files with ShellCheck
./scripts/lint-shellcheck.sh || echo > "$warn"
echo

# Generate README.md from package description
./scripts/generate-readme.hs || echo > "$warn"
echo

# Check whether any warning was issued; on CI, warnings are errors
if [ "${CI}" = "true" ] && [ -s "$warn" ]; then
    rm "$warn"
    exit 1
else
    rm "$warn"
    exit 0
fi
