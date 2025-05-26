#!/bin/sh

# Check for cabal
cabal="$(which cabal)"
if [ "${cabal}" = "" ]; then
    echo "Requires cabal; no version found"
    exit 1
fi
cabal_actual_version="$(${cabal} --numeric-version | head -n 1)"

# Lint Cabal files with cabal
echo "Linting Cabal source files with cabal version ${cabal_actual_version}..."
# shellcheck disable=SC2016
if ! git ls-files --exclude-standard --no-deleted --deduplicate '*.cabal' | xargs -L 1 sh -c 'echo "$0" && cd "$(dirname "$0")" && cabal check'; then
    exit 1
fi
