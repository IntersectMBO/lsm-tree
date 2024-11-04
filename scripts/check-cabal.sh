#!/bin/sh

# Check for cabal
cabal="$(which cabal)"
if [ "${cabal}" = "" ]; then
    echo "Requires cabal; no version found"
    exit 1
fi

# Check Cabal files with cabal
echo "Checking Cabal source files with cabal"
# shellcheck disable=SC2016
if ! git ls-files --exclude-standard --no-deleted --deduplicate '*.cabal' | xargs -L 1 sh -c 'echo "$0" && cd "$(dirname "$0")" && cabal check'; then
    exit 1
fi
