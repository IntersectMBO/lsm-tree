#!/bin/sh

# shellcheck disable=SC2016
git ls-files --exclude-standard --no-deleted --deduplicate '*.cabal' | xargs -L 1 sh -c 'echo "$0" && cd "$(dirname "$0")" && cabal check'
