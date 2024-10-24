#!/bin/sh

export LC_ALL=C.UTF-8

# shellcheck disable=SC2016
git ls-files --exclude-standard --no-deleted --deduplicate '*.cabal' | xargs -L 1 sh -c 'echo "$0" && cabal-fmt -i "$0"'
