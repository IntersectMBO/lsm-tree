#!/usr/bin/env bash

set -euo pipefail

PARGS="."
CARGS="-c .stylish-haskell.yaml"

while getopts p:c flag
do
    case "${flag}" in
        p) PARGS="${OPTARG}";;
        c) CARGS="-c ${OPTARG}";;
        *) exit 1;;
    esac
done

echo "Running stylish-haskell script with arguments: $PARGS $CARGS"

export LC_ALL=C.UTF-8

# shellcheck disable=SC2086
find $PARGS \
  \( -path './dist-newstyle' -o -path './.git*' -o -path './_bench_*' \) -prune \
  -o \( -iname '*.hs' -exec stylish-haskell -i -c .stylish-haskell.yaml {} + \)
