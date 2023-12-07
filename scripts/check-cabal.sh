#!/usr/bin/env bash

set -euo pipefail

for x in $(find . -name '*.cabal' | grep -v dist-newstyle | cut -c 3-); do
  (
    d=$(dirname $x)
    if [ $(basename $d) != "bloomfilter" ]; then
      echo "== $d =="
      cd $d
      cabal check
    fi
  )
done
