#!/usr/bin/env bash

set -euo pipefail

find . -iname '*.cabal' \
  -not -path "./dist-newstyle" \
  -exec cabal-fmt -i {} +
