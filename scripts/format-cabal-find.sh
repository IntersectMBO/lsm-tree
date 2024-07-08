#!/usr/bin/env bash

set -euo pipefail

find . -path "./dist-newstyle" -prune -o \
  -name '*.cabal' -exec cabal-fmt -i {} +
