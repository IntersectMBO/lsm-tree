#!/usr/bin/env bash

set -euo pipefail

fdfind -p . -E bloomfilter -e cabal -x cabal-fmt -i
