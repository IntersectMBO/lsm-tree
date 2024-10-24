#!/bin/sh

# shellcheck disable=SC1007
SCRIPTS_DIR=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)

echo "check-cabal"
./check-cabal.sh
echo

echo "format-cabal"
./format-cabal.sh
echo

echo "format-stylish"
./format-stylish.sh
echo
