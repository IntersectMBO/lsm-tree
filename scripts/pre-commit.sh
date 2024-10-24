#!/bin/sh

echo "check-cabal"
./check-cabal.sh
echo

echo "format-cabal"
./format-cabal.sh
echo

echo "format-stylish"
./format-stylish.sh
echo
