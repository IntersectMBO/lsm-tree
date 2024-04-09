#!/usr/bin/env bash

set -euo pipefail

# In Ubuntu systems the fd command is called fdfind.
# First, try to find the 'fdfind' command
fdcmd="fdfind"
if ! command -v "$fdcmd" &> /dev/null; then
    # If 'fdfind' is not found, try 'fd'
    fdcmd="fd"
    if ! command -v "$fdcmd" &> /dev/null; then
        echo "Error: Neither 'fd' nor 'fdfind' command found." >&2
        exit 1
    fi
fi

$fdcmd -p . -e cabal -x cabal-fmt -i
