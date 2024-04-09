#!/usr/bin/env bash

set -euo pipefail

PARGS="-p ."
CARGS="-c .stylish-haskell.yaml"

while getopts p:c flag
do
    case "${flag}" in
        p) PARGS="-p ${OPTARG}";;
        c) CARGS="-c ${OPTARG}";;
        *) exit 1;;
    esac
done

export LC_ALL=C.UTF-8

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

echo "Running stylish-haskell script with arguments: $PARGS $CARGS"

# shellcheck disable=SC2086
$fdcmd $PARGS -e hs -X stylish-haskell $CARGS -i
