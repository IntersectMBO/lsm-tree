#!/usr/bin/env bash

set -euo pipefail

PARGS="-p ."
CARGS=""

while getopts p:cd flag
do
    case "${flag}" in
        p) PARGS="-p ${OPTARG}";;
        c) CARGS="-c ${OPTARG}";;
        d) CARGS="-c .stylish-haskell.yaml";;
    esac
done

echo "Running stylish-haskell script with arguments: $PARGS $CARGS"

export LC_ALL=C.UTF-8

fdfind $PARGS -e hs -X stylish-haskell $CARGS -i
