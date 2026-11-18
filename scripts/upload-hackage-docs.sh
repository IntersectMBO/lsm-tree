#!/bin/sh
set -e

# create a temporary directory for the output, and removes it when the script
# exits
dir=$(mktemp -d dist-docs.XXXXXX)
trap 'rm -r "$dir"' EXIT

# workaround for https://github.com/haskell/cabal/issues/10368: first build
# haddocks normally, before building again using --haddock-for-hackage.
cabal haddock --builddir="$dir"                       --enable-documentation $1

# build haddocks for hackage
cabal haddock --builddir="$dir" --haddock-for-hackage --enable-documentation $1

# upload the haddocks to hackage
cabal upload -d --publish $dir/*-docs.tar.gz
