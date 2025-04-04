#!/bin/sh

export LC_ALL=C.UTF-8

# Check for cabal-docspec
cabal_docspec_required_version="0.0.0.20240414"
cabal_docspec="$(which cabal-docspec)"
if [ "${cabal_docspec}" = "" ]; then
    echo "Requires cabal-docspec version ${cabal_docspec_required_version}; no version found"
    exit 1
fi
cabal_docspec_installed_version="$($cabal_docspec --version)"

# Test Haskell files with cabal-docspec
echo "Testing Haskell files with cabal-docspec version ${cabal_docspec_installed_version}"
# shellcheck disable=SC2016
if [ "${SKIP_CABAL_BUILD}" = "" ]; then
    if ! cabal build all; then
        exit 1
    fi
fi
cabal-docspec \
    --extra-package directory \
    --extra-package lsm-tree:prototypes \
    -XOverloadedStrings \
    -XNumericUnderscores \
    -XTypeApplications \
    -XScopedTypeVariables
