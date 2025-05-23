#!/bin/sh

export LC_ALL=C.UTF-8

# Check for cabal-docspec
cabal_docspec_required_version="0.0.0.20240703"
cabal_docspec="$(which cabal-docspec)"
if [ "${cabal_docspec}" = "" ]; then
    echo "Requires cabal-fmt version ${cabal_docspec_required_version}; no version found"
    exit 1
fi
cabal_docspec_installed_version="$($cabal_docspec --version)"
if [ ! "${cabal_docspec_installed_version}" = "${cabal_docspec_required_version}" ]; then
     >&2 printf "\033[1;31mWARNING: expecting cabal-docspec %s; version %s found. Running anyway...\033[00m\n" "${cabal_docspec_required_version}" "${cabal_docspec_installed_version}"
fi

# Test Haskell files with cabal-docspec
echo "Testing Haskell files with cabal-docspec version ${cabal_docspec_installed_version}"
# shellcheck disable=SC2016
if [ "${SKIP_CABAL_BUILD}" = "" ]; then
    if ! cabal build all; then
        exit 1
    fi
fi
cabal-docspec \
    -Wno-cpphs \
    -Wno-missing-module-file \
    -Wno-skipped-property \
    -XDerivingStrategies \
    -XDerivingVia \
    -XGeneralisedNewtypeDeriving \
    -XOverloadedStrings \
    -XRankNTypes \
    -XTypeApplications \
    -XTypeFamilies \
    -XNumericUnderscores \
    -XInstanceSigs \
    --extra-package blockio \
    --extra-package blockio:sim \
    --extra-package directory \
    --extra-package lsm-tree:prototypes \
    --extra-package process
