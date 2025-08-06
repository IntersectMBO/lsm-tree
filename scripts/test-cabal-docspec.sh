#!/bin/sh

export LC_ALL=C.UTF-8

# POSIX compliant method for 'pipefail':
warn=$(mktemp)

# Check cabal-docspec version
cabal_docspec_expect_version="0.0.0.20240703"
if [ "${cabal_docspec}" = "" ]; then
    cabal_docspec=$(which "cabal-docspec")
    if [ "${cabal_docspec}" = "" ]; then
        echo "Requires cabal-docspec ${cabal_docspec_expect_version}; no version found"
        exit 1
    fi
fi
cabal_docspec_actual_version="$(${cabal_docspec} --version | head -n 1)"
if [ ! "${cabal_docspec_actual_version}" = "${cabal_docspec_expect_version}" ]; then
    echo "Expected cabal-docspec ${cabal_docspec_expect_version}; version ${cabal_docspec_actual_version} found"
    echo > "$warn"
fi

# Test Haskell files with cabal-docspec
echo "Testing Haskell files with cabal-docspec version ${cabal_docspec_actual_version}..."
# shellcheck disable=SC2016
if [ "${SKIP_CABAL_BUILD}" = "" ]; then
    if ! cabal build all; then
        exit 1
    fi
fi
${cabal_docspec} \
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
    --extra-package bytestring \
    --extra-package directory \
    --extra-package filepath \
    --extra-package lsm-tree:prototypes \
    --extra-package process \
    || exit 1

# Check whether any warning was issued; on CI, warnings are errors
if [ "${CI}" = "true" ] && [ -s "$warn" ]; then
    rm "$warn"
    exit 1
else
    rm "$warn"
    exit 0
fi
