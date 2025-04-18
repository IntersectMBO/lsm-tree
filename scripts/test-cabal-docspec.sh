#!/bin/sh

export LC_ALL=C.UTF-8

~/Downloads/cabal-docspec-0.0.0.20240703-x86_64-linux \
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
    --extra-package directory \
    --extra-package lsm-tree:prototypes \
    --extra-package blockio \
    --extra-package blockio:sim