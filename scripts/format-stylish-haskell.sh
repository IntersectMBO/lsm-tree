#!/bin/sh

export LC_ALL=C.UTF-8

# POSIX compliant method for 'pipefail':
warn=$(mktemp)

# Check stylish-haskell version
stylish_haskell_expect_version="0.15.1.0"
if [ "${stylish_haskell}" = "" ]; then
    stylish_haskell=$(which "stylish-haskell")
    if [ "${stylish_haskell}" = "" ]; then
        echo "Requires stylish-haskell ${stylish_haskell_expect_version}; no version found"
        exit 1
    fi
fi
stylish_haskell_actual_version="$($stylish_haskell --version | head -n 1 | cut -d' ' -f2)"
if [ ! "${stylish_haskell_actual_version}" = "${stylish_haskell_expect_version}" ]; then
    echo "Expected stylish-haskell ${stylish_haskell_expect_version}; version ${stylish_haskell_actual_version} found"
    echo > "$warn"
fi

# Format Haskell files with stylish-haskell
echo "Formatting Haskell source files with stylish-haskell version ${stylish_haskell_actual_version}..."
# shellcheck disable=SC2016
if ! git ls-files --exclude-standard --no-deleted --deduplicate '*.hs' | xargs -L 50 stylish-haskell -i -c .stylish-haskell.yaml; then
    exit 1
fi

# Check whether any warning was issued; on CI, warnings are errors
if [ "${CI}" = "true" ] && [ -s "$warn" ]; then
    rm "$warn"
    exit 1
else
    rm "$warn"
    exit 0
fi
