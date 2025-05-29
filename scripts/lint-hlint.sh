#!/bin/sh

export LC_ALL=C.UTF-8

# POSIX compliant method for 'pipefail':
warn=$(mktemp)

# Check for hlint
hlint_expect_version="3.10"
if [ "${hlint}" = "" ]; then
    hlint=$(which "hlint")
    if [ "${hlint}" = "" ]; then
        echo "Requires hlint ${hlint_expect_version}; no version found"
        exit 1
    fi
fi
hlint_actual_version="$(${hlint} --version | head -n 1 | cut -d' ' -f 2 | sed -E 's/v(.*),/\1/')"
if [ ! "${hlint_actual_version}" = "${hlint_expect_version}" ]; then
    echo "Expected hlint ${hlint_expect_version}; version ${hlint_actual_version} found"
    echo > "$warn"
fi

# Lint Haskell files with HLint
echo "Linting Haskell files with HLint version ${hlint_actual_version}..."
# shellcheck disable=SC2086
git ls-files --exclude-standard --no-deleted --deduplicate '*.hs' | xargs -L50 ${hlint}

# Check whether any warning was issued; on CI, warnings are errors
if [ "${CI}" = "true" ] && [ -s "$warn" ]; then
    rm "$warn"
    exit 1
else
    rm "$warn"
    exit 0
fi
