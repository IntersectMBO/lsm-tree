#!/bin/sh

export LC_ALL=C.UTF-8

# Check for hlint
hlint_expect_version="3.8"
if [ "${hlint}" = "" ]; then
    hlint=$(which "hlint")
    if [ "${hlint}" = "" ]; then
        echo "Requires hlint ${hlint_expect_version}; no version found"
        exit 1
    fi
fi
hlint_actual_version=$(${hlint} --version | head -n 1 | cut -d' ' -f 2 | sed -E 's/v(.*),/\1/')
if [ ! "${hlint_actual_version}" = "${hlint_expect_version}" ]; then
    >&2 printf "\033[1;31mWARNING: expecting hlint %s; version %s found. Running anyway...\033[00m\n" "${hlint_expect_version}" "${hlint_actual_version}"
fi

# Lint Haskell files with HLint
echo "Linting Haskell files with HLint version ${hlint_actual_version}"
# shellcheck disable=SC2086
git ls-files --exclude-standard --no-deleted --deduplicate '*.hs' | xargs -L50 ${hlint}
