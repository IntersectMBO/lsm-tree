#!/bin/sh

export LC_ALL=C.UTF-8

# Check for cabal-fmt
cabal_fmt_required_version="0.1.12"
cabal_fmt="$(which cabal-fmt)"
if [ "${cabal_fmt}" = "" ]; then
    echo "Requires cabal-fmt version ${cabal_fmt_required_version}; no version found"
    exit 1
fi
cabal_fmt_installed_version="$($cabal_fmt --version | head -n 1 | cut -d' ' -f2)"
if [ ! "${cabal_fmt_installed_version}" = "${cabal_fmt_required_version}" ]; then
    echo "Requires cabal-fmt version ${cabal_fmt_required_version}; found version ${cabal_fmt_installed_version}"
    exit 1
fi

# Check Cabal files with cabal-fmt
echo "Formatting Cabal source files with cabal-fmt version ${cabal_fmt_required_version}"
# shellcheck disable=SC2016
if ! git ls-files --exclude-standard --no-deleted --deduplicate '*.cabal' | xargs -L 1 sh -c 'echo "$0" && cabal-fmt -i "$0"'; then
    exit 1
fi
