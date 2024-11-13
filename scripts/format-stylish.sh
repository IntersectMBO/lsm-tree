#!/bin/sh

export LC_ALL=C.UTF-8

# Check for stylish-haskell
stylish_haskell_required_version="0.14.6.0"
stylish_haskell="$(which stylish-haskell)"
if [ "${stylish_haskell}" = "" ]; then
    echo "Requires stylish-haskell version ${stylish_haskell_required_version}; no version found"
    exit 1
fi
stylish_haskell_installed_version="$($stylish_haskell --version | head -n 1 | cut -d' ' -f2)"
if [ ! "${stylish_haskell_installed_version}" = "${stylish_haskell_required_version}" ]; then
    echo "Requires stylish-haskell version ${stylish_haskell_required_version}; found version ${stylish_haskell_installed_version}"
    exit 1
fi

# Check Haskell files with stylish-haskell
echo "Formatting Haskell source files with stylish-haskell version ${stylish_haskell_required_version}"
# shellcheck disable=SC2016
if ! git ls-files --exclude-standard --no-deleted --deduplicate '*.hs' | xargs -L 50 stylish-haskell -i -c .stylish-haskell.yaml; then
    exit 1
fi
