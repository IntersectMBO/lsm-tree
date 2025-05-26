#!/bin/sh

export LC_ALL=C.UTF-8

# POSIX compliant method for 'pipefail':
warn=$(mktemp)

# Check for shellcheck
shellcheck_expect_version="0.10.0"
if [ "${shellcheck}" = "" ]; then
    shellcheck=$(which "shellcheck")
    if [ "${shellcheck}" = "" ]; then
        echo "Requires shellcheck ${shellcheck_expect_version}; no version found"
        exit 1
    fi
fi
shellcheck_actual_version="$(${shellcheck} --version | head -n2 | tail -n1 | cut -d' ' -f2)"
if [ ! "${shellcheck_actual_version}" = "${shellcheck_expect_version}" ]; then
    echo "Expected shellcheck ${shellcheck_expect_version}; version ${shellcheck_actual_version} found"
    echo > "$warn"
fi

# Run shellcheck:
echo "Lint GitHub Actions workflows with shellcheck ${shellcheck_actual_version}..."
# shellcheck disable=SC2086
git ls-files --exclude-standard --no-deleted --deduplicate '*.sh' | xargs -L50 ${shellcheck}

# Check whether any warning was issued; on CI, warnings are errors
if [ "${CI}" = "true" ] && [ -s "$warn" ]; then
    rm "$warn"
    exit 1
else
    rm "$warn"
    exit 0
fi
