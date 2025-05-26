#!/bin/sh

export LC_ALL=C.UTF-8

# POSIX compliant method for 'pipefail':
warn=$(mktemp)

# Check for actionlint
actionlint_expect_version="1.7.7"
if [ "${actionlint}" = "" ]; then
    actionlint=$(which "actionlint")
    if [ "${actionlint}" = "" ]; then
        echo "Requires actionlint ${actionlint_expect_version}; no version found"
        exit 1
    fi
fi
actionlint_actual_version="$(${actionlint} --version | head -n 1)"
if [ ! "${actionlint_actual_version}" = "${actionlint_expect_version}" ]; then
    echo "Expected actionlint ${actionlint_expect_version}; version ${actionlint_actual_version} found"
    echo > "$warn"
fi

# Run actionlint:
echo "Lint GitHub Actions workflows with actionlint ${actionlint_actual_version}..."
# shellcheck disable=SC2086
git ls-files --exclude-standard --no-deleted --deduplicate '.github/workflows/*.yml' | xargs -L50 ${actionlint}

# Check whether any warning was issued; on CI, warnings are errors
if [ "${CI}" = "true" ] && [ -s "$warn" ]; then
    rm "$warn"
    exit 1
else
    rm "$warn"
    exit 0
fi
