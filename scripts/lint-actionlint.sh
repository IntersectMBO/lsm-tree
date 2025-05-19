#!/bin/sh

# Check for actionlint
actionlint_expect_version="1.7.7"
if [ "${actionlint}" = "" ]; then
    actionlint=$(which "actionlint")
    if [ "${actionlint}" = "" ]; then
        echo "Requires actionlint ${actionlint_expect_version}; no version found"
        exit 1
    fi
fi
actionlint_actual_version=$(${actionlint} --version | head -n 1)
if [ ! "${actionlint_actual_version}" = "${actionlint_expect_version}" ]; then
    echo "Requires actionlint ${actionlint_expect_version}; version ${actionlint_actual_version} found"
    exit 1
fi

# Run actionlint:
echo "Lint GitHub Actions workflows..."
# shellcheck disable=SC2086
git ls-files --exclude-standard --no-deleted --deduplicate '.github/workflows/*.yml' | xargs -L50 ${actionlint}
