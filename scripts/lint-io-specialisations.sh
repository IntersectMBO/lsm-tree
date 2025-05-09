#! /usr/bin/env sh

absence_allowed_file=scripts/io-specialisations/absence-allowed
absence_finder=scripts/io-specialisations/find-absent.sh

set -e

IFS='
'

export LC_COLLATE=C LC_TYPE=C

printf 'Linting the main library regarding `IO` specialisations\n'

if ! [ -f "$absence_allowed_file" ]
then
    printf 'There is no regular file `%s`.\n' "$absence_allowed_file"
    exit 2
fi >&2
if ! sort -C "$absence_allowed_file"
then
    printf 'The entries in `%s` are not sorted.\n' "$absence_allowed_file"
    exit 2
fi >&2

hs_files=$(
    git ls-files \
        --exclude-standard --no-deleted --deduplicate \
        'src/*.hs' 'src/**/*.hs'
)
absent=$(
    "$absence_finder" $hs_files
)
missing=$(
    printf '%s\n' "$absent" | sort | comm -23 - "$absence_allowed_file"
)
if [ -n "$missing" ]
then
    printf '`IO` specialisations for the following operations are '
    printf 'missing:\n'
    printf '%s\n' "$missing" | sed -e 's/.*/  * `&`/'
    exit 1
fi
printf 'All required `IO` specialisations are present.\n'
