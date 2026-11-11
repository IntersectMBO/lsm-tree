#! /usr/bin/env sh

set -e

export LC_COLLATE=C LC_CTYPE=C

header_fields_re="^ +\\| *$(printf '%s *\\| *' \
    'Resource' \
    'Operation' \
    'Merge policy' \
    'Merge schedule' \
    'Worst-case disk I\\\/O complexity'
)\$"

printf '%s\n' 'Resource,Operation,Merge policy,Merge schedule,Worst-case disk I\\\/O complexity'

sed -E -e '
    # Drop everything from the beginning to the header fields
    1,/'"$header_fields_re"'/ d
    # Drop everything after the table
    /^ *$/,$ d
    # Drop separating lines
    /^ +\+([=-]+\+)* *$/ d
    # Switch to delimiting by commas
    s/ *\| */,/g
    # Remove initial comma
    s/^,//
    # Remove final comma
    s/,$//
    # Remove escaping of slashes
    s/\\\//\//g
    # Remove (leading) occurrences of “N/A”
    s/(^|,)N\/A/\1/g
    # Remove monospace and inline math markup
    s/@|\\\(|\\\)//g
    # Fill empty “Resource” field with previous content
    /^,/ {
        G
        s/(.*)\n([^,]*).*/\2\1/
    }
    # Fill empty “Operation” field with previous content
    /^[^,]*,,/ {
        G
        s/([^,]*,)(.*)\n[^,]*,([^,]*).*/\1\3\2/
    }
    # Store the current output line
    h
' "$@"
