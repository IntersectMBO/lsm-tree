#! /usr/bin/env sh

# Usage notes:
#
#   * This utility requires that any Haddock annotation that contains
#     information about time complexities appears in the form of a
#     nested comment (a comment delimited by `{-` and `-}`). This should
#     not be an unreasonable demand, since any such Haddock annotation
#     should span multiple lines of source code and nested comments are
#     the preferred structure for multi-line comments.
#
# Implementation notes:
#
#   * The `sed` script that essentially performs all the work uses the
#     hold space to hold time complexity information that has not yet
#     been output. Every time a global type signature is encountered,
#     the identifier from this type signature is attached to the stored
#     time complexities, and the resulting data is flushed to the
#     standard output. The hold space additionally contains a trailing
#     line consisting of only `=` if any time complexity encountered in
#     the currently processed line refers to the situation where all
#     tables in a session have the same merge policy.

set -e

export LC_COLLATE=C LC_CTYPE=C

o_expr_re='\\\((O\(.*\))\\\)' # includes an anchor for back-references

unconditional_prefix_re='The worst-case disk I\\\/O complexity of this operation is '
unconditional_suffix_re='[.,]'
unconditional_re="^$unconditional_prefix_re$o_expr_re$unconditional_suffix_re\$"

nothing_left_open_prefix_re='If there are no open tables or cursors .*, then the disk I\\\/O complexity of this operation is '
nothing_left_open_suffix_re='\.'
nothing_left_open_re="^$nothing_left_open_prefix_re$o_expr_re$nothing_left_open_suffix_re\$"

same_merge_policy_re='^The following assumes all tables in the session have the same merge policy:$'

newline_rs='\
' # for use within replacement strings of `s`-commands

sed -En -e '
    # Collect all time complexity information from a Haddock annotation
    /^\{- *\|/,/-}/ {
        # Store an unconditional time complexity
        s/'"$unconditional_re"'/,,\1,/
        t store
        # Store a time complexity for the “nothing left open” case
        s/'"$nothing_left_open_re"'/,,\1,Nothing left open/
        t store
        # Store a time complexity with unknown condition
        s/^.*'"$o_expr_re"'.*$/,,\1,Unknown condition/
        t store
        # Note down the occurrence of a “same merge policy” restriction
        /'"$same_merge_policy_re"'/ {
            s/.*/=/
            H
            d
        }
        # Possibly fetch parameter-specific time complexity information
        /^\['"'"'[^'"'"']+'"'"'(\\\/'"'"'[^'"'"']+'"'"')?]/ {
            # Construct the parameter fields
            s/\['"'"'([^'"'"']+)'"'"'/\1,/
            s/\\\/|'"'"'//g
            s/]:$//
            # Get the next line
            N
            # Store the time complexity from a time complexity item
            s/\n *'"$o_expr_re"'\.$/,\1,/
            t store
            # Ignore an item that is not a time complexity item
            d
        }
        # Skip the current line
        d
        # Store a found time complexity
        :store
        H
        x
        s/\n=\n(.*)/'"$newline_rs"'\1Same merge policy'"$newline_rs"'=/
        x
        # Continue with the next line
        d
    }
    # Output the stored time complexities with their global identifier
    /^[^ ]+ *::/ {
        # Remove everything except the identifier
        s/ *::.*//
        # Swap the identifier and the time complexity information
        x
        # Remove any “same merge policy” note
        s/\n=$//
        # Mark the starts of the time complexity entries
        s/\n/'"$newline_rs$newline_rs"'/g
        # Mark the end of the time complexity information
        s/$/'"$newline_rs"'!/
        # Append the identifier
        G
        # Turn the entry start markers into identifier fields
        :add-id
        s/\n\n(.*\n!\n(.*))/'"$newline_rs"'\2,\1/
        t add-id
        # Remove everything after the time complexity information
        s/\n!\n.*//
        # Output the time complexity information
        s/\n//p
        # Clear the store
        b reset
    }
    # Skip the current line if it doesn’t start a declaration or binding
    /^([-{ ]|$)/ d
    # Clear the store
    :reset
    s/.*//
    h
' "$@"
