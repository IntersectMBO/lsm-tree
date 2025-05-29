#! /usr/bin/env sh

# Usage notes:
#
#   * The arguments to this utility specify the files to check. If no
#     arguments are given, standard input is checked. A typical usage of
#     this utility is with the `**` glob wildcard, supported in
#     particular by the Z Shell and by Bash with the `extglob` option
#     set. For example, the following command will check all Haskell
#     source files of the main library:
#
#         scripts/io-specialisations/find-absent.sh src/**/*.hs
#
#   * The results of this utility are not reliable, but should generally
#     be correct for “reasonably styled” code. One important restriction
#     is that, in order to be considered in need of having an `IO`
#     specialisation, an operation must have an application of a type
#     variable named `m` as its result type.
#
#   * This utility requires GNU sed. If there is a command `gsed`
#     available, then this utility will consider it to be GNU sed and
#     use it. If there is no command `gsed` available but the operating
#     system is Linux, then this utility will assume that `sed` is GNU
#     sed and use it. In all other cases, this utility will fail.
#
# Implementation notes:
#
#   * The `sed` script that essentially performs all the work uses the
#     hold space to hold the name of the current module and the name of
#     the operation to which the most recently found `IO` specialisation
#     or inlining directive refers. These two names are stored with a
#     space between them. The strings before and after the space can
#     also be empty:
#
#       - The string before the space is empty when the module name is
#         not given on the same line as the `module` keyword. This
#         causes the module name to not appear in the output but
#         otherwise does not have any drawback.
#
#       - The string after the space is empty when no `IO`
#         specialisation or inlining directive has been found yet in the
#         current module or the most recently found such directive is
#         considered to not be relevant for the remainder of the module.
#
#   * This utility requires GNU sed because it uses a backreference in
#     an extended regular expression, something that the POSIX standard
#     does not guarantee to work.

set -e

export LC_COLLATE=C LC_CTYPE=C

if command -v gsed >/dev/null
then
    gnu_sed=gsed
elif [ $(uname) = Linux ]
then
    gnu_sed=sed
else
    printf 'GNU sed not found\n' >&2
    exit 1
fi

specialise='SPECIALI[SZ]E'
pragma_types="($specialise|INLINE)"
hic='[[:alnum:]_#]' # Haskell identifier character

$gnu_sed -En -e '
    :start
    # Process the first line of a module header
    /^module / {
        s/module +([^ ]*).*/\1 /
        h
    }
    # Process a `SPECIALISE` or `INLINE` pragma
    /^\{-# *'"$pragma_types"'( |$)/ {
        # Remove any pragma operation name from the hold space
        x
        s/ .*//
        x
        # Add the pragma to the hold space
        :prag-add
        H
        /#-\}/ !{
            n
            b prag-add
        }
        # Get the contents of the hold space
        g
        # Skip a `SPECIALISE` pragma with a non-`IO` result type
        /\{-# *'"$specialise"'( |\n)/ {
            s/.*(::|=>|->)( |\n)*//
            /^IO / !{
                g
                s/\n.*/ /
                h
                d
            }
            g
        }
        # Store the operation name along with the module name
        s/\{-# *'"$pragma_types"'( |\n)+//
        s/\n('"$hic"'*).*/ \1/
        h
    }
    # Process a potential type signature
    /^[[:lower:]_]/ {
        # Add the potential type signature to the hold space
        :tsig-add
        s/ -- .*//
        H
        n
        /^ / b tsig-add
        # Get the persistent data and save the next line
        x
        # Process a type signature with a context
        /^[^ ]* '"$hic"'*\n'"$hic"'+( |\n)*::.+=>/ {
            # Place the result type next to the operation name
            s/([^ ]* '"$hic"'*\n'"$hic"'+).*(=>|->)( |\n)*/\1 /
            # Handle the case of a monadic result type
            /^[^ ]* '"$hic"'*\n[^ ]+ m / {
                # Handle the case of pragma absence
                /^[^ ]* ('"$hic"'*)\n\1 / !{
                    s/([^ ]*) '"$hic"'*\n([^ ]+).*/\1.\2/p
                    s/\.[^.]+$/ /
                    b tsig-fin
                }
            }
        }
        # Clean up and forget about the pragma operation name if any
        s/ .*/ /
        # Get the saved next line and store the persistent data
        :tsig-fin
        x
        # Continue
        b start
    }
' "$@"
