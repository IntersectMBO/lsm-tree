#!/bin/bash
#
# This script is based on https://github.com/IntersectMBO/ouroboros-consensus/blob/4ff9aa5596598bb3681864b830c8a33e294d9bfe/scripts/docs/haddocks.sh
#
# Build haddock documentation and an index for all projects in the `lsm-tree`
# repository.
#
# usage:
# ./scripts/haddocks.sh directory [true|false]
#
# $1 - where to put the generated pages, this directory contents will be wiped
#      out (so don't pass `/` or `./` - the latter will delete your 'dist-newstyle')
#      (the default is './haddocks')
# $2 - whether to re-build haddocks with `cabal haddock` command or a component name
#      (the default is true)
# $3 - cabal build directory
#      (the default is "dist-newstyle")
# $4 - include documentation for sub-libraries
#      (the default is true)
# $5 - include documentation for test suites
#      (the default is false)
# $6 - include documentation for benchmark suites
#      (the default is false)

set -euo pipefail

OUTPUT_DIR=${1:-"./docs/haddocks"}
REGENERATE=${2:-"true"}
BUILD_DIR=${3:-"dist-newstyle"}
INCLUDE_SUBLIBS=${4:-"true"}
INCLUDE_TESTS=${5:-"false"}
INCLUDE_BENCHMARKS=${6:-"false"}

# the directory containing this script
SCRIPTS_DIR=$(realpath $(dirname $(realpath $0)))

GHC_VERSION=$(ghc --numeric-version)
OS_ARCH="$(cat dist-newstyle/cache/plan.json | jq -r '.arch + "-" + .os' | head -n 1 | xargs)"

# we don't include `--use-index` option, because then quickjump data is not
# generated.  This is not ideal, but there is no way to generate only top level
# `doc-index.html` file.  With this approach we get:
# * `doc-index.json` and `doc-index.html` per package
# * we can generate top level `doc-index.json` (which will only work at the top
#   level).
# * we could ammend package level `doc-index.json` files, but it's enough ...
#   this should be fixed upstream.
HADDOCK_OPTS=(
    --builddir "${BUILD_DIR}"
    --disable-optimization
    --haddock-all
    --haddock-internal
    --haddock-html
    --haddock-quickjump
    --haddock-hyperlink-source
    --haddock-option "--show-all"
    --haddock-option "--use-unicode"
  )

# build documentation of all modules
if [ ${REGENERATE} == "true" ]; then
  cabal haddock "${HADDOCK_OPTS[@]}" all
elif [ ${REGENERATE} != "false" ]; then
  cabal haddock "${HADDOCK_OPTS[@]}" ${REGENERATE}
fi

if [[ !( -d ${OUTPUT_DIR} ) ]]; then
  mkdir -p ${OUTPUT_DIR}
fi

# make all files user writable
chmod -R u+w "${OUTPUT_DIR}"

# copy the new docs
for dir in $(ls "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}"); do
  package=$(echo "${dir}" | sed 's/-[0-9]\+\(\.[0-9]\+\)*//')
  cp -r "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/noopt/doc/html/${package}" ${OUTPUT_DIR}
  # copy test packages documentation when it exists
  if [ ${INCLUDE_TESTS} == "true" ] && [ -d "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/t" ]; then
      for test_package in $(ls "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/t"); do
          if [ -d "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/t/${test_package}/noopt/doc/html/${package}/${test_package}" ]; then
              cp -r "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/t/${test_package}/noopt/doc/html/${package}/${test_package}" "${OUTPUT_DIR}/${package}-${test_package}"
          fi
      done
  fi
  # copy benchmark packages documentation when it exists
  if [ ${INCLUDE_BENCHMARKS} == "true" ] && [ -d "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/b" ]; then
      for bench_package in $(ls "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/b"); do
          if [ -d "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/b/${bench_package}/noopt/doc/html/${package}/${bench_package}" ]; then
              cp -r "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/b/${bench_package}/noopt/doc/html/${package}/${bench_package}" "${OUTPUT_DIR}/${package}-${bench_package}"
          fi
      done
  fi
  # copy sublib documentation when it exists
  if [ ${INCLUDE_SUBLIBS} == "true" ] && [ -d "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/l" ]; then
      for sublib in $(ls "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/l"); do
          if [ -d "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/l/${sublib}/noopt/doc/html/${package}" ]; then
              cp -r "${BUILD_DIR}/build/${OS_ARCH}/ghc-${GHC_VERSION}/${dir}/l/${sublib}/noopt/doc/html/${package}" "${OUTPUT_DIR}/${package}-${sublib}"
          fi
      done
  fi
done


# --read-interface options
interface_options () {
    for package in $(ls "${OUTPUT_DIR}"); do
        if [[ -d "${OUTPUT_DIR}/${package}" ]]; then
            # take the first haddock file found.
            # there should be only one but the filename is the name of the main pacakage
            # and can differ from the name of the enclosing directory
            haddock_file=$(ls -1 ${OUTPUT_DIR}/${package}/*.haddock | head -1)
            echo "--read-interface=${package},${haddock_file}"
        fi
  done
}

# Generate top level index using interface files
#
haddock \
  -o ${OUTPUT_DIR} \
  --title "lsm-tree" \
  --package-name "lsm-tree" \
  --gen-index \
  --gen-contents \
  --quickjump \
  $(interface_options)

# Assemble a toplevel `doc-index.json` from package level ones.
#
echo "[]" > "${OUTPUT_DIR}/doc-index.json"
for file in $(ls $OUTPUT_DIR/*/doc-index.json); do
  project=$(basename $(dirname $file));
  jq -s \
    ".[0] + [.[1][] | (. + {link: (\"${project}/\" + .link)}) ]" \
    "${OUTPUT_DIR}/doc-index.json" \
    ${file} \
    > /tmp/doc-index.json
  mv /tmp/doc-index.json "${OUTPUT_DIR}/doc-index.json"
done

echo "Generated documentation in ${OUTPUT_DIR}"