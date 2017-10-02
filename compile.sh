#! /usr/bin/env bash

set -o errexit -o nounset -o pipefail -o xtrace

: ${PREFIX:="."}

declare -a SRC
SRC=(htable rng utils pairs store acc rotate query)

CFLAGS="-g -O3 -march=native -pipe -std=gnu11 -D_GNU_SOURCE"
CFLAGS="$CFLAGS -I${PREFIX}/src"

CFLAGS="$CFLAGS -Werror -Wall -Wextra"
CFLAGS="$CFLAGS -Wundef"
CFLAGS="$CFLAGS -Wcast-align"
CFLAGS="$CFLAGS -Wwrite-strings"
CFLAGS="$CFLAGS -Wunreachable-code"
CFLAGS="$CFLAGS -Wformat=2"
CFLAGS="$CFLAGS -Wswitch-enum"
CFLAGS="$CFLAGS -Wswitch-default"
CFLAGS="$CFLAGS -Winit-self"
CFLAGS="$CFLAGS -Wno-strict-aliasing"
CFLAGS="$CFLAGS -fno-strict-aliasing"

OBJ=""
for src in "${SRC[@]}"; do
    gcc -c -o "$src.o" "${PREFIX}/src/$src.c" $CFLAGS
    OBJ="$OBJ $src.o"
done
ar rcs librill.a $OBJ

gcc -o rill_load "${PREFIX}/src/rill_load.c" librill.a $CFLAGS
gcc -o rill_dump "${PREFIX}/src/rill_dump.c" librill.a $CFLAGS
gcc -o rill_query "${PREFIX}/src/rill_query.c" librill.a $CFLAGS
gcc -o rill_historical_dump "${PREFIX}/src/rill_historical_dump.c" librill.a $CFLAGS
gcc -o rill_rotate "${PREFIX}/src/rill_rotate.c" librill.a $CFLAGS

gcc -o test_coder "${PREFIX}/test/coder_test.c" librill.a $CFLAGS && ./test_coder
gcc -o test_rotate "${PREFIX}/test/rotate_test.c" librill.a $CFLAGS
