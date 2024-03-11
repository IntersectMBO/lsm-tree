#ifndef HS_XXHASH
#define HS_XXHASH

#include <stdint.h>

#define XXH_INLINE_ALL
#include "xxhash.h"

static inline uint64_t hs_XXH3_64bits_withSeed_offset(const uint8_t *ptr, size_t off, size_t len, uint64_t seed) {
    return XXH3_64bits_withSeed(ptr + off, len, seed);
}

#endif /* HS_XXHASH */
