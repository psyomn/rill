/* test.h
   Rémi Attab (remi.attab@gmail.com), 11 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#pragma once

#include "rill.h"
#include "utils.h"
#include "htable.h"
#include "rng.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

#include <sys/types.h>
#include <dirent.h>
#include <unistd.h>


// -----------------------------------------------------------------------------
// pairs
// -----------------------------------------------------------------------------

struct rill_kv kv(rill_key_t key, rill_val_t val)
{
    return (struct rill_kv) { .key = key, .val = val };
}

#define make_pair(...)                                          \
    ({                                                          \
        struct rill_kv kvs[] = { __VA_ARGS__ };                 \
        make_pair_impl(kvs, sizeof(kvs) / sizeof(kvs[0]));      \
    })

struct rill_pairs *make_pair_impl(const struct rill_kv *kv, size_t len)
{
    struct rill_pairs *pairs = rill_pairs_new(len);
    for (size_t i = 0; i < len; ++i)
        pairs = rill_pairs_push(pairs, kv[i].key, kv[i].val);
    return pairs;
}

enum { rng_range_key = 500, rng_range_val = 100 };

struct rill_pairs *make_rng_pairs(struct rng *rng)
{
    enum { len = 1000 };
    struct rill_pairs *pairs = rill_pairs_new(len);

    for (size_t i = 0; i < len; ++i) {
        uint64_t key = rng_gen_range(rng, 1, rng_range_key);
        uint64_t val = rng_gen_range(rng, 1, rng_range_val);
        pairs = rill_pairs_push(pairs, key, val);
        assert(pairs);
    }

    rill_pairs_compact(pairs);

    return pairs;
}


// -----------------------------------------------------------------------------
// rm
// -----------------------------------------------------------------------------

void rm(const char *path)
{
    DIR *dir = opendir(path);
    if (!dir) return;

    struct dirent *entry;
    while (true) {
        if (!(entry = readdir(dir))) break;
        else if (entry->d_type != DT_REG) continue;

        char file[PATH_MAX];
        snprintf(file, sizeof(file), "%s/%s", path, entry->d_name);
        unlink(file);
    }

    closedir(dir);
    rmdir(path);
}
