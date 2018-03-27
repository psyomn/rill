#include "rill.h"

#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <unistd.h>

static inline void make_filename(char path[static PATH_MAX], size_t num) {
    path[0] = 0;
    sprintf(path, "%03ld.rill.shard", num);
}

int main(int argc, char* argv[]) {
    (void) argc, (void) argv;
    FILE* sin = freopen(NULL, "rb", stdin);
    size_t pairs_left = 0, pairs_count = 0;

    (void) fread(&pairs_count, sizeof(pairs_count), 1, sin);

    enum { max_shards = 10 };
    const size_t chunk = pairs_count / max_shards;
    char buffer[PATH_MAX];

    pairs_left = pairs_count;

    for (size_t j = 0; j < max_shards; ++j) {
        make_filename(buffer, j);
        struct rill_pairs* pairs = rill_pairs_new(chunk);
        size_t read_amount = chunk >= pairs_left ? pairs_left : chunk;

        /* handle odd number of pairs that go above chunks */
        read_amount += pairs_left - chunk < chunk ? pairs_left - chunk : 0;

        for (size_t i = 0; i < read_amount; ++i) {
            struct rill_kv kvs = {0};
            (void) fread(&kvs, 1, sizeof(kvs), sin);
            pairs = rill_pairs_push(pairs, kvs.key, kvs.val);
        }

        pairs_left -= read_amount;

        if(!rill_store_write(buffer, 0, 0, pairs)) {
            fprintf(stderr, "problem writing shard\n");
            exit(1);
        }

        rill_pairs_free(pairs);
    }
    fclose(sin);

    /* merge shards */
    const char* final = "final.rill";

    struct rill_store *stores[max_shards];

    for (size_t i = 0; i < max_shards; ++i) {
        make_filename(buffer, i);
        stores[i] = rill_store_open(buffer);
    }

    if (!rill_store_merge(final, 0, 0, stores, max_shards)) {
        fprintf(stderr, "problem merging\n");
        exit(1);
    }

    for (size_t i = 0; i < max_shards; ++i) {
        rill_store_close(stores[i]);
        make_filename(buffer, i);
        unlink(buffer);
    }

    return 0;
}
