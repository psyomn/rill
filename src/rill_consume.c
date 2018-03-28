#include "rill.h"

#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>

static inline void make_filename(char path[static PATH_MAX], size_t num) {
    path[0] = 0;
    snprintf(path, PATH_MAX, "%03ld.rill.shard", num);
}

int main(int argc, char* argv[]) {
    (void) argc, (void) argv;

    enum { max_shards = 10 };
    FILE* sin = freopen(NULL, "rb", stdin);

    size_t pairs_count = 0;
    rill_ts_t ts = 0;
    rill_ts_t quant = 0;
    size_t filename_size = 0;
    char* filename = NULL;

    if (!fread(&filename_size, sizeof(filename_size), 1, sin)) abort();
    filename_size += 20; // used for strncat later
    filename = calloc(filename_size, sizeof(*filename));

    if (!fread(filename, sizeof(*filename), filename_size, sin)) abort();

    if (!fread(&ts, sizeof(ts), 1, sin)) abort();
    if (!fread(&quant, sizeof(quant), 1, sin)) abort();
    if (!fread(&pairs_count, sizeof(pairs_count), 1, sin)) abort();

    const size_t chunk = pairs_count / max_shards;
    char buffer[PATH_MAX];

    size_t pairs_left = pairs_count;

    for (size_t j = 0; j < max_shards; ++j) {
        make_filename(buffer, j);

        struct rill_pairs* pairs = rill_pairs_new(chunk);
        size_t read_amount =
            chunk >= pairs_left ? pairs_left : chunk;

        /* handle odd number of pairs that go above chunks */
        read_amount += pairs_left - chunk < chunk ? pairs_left - chunk : 0;

        for (size_t i = 0; i < read_amount; ++i) {
            struct rill_kv kvs = {0};
            if(!fread(&kvs, 1, sizeof(kvs), sin)) abort();
            pairs = rill_pairs_push(pairs, kvs.key, kvs.val);
        }

        pairs_left -= read_amount;

        /* fork child here? */
        if(!rill_store_write(buffer, 0, 0, pairs)) {
            fprintf(stderr, "problem writing shard\n");
            exit(1);
        }
        rill_pairs_free(pairs);
        /* end fork child here? */
    }
    fclose(sin);

    /* 00012.rill -> 00012.rill.convert */
    strncat(filename, ".convert", filename_size);

    struct rill_store *stores[max_shards];

    for (size_t i = 0; i < max_shards; ++i) {
        make_filename(buffer, i);
        stores[i] = rill_store_open(buffer);
    }

    if (!rill_store_merge(filename, 0, 0, stores, max_shards)) {
        fprintf(stderr, "problem merging\n");
        exit(1);
    }

    for (size_t i = 0; i < max_shards; ++i) {
        rill_store_close(stores[i]);
        make_filename(buffer, i);
        unlink(buffer);
    }

    free(filename);

    return 0;
}
