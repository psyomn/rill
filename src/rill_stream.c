#include "rill.h"

#include <string.h>
#include <stdio.h>
#include <unistd.h>
#include <stdlib.h>

void usage(const char* name) {
    fprintf(stderr,
            "usage:\n"
            "  %s database.rill\n\n", name);
    exit(1);
}

int main(int argc, char* argv[]) {
    if (argc <= 1) usage(argv[0]);

    FILE* sout = freopen(NULL, "wb", stdout);
    struct rill_store* store = rill_store_open(argv[1]);
    struct rill_store_it *it = rill_store_begin(store);
    const size_t pairs_count = rill_store_pairs(store);

    const rill_ts_t ts = rill_store_ts(store);
    const size_t quant = rill_store_quant(store);

    struct rill_kv kv = {0};

    fwrite(&ts, sizeof(ts), 1, sout);
    fwrite(&quant, sizeof(quant), 1, sout);
    fwrite(&pairs_count, sizeof(pairs_count), 1, sout);

    while (rill_store_it_next(it, &kv)) {
        if (rill_kv_nil(&kv)) break;
        fwrite(&kv, sizeof(kv), 1, sout);
    }

    fclose(sout);
    rill_store_it_free(it);
    rill_store_close(store);
    return 0;
}
