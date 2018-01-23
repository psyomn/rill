/* dump.c
   Rémi Attab (remi.attab@gmail.com), 07 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <getopt.h>
#include <stdlib.h>
#include <stdio.h>

void usage()
{
    fprintf(stderr, "rill_dump [-h] [-a] [-b] [-A] [-B] <file>\n");
    exit(1);
}

int main(int argc, char **argv)
{
    bool header = false;
    bool keys_a = false;
    bool keys_b = false;
    bool pairs_a = false;
    bool pairs_b = false;

    int opt = 0;
    while ((opt = getopt(argc, argv, "+habABD")) != -1) {
        switch (opt) {
        case 'h': header = true; break;
        case 'a': keys_a = true; break;
        case 'b': keys_b = true; break;
        case 'A': pairs_a = true; break;
        case 'B': pairs_b = true; break;
        default:
            fprintf(stderr, "unknown argument: %c\n", opt);
            usage();
        }
    }

    if (!header && !keys_a && !keys_b && !pairs_a && !pairs_b) usage();
    if (optind >= argc) usage();

    struct rill_store *store = rill_store_open(argv[optind]);
    if (!store) rill_exit(1);

    if (header) {
        printf("file:        %s\n", rill_store_file(store));
        printf("version:     %u\n", rill_store_version(store));
        printf("ts:          %lu\n", rill_store_ts(store));
        printf("quant:       %lu\n", rill_store_quant(store));
        printf("keys data a: %zu\n", rill_store_keys_count(store, rill_col_a));
        printf("keys data b: %zu\n", rill_store_keys_count(store, rill_col_b));
        printf("pairs:       %lu\n", rill_store_pairs(store));
        printf("index a len: %zu\n", rill_store_index_len(store, rill_col_a));
        printf("index b len: %zu\n", rill_store_index_len(store, rill_col_b));
    }

    if (keys_a || keys_b) {
        const enum rill_col col = keys_a ? rill_col_a : rill_col_b;
        const size_t keys_len = rill_store_keys_count(store, col);
        rill_key_t *keys = calloc(keys_len, sizeof(*keys));

        (void) rill_store_keys(store, keys, keys_len, col);

        printf("vals %c:\n", col ? 'b' : 'a');

        for (size_t i = 0; i < keys_len; ++i)
            printf("  0x%lx\n", keys[i]);
    }

    if (pairs_a || pairs_b) {
        struct rill_kv kv = {0};
        const enum rill_col col = pairs_a ? rill_col_a : rill_col_b;
        struct rill_store_it *it = rill_store_begin(store, col);

        printf("pairs %c:\n", pairs_a ? 'a' : 'b');
        while (rill_store_it_next(it, &kv)) {
            if (rill_kv_nil(&kv)) break;
            printf("  0x%lx 0x%lx\n", kv.key, kv.val);
        }

        rill_store_it_free(it);
    }

    rill_store_close(store);
    return 0;
}
