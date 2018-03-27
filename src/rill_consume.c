#include "rill.h"

#include <stdio.h>
#include <limits.h>
#include <stdlib.h>
#include <unistd.h>

enum
{
    mins_in_hour = 60,
    hours_in_day = 24,
    days_in_week = 8, // more closely approximates a month
    weeks_in_month = 4,
    months_in_expire = 16,

    sec_secs = 1,
    min_secs = 60 * sec_secs,
    hour_secs = mins_in_hour * min_secs,
    day_secs = hours_in_day * hour_secs,
    week_secs = days_in_week * day_secs,
    month_secs = weeks_in_month * week_secs,
    expire_secs = months_in_expire * month_secs,
};

static inline void make_filename(char path[static PATH_MAX], size_t num) {
    path[0] = 0;
    snprintf(path, PATH_MAX, "%03ld.rill.shard", num);
}

static bool file_name(
    const char *dir,
    rill_ts_t ts,
    rill_ts_t quant,
    char *out,
    size_t len)
{
    rill_ts_t month = ts / month_secs;
    rill_ts_t week = (ts / week_secs) % weeks_in_month;
    rill_ts_t day = (ts / day_secs) % days_in_week;
    rill_ts_t hour = (ts / hour_secs) % hours_in_day;

    char base[NAME_MAX];
    if (quant == hour_secs)
        snprintf(base, sizeof(base), "%s/%05lu-%02lu-%02lu-%02lu.rill",
                dir, month, week, day, hour);
    else if (quant == day_secs)
        snprintf(base, sizeof(base), "%s/%05lu-%02lu-%02lu.rill", dir, month, week, day);
    else if (quant == week_secs)
        snprintf(base, sizeof(base), "%s/%05lu-%02lu.rill", dir, month, week);
    else if (quant == month_secs)
        snprintf(base, sizeof(base), "%s/%05lu.rill", dir, month);
    else assert(false);

    strncpy(out, base, len < sizeof(base) ? len : sizeof(base));

    int ret;
    size_t i = 0;
    while ((ret = file_exists(out)) == 1)
        snprintf(out, len, "%s.%lu", base, i++);

    if (ret == -1) return false;
    return true;
}

int main(int argc, char* argv[]) {
    (void) argc, (void) argv;

    enum { max_shards = 10 };
    FILE* sin = freopen(NULL, "rb", stdin);
    size_t pairs_count = 0;
    rill_ts_t ts = 0;
    rill_ts_t quant = 0;

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
        read_amount +=
            pairs_left - chunk < chunk ? pairs_left - chunk : 0;

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

    /* merge shards */
    const char* final = "final.rill";
    snprintf(final_name, PATH_MAX,
             "%zu-%zu-convert.rill",
             original);

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
