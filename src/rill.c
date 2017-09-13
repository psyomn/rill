/* rill.c
   Rémi Attab (remi.attab@gmail.com), 03 Sep 2017
   FreeBSD-style copyright and disclaimer apply
*/

#include "rill.h"
#include "utils.h"

#include <assert.h>
#include <stdlib.h>
#include <stdatomic.h>
#include <dirent.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/types.h>


// -----------------------------------------------------------------------------
// kv
// -----------------------------------------------------------------------------

extern inline bool rill_kv_nil(const struct rill_kv *);
extern inline int rill_kv_cmp(const struct rill_kv *, const struct rill_kv *);


// -----------------------------------------------------------------------------
// config
// -----------------------------------------------------------------------------

enum { hours = 24, days = 30, months = 13};
enum
{
    quant_hour = 60 * 60,
    quant_day = hours * quant_hour,
    quant_month = days * quant_day,
};


// -----------------------------------------------------------------------------
// rill
// -----------------------------------------------------------------------------

struct rill
{
    const char *dir;
    rill_ts_t ts;

    lock_t lock;
    struct rill_pairs *acc;
    struct rill_pairs *dump;

    struct rill_store *hourly[hours];
    struct rill_store *daily[days];
    struct rill_store *monthly[months];
};

static bool load_store(struct rill *db, const char *file)
{
    struct rill_store *store = rill_store_open(file);
    if (!store) goto fail_open;

    rill_ts_t ts = rill_store_ts(store);
    size_t quant = rill_store_quant(store);

    struct rill_store **bucket = NULL;
    switch (quant) {
    case quant_hour: bucket = &db->hourly[(ts / quant_hour) % hours]; break;
    case quant_day: bucket = &db->daily[(ts / quant_hour) % days]; break;
    case quant_month: bucket = &db->monthly[(ts / quant_month) % months]; break;
    default:
        fail("unknown quant '%lu' for '%s'", quant, file);
        goto fail_quant;
    }

    if (*bucket) {
        fail("file '%s' is a duplicate for quant '%lu' at timestamp %lu'",
                file, quant, ts);
        goto fail_dup;
    }

    *bucket = store;

    return true;

  fail_dup:
  fail_quant:
    rill_store_close(store);
  fail_open:
    return false;
}

struct rill * rill_open(const char *dir)
{
    struct rill *db = calloc(1, sizeof(*db));
    if (!db) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_struct;
    }

    db->dir = strndup(dir, PATH_MAX);
    if (!db->dir) {
        fail("unable to allocate memory for '%s'", dir);
        goto fail_alloc_dir;
    }

    if (!(db->acc = rill_pairs_new(1 * 1000 * 1000))) {
        fail("unable to allocate pairs for '%s'", dir);
        goto fail_alloc_acc;
    }

    if (!(db->dump = rill_pairs_new(1 * 1000 * 1000))) {
        fail("unable to allocate pairs for '%s'", dir);
        goto fail_alloc_dump;
    }

    if (mkdir(dir, 0775) == -1 && errno != EEXIST) {
        fail_errno("unable to open create dir '%s'", dir);
        goto fail_mkdir;
    }

    DIR *dir_handle = opendir(dir);
    if (!dir_handle) {
        fail_errno("unable to open dir '%s'", dir);
        goto fail_dir;
    }

    struct dirent it, *entry;
    while (true) {
        if (readdir_r(dir_handle, &it, &entry) == -1) {
            fail_errno("unable to read dir '%s'", dir);
            goto fail_readdir;
        }
        else if (!entry) break;
        else if (entry->d_type != DT_REG) continue;

        char file[PATH_MAX];
        snprintf(file, sizeof(file), "%s/%s", db->dir, entry->d_name);
        (void) load_store(db, file);
    }

    closedir(dir_handle);

    return db;

  fail_readdir:
    closedir(dir_handle);
  fail_dir:
  fail_mkdir:
    rill_pairs_free(db->dump);
  fail_alloc_dump:
    rill_pairs_free(db->acc);
  fail_alloc_acc:
    free((char *) db->dir);
  fail_alloc_dir:
    free(db);
  fail_alloc_struct:
    return NULL;
}

void rill_close(struct rill *db)
{
    for (size_t i = 0; i < hours; ++i) {
        if (db->hourly[i]) rill_store_close(db->hourly[i]);
    }

    for (size_t i = 0; i < days; ++i) {
        if (db->daily[i]) rill_store_close(db->daily[i]);
    }

    for (size_t i = 0; i < months; ++i) {
        if (db->monthly[i]) rill_store_close(db->monthly[i]);
    }


    free((char *) db->dir);
    rill_pairs_free(db->acc);
    rill_pairs_free(db->dump);
    free(db);
}


// -----------------------------------------------------------------------------
// ingest
// -----------------------------------------------------------------------------

bool rill_ingest(struct rill *db, rill_key_t key, rill_val_t val)
{
    if (!key) {
        fail("invalid nil key '%lu'", key);
        return false;
    }

    if (!val) {
        fail("invalid nil value '%lu'", val);
        return false;
    }

    struct rill_pairs *result;
    {
        lock(&db->lock);

        result = rill_pairs_push(db->acc, key, val);

        unlock(&db->lock);
    }

    if (result) db->acc = result;
    return result != NULL;
}


// -----------------------------------------------------------------------------
// rotate
// -----------------------------------------------------------------------------
// \todo since we're deleting data, should be reviewed for robustness.
//
// \todo if we have a gap in ingestion, it's possible that we don't expire some
//       data or that we hit one of the asserts. Need to improve the mechanism a
//       bit.


static bool rotate_monthly(
        struct rill *db,
        struct rill_store **store,
        rill_ts_t ts,
        struct rill_store **list, size_t len)
{
    char file[PATH_MAX];
    snprintf(file, sizeof(file), "%s/%06lu.rill", db->dir, ts / quant_month);

    if (*store) {
        (void) rill_store_rm(*store);
        *store = NULL;
    }

    bool all_null = true;
    for (size_t i = 0; i < len; ++i) all_null = all_null && !list[i];
    if (all_null) return true;

    if (!rill_store_merge(file, ts, quant_month, list, len)) return false;
    if (!(*store = rill_store_open(file))) return false;

    for (size_t i = 0; i < len; ++i) {
        if (!list[i]) continue;
        (void) rill_store_rm(list[i]);
        list[i] = NULL;
    }

    return true;
}

static bool rotate_daily(
        struct rill *db,
        struct rill_store **store,
        rill_ts_t ts,
        struct rill_store **list, size_t len)
{
    char file[PATH_MAX];
    snprintf(file, sizeof(file), "%s/%06lu-%02lu.rill", db->dir,
            ts / quant_month,
            (ts / quant_day) % days);

    assert(!*store);

    bool all_null = true;
    for (size_t i = 0; i < len; ++i) all_null = all_null && !list[i];
    if (all_null) return true;

    if (!rill_store_merge(file, ts, quant_day, list, len)) return false;
    if (!(*store = rill_store_open(file))) return false;

    for (size_t i = 0; i < len; ++i) {
        if (!list[i]) continue;
        (void) rill_store_rm(list[i]);
        list[i] = NULL;
    }

    return true;
}

static bool rotate_hourly(struct rill *db, struct rill_store **store, rill_ts_t ts)
{
    char file[PATH_MAX];
    snprintf(file, sizeof(file), "%s/%06lu-%02lu-%02lu.rill", db->dir,
            ts / quant_month,
            (ts / quant_day) % days,
            (ts / quant_hour) % hours);

    {
        lock(&db->lock);

        struct rill_pairs *tmp = db->acc;
        db->acc = db->dump;
        db->dump = tmp;

        unlock(&db->lock);
    }

    assert(!*store);
    if (db->dump->len) {
        if (!rill_store_write(file, ts, quant_hour, db->dump)) return false;
        if (!(*store = rill_store_open(file))) return false;
    }

    rill_pairs_clear(db->dump);
    return true;
}

bool rill_rotate(struct rill *db, rill_ts_t now)
{
    if (now / quant_hour != db->ts / quant_hour) {
        size_t quant = db->ts / quant_hour;
        if (!rotate_hourly(db, &db->hourly[(now / quant_hour) % hours], db->ts)) {
            fail("unable to complete hourly rotation '%lu'", quant);
            return false;
        }
    }

    if (now / quant_day != db->ts / quant_day) {
        size_t quant = db->ts / quant_day;
        if (!rotate_daily(db, &db->daily[quant % days], db->ts, db->hourly, hours)) {
            fail("unable to complete daily rotation '%lu'", quant);
            return false;
        }
    }

    if (now / quant_month != db->ts / quant_month) {
        size_t quant = db->ts / quant_month;
        if (!rotate_monthly(db, &db->monthly[quant % months], db->ts, db->daily, days)) {
            fail("unable to complete monthly rotation '%lu'", quant);
            return false;
        }
    }

    db->ts = now;
    return true;
}


// -----------------------------------------------------------------------------
// query
// -----------------------------------------------------------------------------

struct rill_pairs *rill_query_key(
        struct rill *db,
        const rill_key_t *keys, size_t len,
        struct rill_pairs *out)
{
    struct rill_pairs *result = out;
    if (!len) return result;

    for (size_t i = 0; i < hours; ++i) {
        if (!db->hourly[i]) continue;
        result = rill_store_scan_key(db->hourly[i], keys, len, result);
        if (!result) return NULL;
    }

    for (size_t i = 0; i < days; ++i) {
        if (!db->daily[i]) continue;
        result = rill_store_scan_key(db->daily[i], keys, len, result);
        if (!result) return NULL;
    }

    for (size_t i = 0; i < months; ++i) {
        if (!db->monthly[i]) continue;
        result = rill_store_scan_key(db->monthly[i], keys, len, result);
        if (!result) return NULL;
    }

    rill_pairs_compact(result);
    return result;
}

struct rill_pairs *rill_query_val(
        struct rill *db,
        const rill_val_t *vals, size_t len,
        struct rill_pairs *out)
{
    struct rill_pairs *result = out;
    if (!len) return result;

    for (size_t i = 0; i < hours; ++i) {
        if (!db->hourly[i]) continue;
        result = rill_store_scan_val(db->hourly[i], vals, len, result);
        if (!result) return result;
    }

    for (size_t i = 0; i < days; ++i) {
        if (!db->daily[i]) continue;
        result = rill_store_scan_val(db->daily[i], vals, len, result);
        if (!result) return result;
    }

    for (size_t i = 0; i < months; ++i) {
        if (!db->monthly[i]) continue;
        result = rill_store_scan_val(db->monthly[i], vals, len, result);
        if (!result) return result;
    }

    rill_pairs_compact(result);
    return result;
}
