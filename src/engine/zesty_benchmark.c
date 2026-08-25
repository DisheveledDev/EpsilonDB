#include "zesty_benchmark.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Maximum records per partition we are willing to run synchronously. */
#define BENCH_MAX_RECORDS 1000000
#define BENCH_MAX_PARTITIONS 100
#define BENCH_KEYSpace "data"

typedef struct {
    long long count;
    long long us;
} bench_phase;

static long long now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

static void phase_add(cJSON *obj, const char *name, bench_phase p)
{
    double seconds = p.us / 1000000.0;
    double rate = seconds > 0 ? (double)p.count / seconds : 0;
    cJSON *o = cJSON_AddObjectToObject(obj, name);
    cJSON_AddNumberToObject(o, "count", (double)p.count);
    cJSON_AddNumberToObject(o, "seconds", seconds);
    cJSON_AddNumberToObject(o, "ops_per_sec", rate);
}

static cJSON *make_filter(const char *key, const char *op, double value)
{
    cJSON *f = cJSON_CreateObject();
    if (!f) {
        return NULL;
    }
    cJSON_AddStringToObject(f, "key", key);
    cJSON_AddStringToObject(f, "operator", op);
    cJSON_AddNumberToObject(f, "value", value);
    return f;
}

static void write_record(zdb_engine *engine, const char *partition, int i)
{
    char id[64];
    char value[160];
    snprintf(id, sizeof(id), "rec-%d", i);
    snprintf(value, sizeof(value),
             "{\"n\":%d,\"name\":\"user-%d\",\"age\":%d,\"active\":%s}",
             i, i, i % 100, i % 2 == 0 ? "true" : "false");
    zdb_put(engine, partition, BENCH_KEYSpace, id, value, -1);
}

static void delete_record(zdb_engine *engine, const char *partition, int i)
{
    char id[64];
    snprintf(id, sizeof(id), "rec-%d", i);
    zdb_delete(engine, partition, BENCH_KEYSpace, id);
}

cJSON *zdb_benchmark_run(zdb_config *cfg, int replication_factor,
                         long long cache_size, const char *journal_mode,
                         int partitions, int records_per_partition)
{
    if (!cfg) {
        return NULL;
    }
    if (replication_factor < 1) {
        replication_factor = 1;
    }
    if (partitions < 1 || partitions > BENCH_MAX_PARTITIONS) {
        partitions = 10;
    }
    if (records_per_partition < 1 ||
        records_per_partition > BENCH_MAX_RECORDS) {
        records_per_partition = 100000;
    }
    if (!journal_mode || !*journal_mode) {
        journal_mode = "TRUNCATE";
    }

    zdb_engine *engine = zdb_config_engine(cfg);

    char db_name[128];
    snprintf(db_name, sizeof(db_name), "__bench_%ld_%d", (long)time(NULL),
             (int)getpid());

    cJSON *report = cJSON_CreateObject();
    if (!report) {
        return NULL;
    }
    cJSON_AddStringToObject(report, "database", db_name);
    cJSON_AddNumberToObject(report, "partitions", (double)partitions);
    cJSON_AddNumberToObject(report, "records_per_partition",
                            (double)records_per_partition);
    cJSON_AddNumberToObject(report, "total_records",
                            (double)partitions * records_per_partition);
    cJSON_AddNumberToObject(report, "replication_factor",
                            (double)replication_factor);
    cJSON_AddNumberToObject(report, "cache_size", (double)cache_size);
    cJSON_AddStringToObject(report, "journal_mode", journal_mode);

    /* --- setup --------------------------------------------------------- */
    zdb_database_create(cfg, db_name, replication_factor);
    zdb_shard_settings settings;
    zdb_shard_settings_default(&settings);
    settings.cache_size = cache_size;
    snprintf(settings.journal_mode, sizeof(settings.journal_mode), "%s",
             journal_mode);
    for (int p = 0; p < partitions; p++) {
        char pname[64];
        snprintf(pname, sizeof(pname), "p%d", p);
        zdb_partition_create(cfg, db_name, pname, ZDB_MASK_ALLOW_ALL,
                             ZDB_MASK_ALLOW_ALL, ZDB_MASK_ALLOW_ALL,
                             ZDB_MASK_ALLOW_ALL);
        zdb_partition_set_settings(cfg, db_name, pname, &settings);
    }

    /* --- writes -------------------------------------------------------- */
    {
        long long t0 = now_us();
        for (int p = 0; p < partitions; p++) {
            char pname[64];
            snprintf(pname, sizeof(pname), "p%d", p);
            for (int i = 0; i < records_per_partition; i++) {
                write_record(engine, pname, i);
            }
        }
        bench_phase ph = { (long long)partitions * records_per_partition,
                           now_us() - t0 };
        phase_add(report, "writes", ph);
    }

    /* --- gets (sample) ------------------------------------------------- */
    {
        int sample = records_per_partition > 10000 ? 10000
                                                   : records_per_partition;
        long long t0 = now_us();
        for (int p = 0; p < partitions; p++) {
            char pname[64];
            snprintf(pname, sizeof(pname), "p%d", p);
            for (int i = 0; i < sample; i++) {
                char id[64];
                snprintf(id, sizeof(id), "rec-%d",
                         (i * 17) % records_per_partition);
                cJSON *d = zdb_get(engine, pname, BENCH_KEYSpace, id);
                cJSON_Delete(d);
            }
        }
        bench_phase ph = { (long long)partitions * sample, now_us() - t0 };
        phase_add(report, "gets", ph);
    }

    /* --- queries (filtered) ------------------------------------------- */
    {
        int sample = 20;
        long long t0 = now_us();
        for (int p = 0; p < partitions; p++) {
            char pname[64];
            snprintf(pname, sizeof(pname), "p%d", p);
            for (int q = 0; q < sample; q++) {
                cJSON *f = make_filter("age", "lt", 10 + q * 5);
                cJSON *rows = zdb_query(engine, pname, BENCH_KEYSpace, f);
                cJSON_Delete(f);
                cJSON_Delete(rows);
            }
        }
        bench_phase ph = { (long long)partitions * sample, now_us() - t0 };
        phase_add(report, "queries", ph);
    }

    /* --- updates (sample) --------------------------------------------- */
    {
        int sample = records_per_partition > 10000 ? 10000
                                                   : records_per_partition;
        long long t0 = now_us();
        for (int p = 0; p < partitions; p++) {
            char pname[64];
            snprintf(pname, sizeof(pname), "p%d", p);
            for (int i = 0; i < sample; i++) {
                char id[64];
                char value[128];
                int idx = (i * 13) % records_per_partition;
                snprintf(id, sizeof(id), "rec-%d", idx);
                snprintf(value, sizeof(value),
                         "{\"n\":%d,\"name\":\"updated-%d\",\"age\":%d}",
                         idx, idx, idx % 100);
                zdb_put(engine, pname, BENCH_KEYSpace, id, value, -1);
            }
        }
        bench_phase ph = { (long long)partitions * sample, now_us() - t0 };
        phase_add(report, "updates", ph);
    }

    /* --- deletes (back to no content) --------------------------------- */
    {
        long long t0 = now_us();
        for (int p = 0; p < partitions; p++) {
            char pname[64];
            snprintf(pname, sizeof(pname), "p%d", p);
            for (int i = 0; i < records_per_partition; i++) {
                delete_record(engine, pname, i);
            }
        }
        bench_phase ph = { (long long)partitions * records_per_partition,
                           now_us() - t0 };
        phase_add(report, "deletes", ph);
    }

    /* --- teardown: delete shard files, partitions and the database ----- */
    for (int p = 0; p < partitions; p++) {
        char pname[64];
        char path[1024];
        char key[33];
        snprintf(pname, sizeof(pname), "p%d", p);
        if (zdb_shard_path(engine, pname, BENCH_KEYSpace, path, sizeof(path),
                           key)) {
            zdb_shard_gc(engine, key);
        }
    }
    zdb_database_delete(cfg, db_name);

    return report;
}
