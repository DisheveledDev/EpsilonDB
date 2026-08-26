#include "epsilon_benchmark.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

/* Maximum records per partition we are willing to run synchronously. */
#define BENCH_MAX_RECORDS 1000000
#define BENCH_MAX_PARTITIONS 100
#define BENCH_MAX_THREADS 64
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

static void write_record(edb_engine *engine, const char *partition, int i)
{
    char id[64];
    char value[160];
    snprintf(id, sizeof(id), "rec-%d", i);
    snprintf(value, sizeof(value),
             "{\"n\":%d,\"name\":\"user-%d\",\"age\":%d,\"active\":%s}",
             i, i, i % 100, i % 2 == 0 ? "true" : "false");
    edb_put(engine, partition, BENCH_KEYSpace, id, value, -1);
}

static void delete_record(edb_engine *engine, const char *partition, int i)
{
    char id[64];
    snprintf(id, sizeof(id), "rec-%d", i);
    edb_delete(engine, partition, BENCH_KEYSpace, id);
}

/* Phase identifiers for the worker threads. */
#define PHASE_WRITES 0
#define PHASE_GETS   1
#define PHASE_QUERIES 2
#define PHASE_UPDATES 3
#define PHASE_DELETES 4

/* One worker: owns a strided subset of the partitions and runs a single
 * phase over them. Partitions never overlap between workers, so every
 * partition is read/written independently and concurrently. */
typedef struct {
    edb_engine *engine;
    const char *db_name;      /* partition name prefix */
    int partitions;
    int records_per_partition;
    int sample;               /* gets/updates sample size */
    int thread_id;
    int threads;
    int phase;
    long long ops;            /* ops performed by this worker */
} bench_worker;

static void *bench_worker_main(void *arg)
{
    bench_worker *w = arg;
    char pname[64];
    for (int p = w->thread_id; p < w->partitions; p += w->threads) {
        snprintf(pname, sizeof(pname), "%.48s_p%d", w->db_name, p);
        switch (w->phase) {
        case PHASE_WRITES:
            for (int i = 0; i < w->records_per_partition; i++) {
                write_record(w->engine, pname, i);
                w->ops++;
            }
            break;
        case PHASE_GETS:
            for (int i = 0; i < w->sample; i++) {
                char id[64];
                snprintf(id, sizeof(id), "rec-%d",
                         (i * 17) % w->records_per_partition);
                cJSON *d = edb_get(w->engine, pname, BENCH_KEYSpace, id);
                cJSON_Delete(d);
                w->ops++;
            }
            break;
        case PHASE_QUERIES:
            for (int q = 0; q < 20; q++) {
                cJSON *f = make_filter("age", "lt", 10 + q * 5);
                cJSON *rows = edb_query(w->engine, pname, BENCH_KEYSpace, f);
                cJSON_Delete(f);
                cJSON_Delete(rows);
                w->ops++;
            }
            break;
        case PHASE_UPDATES:
            for (int i = 0; i < w->sample; i++) {
                char id[64];
                char value[128];
                int idx = (i * 13) % w->records_per_partition;
                snprintf(id, sizeof(id), "rec-%d", idx);
                snprintf(value, sizeof(value),
                         "{\"n\":%d,\"name\":\"updated-%d\",\"age\":%d}",
                         idx, idx, idx % 100);
                edb_put(w->engine, pname, BENCH_KEYSpace, id, value, -1);
                w->ops++;
            }
            break;
        case PHASE_DELETES:
            for (int i = 0; i < w->records_per_partition; i++) {
                delete_record(w->engine, pname, i);
                w->ops++;
            }
            break;
        default:
            break;
        }
    }
    return NULL;
}

static bench_phase run_phase(edb_engine *engine, const char *db_name,
                             int partitions, int records, int threads,
                             int phase)
{
    int sample = records > 10000 ? 10000 : records;
    pthread_t tids[BENCH_MAX_THREADS];
    bool created[BENCH_MAX_THREADS];
    bench_worker workers[BENCH_MAX_THREADS];
    long long t0 = now_us();
    for (int t = 0; t < threads; t++) {
        memset(&workers[t], 0, sizeof(workers[t]));
        workers[t].engine = engine;
        workers[t].db_name = db_name;
        workers[t].partitions = partitions;
        workers[t].records_per_partition = records;
        workers[t].sample = sample;
        workers[t].thread_id = t;
        workers[t].threads = threads;
        workers[t].phase = phase;
        created[t] = pthread_create(&tids[t], NULL, bench_worker_main,
                                    &workers[t]) == 0;
        if (!created[t]) {
            /* thread creation failed: run this worker inline so the
             * benchmark still completes */
            bench_worker_main(&workers[t]);
        }
    }
    long long ops = 0;
    for (int t = 0; t < threads; t++) {
        if (created[t]) {
            pthread_join(tids[t], NULL);
        }
        ops += workers[t].ops;
    }
    bench_phase ph = { ops, now_us() - t0 };
    return ph;
}

cJSON *edb_benchmark_run(edb_config *cfg, int replication_factor,
                         int partitions, int records_per_partition,
                         int threads)
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
    /* Concurrency: one thread per partition by default; never more threads
     * than partitions (a worker with no partitions would just idle). */
    if (threads < 1 || threads > partitions) {
        threads = partitions;
    }
    if (threads > BENCH_MAX_THREADS) {
        threads = BENCH_MAX_THREADS;
    }

    edb_engine *engine = edb_config_engine(cfg);

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
    cJSON_AddNumberToObject(report, "threads", (double)threads);
    cJSON_AddNumberToObject(report, "replication_factor",
                            (double)replication_factor);

    /* --- setup --------------------------------------------------------- */
    edb_database_create(cfg, db_name, replication_factor);
    for (int p = 0; p < partitions; p++) {
        char pname[64];
        snprintf(pname, sizeof(pname), "%.48s_p%d", db_name, p);
        edb_partition_create(cfg, db_name, pname, EDB_MASK_ALLOW_ALL,
                             EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL,
                             EDB_MASK_ALLOW_ALL);
    }

    /* --- writes -------------------------------------------------------- */
    phase_add(report, "writes",
              run_phase(engine, db_name, partitions, records_per_partition,
                        threads, PHASE_WRITES));

    /* --- gets (sample) ------------------------------------------------- */
    phase_add(report, "gets",
              run_phase(engine, db_name, partitions, records_per_partition,
                        threads, PHASE_GETS));

    /* --- queries (filtered) ------------------------------------------- */
    phase_add(report, "queries",
              run_phase(engine, db_name, partitions, records_per_partition,
                        threads, PHASE_QUERIES));

    /* --- updates (sample) --------------------------------------------- */
    phase_add(report, "updates",
              run_phase(engine, db_name, partitions, records_per_partition,
                        threads, PHASE_UPDATES));

    /* --- deletes (back to no content) --------------------------------- */
    phase_add(report, "deletes",
              run_phase(engine, db_name, partitions, records_per_partition,
                        threads, PHASE_DELETES));

    /* --- teardown: delete shard files, partitions and the database ----- */
    for (int p = 0; p < partitions; p++) {
        char pname[64];
        char path[1024];
        char key[33];
        snprintf(pname, sizeof(pname), "%.48s_p%d", db_name, p);
        if (edb_shard_path(engine, pname, BENCH_KEYSpace, path, sizeof(path),
                           key)) {
            edb_shard_gc(engine, key);
        }
    }
    edb_database_delete(cfg, db_name);

    return report;
}
