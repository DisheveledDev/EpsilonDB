#include "epsilon_analytics.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

typedef struct {
    char partition[256];
    char keyspace[128];
    uint64_t reads;
    uint64_t writes;
    uint64_t updates;
    uint64_t deletes;
    uint64_t read_us;
    uint64_t write_us;
    uint64_t size_bytes;
} shard_stat;

typedef struct {
    char kind[16];          /* "read" | "write" | "delete" | "query" */
    char partition[256];
    char keyspace[128];
    char filter_key[256];   /* sorted, comma-joined filter keys; "" if none */
    uint64_t count;
    uint64_t total_us;
    uint64_t max_us;
} slow_stat;

struct edb_analytics {
    edb_config *cfg;
    edb_engine *engine;
    char node_id[64];
    edb_analytics_flush_fn flush;
    void *flush_ctx;
    edb_analytics_cluster_fn cluster_fn;
    void *cluster_ctx;

    pthread_mutex_t lock;
    shard_stat *shards;
    size_t nshards;
    size_t shards_cap;
    slow_stat *slow;
    size_t nslow;
    size_t slow_cap;

    pthread_t thread;
    bool running;
};

static long long epoch_sec(void)
{
    return (long long)time(NULL);
}

static shard_stat *find_shard(edb_analytics *a, const char *partition,
                              const char *keyspace)
{
    for (size_t i = 0; i < a->nshards; i++) {
        if (strcmp(a->shards[i].partition, partition) == 0 &&
            strcmp(a->shards[i].keyspace, keyspace) == 0) {
            return &a->shards[i];
        }
    }
    if (a->nshards == a->shards_cap) {
        size_t cap = a->shards_cap ? a->shards_cap * 2 : 64;
        shard_stat *grown = realloc(a->shards, cap * sizeof(*grown));
        if (!grown) {
            return NULL;
        }
        a->shards = grown;
        a->shards_cap = cap;
    }
    shard_stat *s = &a->shards[a->nshards++];
    memset(s, 0, sizeof(*s));
    snprintf(s->partition, sizeof(s->partition), "%s", partition);
    snprintf(s->keyspace, sizeof(s->keyspace), "%s", keyspace);
    return s;
}

static slow_stat *find_slow(edb_analytics *a, const char *kind,
                            const char *partition, const char *keyspace,
                            const char *filter_key)
{
    for (size_t i = 0; i < a->nslow; i++) {
        if (strcmp(a->slow[i].kind, kind) == 0 &&
            strcmp(a->slow[i].partition, partition) == 0 &&
            strcmp(a->slow[i].keyspace, keyspace) == 0 &&
            strcmp(a->slow[i].filter_key, filter_key) == 0) {
            return &a->slow[i];
        }
    }
    if (a->nslow == a->slow_cap) {
        size_t cap = a->slow_cap ? a->slow_cap * 2 : 64;
        slow_stat *grown = realloc(a->slow, cap * sizeof(*grown));
        if (!grown) {
            return NULL;
        }
        a->slow = grown;
        a->slow_cap = cap;
    }
    slow_stat *s = &a->slow[a->nslow++];
    memset(s, 0, sizeof(*s));
    snprintf(s->kind, sizeof(s->kind), "%s", kind);
    snprintf(s->partition, sizeof(s->partition), "%s", partition);
    snprintf(s->keyspace, sizeof(s->keyspace), "%s", keyspace);
    snprintf(s->filter_key, sizeof(s->filter_key), "%s", filter_key);
    return s;
}

static int cmp_string(const void *pa, const void *pb)
{
    const char *const *a = pa;
    const char *const *b = pb;
    return strcmp(*a, *b);
}

/* Sorts filter keys and joins them with commas so the same set of filters
 * maps to one slow-query entry regardless of order. */
static void join_filter_keys(const char *const *keys, size_t nkeys,
                             char *out, size_t cap)
{
    out[0] = '\0';
    if (nkeys == 0) {
        return;
    }
    const char **sorted = malloc(nkeys * sizeof(char *));
    if (!sorted) {
        return;
    }
    for (size_t i = 0; i < nkeys; i++) {
        sorted[i] = keys[i];
    }
    qsort(sorted, nkeys, sizeof(char *), cmp_string);
    size_t used = 0;
    for (size_t i = 0; i < nkeys; i++) {
        if (i > 0 && used + 1 < cap) {
            out[used++] = ',';
        }
        size_t len = strlen(sorted[i]);
        if (used + len >= cap) {
            break;
        }
        memcpy(out + used, sorted[i], len);
        used += len;
    }
    out[used] = '\0';
    free(sorted);
}

/* --- recording -------------------------------------------------------- */

static void record_read_locked(edb_analytics *a, const char *partition,
                               const char *keyspace, long long latency_us)
{
    shard_stat *s = find_shard(a, partition, keyspace);
    if (!s) {
        return;
    }
    s->reads++;
    s->read_us += (uint64_t)latency_us;
}

static void record_slow_locked(edb_analytics *a, const char *kind,
                               const char *partition, const char *keyspace,
                               const char *filter_key, long long latency_us)
{
    slow_stat *s = find_slow(a, kind, partition, keyspace, filter_key);
    if (!s) {
        return;
    }
    s->count++;
    s->total_us += (uint64_t)latency_us;
    if ((uint64_t)latency_us > s->max_us) {
        s->max_us = (uint64_t)latency_us;
    }
}

void edb_analytics_record_read(edb_analytics *a, const char *partition,
                               const char *keyspace, long long latency_us)
{
    if (!a) {
        return;
    }
    pthread_mutex_lock(&a->lock);
    record_read_locked(a, partition, keyspace, latency_us);
    record_slow_locked(a, "read", partition, keyspace, "", latency_us);
    pthread_mutex_unlock(&a->lock);
}

void edb_analytics_record_write(edb_analytics *a, const char *partition,
                                const char *keyspace, bool update,
                                long long latency_us)
{
    if (!a) {
        return;
    }
    pthread_mutex_lock(&a->lock);
    shard_stat *s = find_shard(a, partition, keyspace);
    if (s) {
        s->writes++;
        if (update) {
            s->updates++;
        }
        s->write_us += (uint64_t)latency_us;
    }
    record_slow_locked(a, "write", partition, keyspace, "", latency_us);
    pthread_mutex_unlock(&a->lock);
}

void edb_analytics_record_delete(edb_analytics *a, const char *partition,
                                 const char *keyspace, long long latency_us)
{
    if (!a) {
        return;
    }
    pthread_mutex_lock(&a->lock);
    shard_stat *s = find_shard(a, partition, keyspace);
    if (s) {
        s->deletes++;
        s->write_us += (uint64_t)latency_us;
    }
    record_slow_locked(a, "delete", partition, keyspace, "", latency_us);
    pthread_mutex_unlock(&a->lock);
}

void edb_analytics_record_query(edb_analytics *a, const char *partition,
                                const char *keyspace,
                                const char *const *filter_keys, size_t nkeys,
                                long long latency_us)
{
    if (!a) {
        return;
    }
    char filter_key[256];
    join_filter_keys(filter_keys, nkeys, filter_key, sizeof(filter_key));
    pthread_mutex_lock(&a->lock);
    record_read_locked(a, partition, keyspace, latency_us);
    record_slow_locked(a, "query", partition, keyspace, filter_key,
                       latency_us);
    pthread_mutex_unlock(&a->lock);
}

/* --- snapshot flush --------------------------------------------------- */

static cJSON *snapshot_json(edb_analytics *a)
{
    cJSON *doc = cJSON_CreateObject();
    if (!doc) {
        return NULL;
    }
    cJSON_AddStringToObject(doc, "node", a->node_id);
    cJSON_AddNumberToObject(doc, "ts", (double)epoch_sec());

    cJSON *shards = cJSON_AddArrayToObject(doc, "shards");
    for (size_t i = 0; i < a->nshards; i++) {
        shard_stat *s = &a->shards[i];
        if (a->engine) {
            long long bytes = edb_engine_shard_size(a->engine, s->partition,
                                                    s->keyspace);
            s->size_bytes = bytes > 0 ? (uint64_t)bytes : 0;
        }
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "partition", s->partition);
        cJSON_AddStringToObject(o, "keyspace", s->keyspace);
        cJSON_AddNumberToObject(o, "reads", (double)s->reads);
        cJSON_AddNumberToObject(o, "writes", (double)s->writes);
        cJSON_AddNumberToObject(o, "updates", (double)s->updates);
        cJSON_AddNumberToObject(o, "deletes", (double)s->deletes);
        cJSON_AddNumberToObject(o, "read_us", (double)s->read_us);
        cJSON_AddNumberToObject(o, "write_us", (double)s->write_us);
        cJSON_AddNumberToObject(o, "size_bytes", (double)s->size_bytes);
        cJSON_AddItemToArray(shards, o);
    }

    cJSON *slow = cJSON_AddArrayToObject(doc, "slow");
    for (size_t i = 0; i < a->nslow; i++) {
        slow_stat *s = &a->slow[i];
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "kind", s->kind);
        cJSON_AddStringToObject(o, "partition", s->partition);
        cJSON_AddStringToObject(o, "keyspace", s->keyspace);
        cJSON_AddStringToObject(o, "filter", s->filter_key);
        cJSON_AddNumberToObject(o, "count", (double)s->count);
        cJSON_AddNumberToObject(o, "total_us", (double)s->total_us);
        cJSON_AddNumberToObject(o, "max_us", (double)s->max_us);
        cJSON_AddItemToArray(slow, o);
    }

    /* optional cluster metrics (e.g. replication backlog) merged as-is */
    if (a->cluster_fn) {
        cJSON *cluster = a->cluster_fn(a->cluster_ctx);
        if (cJSON_IsObject(cluster)) {
            cJSON_AddItemToObject(doc, "cluster", cluster);
        } else {
            cJSON_Delete(cluster);
        }
    }
    return doc;
}

static void flush_snapshot(edb_analytics *a)
{
    if (!a->flush) {
        return;
    }
    cJSON *doc;
    char *json;
    pthread_mutex_lock(&a->lock);
    doc = snapshot_json(a);
    json = doc ? cJSON_PrintUnformatted(doc) : NULL;
    pthread_mutex_unlock(&a->lock);
    if (!json) {
        cJSON_Delete(doc);
        return;
    }
    a->flush(a->flush_ctx, a->node_id, json,
             epoch_sec() + EDB_ANALYTICS_TTL_SECS);
    free(json);
    cJSON_Delete(doc);
}

static void *flush_thread_main(void *arg)
{
    edb_analytics *a = arg;
    while (true) {
        for (int i = 0; i < EDB_ANALYTICS_FLUSH_SECS; i++) {
            pthread_mutex_lock(&a->lock);
            bool running = a->running;
            pthread_mutex_unlock(&a->lock);
            if (!running) {
                return NULL;
            }
            struct timespec ts = { .tv_sec = 1, .tv_nsec = 0 };
            nanosleep(&ts, NULL);
        }
        flush_snapshot(a);
    }
    return NULL;
}

/* --- lifecycle -------------------------------------------------------- */

edb_analytics *edb_analytics_start(edb_config *cfg, const char *node_id,
                                   edb_analytics_flush_fn flush, void *ctx)
{
    if (!cfg) {
        return NULL;
    }
    edb_analytics *a = calloc(1, sizeof(*a));
    if (!a) {
        return NULL;
    }
    a->cfg = cfg;
    a->engine = edb_config_engine(cfg);
    snprintf(a->node_id, sizeof(a->node_id), "%s",
             node_id && *node_id ? node_id : "local");
    a->flush = flush;
    a->flush_ctx = ctx;
    pthread_mutex_init(&a->lock, NULL);
    a->running = true;
    if (pthread_create(&a->thread, NULL, flush_thread_main, a) != 0) {
        a->running = false;
        pthread_mutex_destroy(&a->lock);
        free(a);
        return NULL;
    }
    return a;
}

void edb_analytics_set_cluster_metrics(edb_analytics *a,
                                       edb_analytics_cluster_fn fn, void *ctx)
{
    if (!a) {
        return;
    }
    pthread_mutex_lock(&a->lock);
    a->cluster_fn = fn;
    a->cluster_ctx = ctx;
    pthread_mutex_unlock(&a->lock);
}

void edb_analytics_stop(edb_analytics *a)
{
    if (!a) {
        return;
    }
    pthread_mutex_lock(&a->lock);
    a->running = false;
    pthread_mutex_unlock(&a->lock);
    pthread_join(a->thread, NULL);
    free(a->shards);
    free(a->slow);
    pthread_mutex_destroy(&a->lock);
    free(a);
}

/* --- reporting -------------------------------------------------------- */

typedef struct {
    char partition[256];
    char keyspace[128];
    uint64_t reads, writes, updates, deletes, read_us, write_us;
    uint64_t size_bytes;
} report_shard;

typedef struct {
    char kind[16];
    char partition[256];
    char keyspace[128];
    char filter_key[256];
    uint64_t count, total_us, max_us;
} report_slow;

static report_shard *agg_shard(report_shard **listp, size_t *n, size_t *cap,
                               const char *partition, const char *keyspace)
{
    report_shard *list = *listp;
    for (size_t i = 0; i < *n; i++) {
        if (strcmp(list[i].partition, partition) == 0 &&
            strcmp(list[i].keyspace, keyspace) == 0) {
            return &list[i];
        }
    }
    if (*n == *cap) {
        size_t c = *cap ? *cap * 2 : 64;
        report_shard *grown = realloc(list, c * sizeof(*grown));
        if (!grown) {
            return NULL;
        }
        *listp = grown;
        *cap = c;
        list = grown;
    }
    report_shard *s = &list[(*n)++];
    memset(s, 0, sizeof(*s));
    snprintf(s->partition, sizeof(s->partition), "%s", partition);
    snprintf(s->keyspace, sizeof(s->keyspace), "%s", keyspace);
    return s;
}

static report_slow *agg_slow(report_slow **listp, size_t *n, size_t *cap,
                             const char *kind, const char *partition,
                             const char *keyspace, const char *filter_key)
{
    report_slow *list = *listp;
    for (size_t i = 0; i < *n; i++) {
        if (strcmp(list[i].kind, kind) == 0 &&
            strcmp(list[i].partition, partition) == 0 &&
            strcmp(list[i].keyspace, keyspace) == 0 &&
            strcmp(list[i].filter_key, filter_key) == 0) {
            return &list[i];
        }
    }
    if (*n == *cap) {
        size_t c = *cap ? *cap * 2 : 64;
        report_slow *grown = realloc(list, c * sizeof(*grown));
        if (!grown) {
            return NULL;
        }
        *listp = grown;
        *cap = c;
        list = grown;
    }
    report_slow *s = &list[(*n)++];
    memset(s, 0, sizeof(*s));
    snprintf(s->kind, sizeof(s->kind), "%s", kind);
    snprintf(s->partition, sizeof(s->partition), "%s", partition);
    snprintf(s->keyspace, sizeof(s->keyspace), "%s", keyspace);
    snprintf(s->filter_key, sizeof(s->filter_key), "%s", filter_key);
    return s;
}

/* Descending on-disk size; ties keep stable order (not required). */
static int cmp_shard_size_desc(const void *pa, const void *pb)
{
    const report_shard *const *a = pa;
    const report_shard *const *b = pb;
    if ((*b)->size_bytes > (*a)->size_bytes) {
        return 1;
    }
    if ((*b)->size_bytes < (*a)->size_bytes) {
        return -1;
    }
    return 0;
}

cJSON *edb_analytics_report(edb_analytics *a)
{
    cJSON *out = cJSON_CreateObject();
    if (!out) {
        return NULL;
    }
    cJSON *nodes = cJSON_AddArrayToObject(out, "nodes");
    cJSON *shards_json = cJSON_AddArrayToObject(out, "shards");
    cJSON *slow_json = cJSON_AddArrayToObject(out, "slow");

    report_shard *shards = NULL;
    size_t nshards = 0, shards_cap = 0;
    report_slow *slow = NULL;
    size_t nslow = 0, slow_cap = 0;
    uint64_t pending_changes = 0;

    if (a && a->engine) {
        cJSON *snapshots = edb_all(a->engine, EDB_SYSTEM_DB,
                                   EDB_ANALYTICS_KEYSPACE, NULL);
        const cJSON *snap = NULL;
        cJSON_ArrayForEach(snap, snapshots) {
            if (!cJSON_IsObject(snap)) {
                continue;
            }
            const cJSON *jnode = cJSON_GetObjectItemCaseSensitive(snap, "node");
            if (cJSON_IsString(jnode) && jnode->valuestring) {
                cJSON_AddItemToArray(nodes, cJSON_CreateString(jnode->valuestring));
            }
            const cJSON *jshards = cJSON_GetObjectItemCaseSensitive(snap, "shards");
            const cJSON *s = NULL;
            cJSON_ArrayForEach(s, jshards) {
                if (!cJSON_IsObject(s)) {
                    continue;
                }
                const cJSON *jp = cJSON_GetObjectItemCaseSensitive(s, "partition");
                const cJSON *jk = cJSON_GetObjectItemCaseSensitive(s, "keyspace");
                if (!cJSON_IsString(jp) || !cJSON_IsString(jk)) {
                    continue;
                }
                report_shard *r = agg_shard(&shards, &nshards, &shards_cap,
                                            jp->valuestring, jk->valuestring);
                if (!r) {
                    continue;
                }
                r->reads += (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "reads"));
                r->writes += (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "writes"));
                r->updates += (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "updates"));
                r->deletes += (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "deletes"));
                r->read_us += (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "read_us"));
                r->write_us += (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "write_us"));
                /* replicas may differ slightly; report the largest copy */
                uint64_t size = (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "size_bytes"));
                if (size > r->size_bytes) {
                    r->size_bytes = size;
                }
            }
            const cJSON *jslow = cJSON_GetObjectItemCaseSensitive(snap, "slow");
            cJSON_ArrayForEach(s, jslow) {
                if (!cJSON_IsObject(s)) {
                    continue;
                }
                const cJSON *jkind = cJSON_GetObjectItemCaseSensitive(s, "kind");
                const cJSON *jp = cJSON_GetObjectItemCaseSensitive(s, "partition");
                const cJSON *jk = cJSON_GetObjectItemCaseSensitive(s, "keyspace");
                const cJSON *jf = cJSON_GetObjectItemCaseSensitive(s, "filter");
                if (!cJSON_IsString(jkind) || !cJSON_IsString(jp) ||
                    !cJSON_IsString(jk) || !cJSON_IsString(jf)) {
                    continue;
                }
                report_slow *r = agg_slow(&slow, &nslow, &slow_cap,
                                          jkind->valuestring,
                                          jp->valuestring, jk->valuestring,
                                          jf->valuestring);
                if (!r) {
                    continue;
                }
                r->count += (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "count"));
                r->total_us += (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "total_us"));
                uint64_t max_us = (uint64_t)cJSON_GetNumberValue(
                    cJSON_GetObjectItemCaseSensitive(s, "max_us"));
                if (max_us > r->max_us) {
                    r->max_us = max_us;
                }
            }
            /* aggregate per-node cluster metrics (e.g. pending_changes) */
            const cJSON *jcluster =
                cJSON_GetObjectItemCaseSensitive(snap, "cluster");
            if (cJSON_IsObject(jcluster)) {
                const cJSON *jpc =
                    cJSON_GetObjectItemCaseSensitive(jcluster, "pending_changes");
                pending_changes += (uint64_t)cJSON_GetNumberValue(jpc);
            }
        }
        cJSON_Delete(snapshots);
    }

    for (size_t i = 0; i < nshards; i++) {
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "partition", shards[i].partition);
        cJSON_AddStringToObject(o, "keyspace", shards[i].keyspace);
        cJSON_AddNumberToObject(o, "reads", (double)shards[i].reads);
        cJSON_AddNumberToObject(o, "writes", (double)shards[i].writes);
        cJSON_AddNumberToObject(o, "updates", (double)shards[i].updates);
        cJSON_AddNumberToObject(o, "deletes", (double)shards[i].deletes);
        cJSON_AddNumberToObject(o, "read_us", (double)shards[i].read_us);
        cJSON_AddNumberToObject(o, "write_us", (double)shards[i].write_us);
        cJSON_AddNumberToObject(o, "size_bytes", (double)shards[i].size_bytes);
        cJSON_AddItemToArray(shards_json, o);
    }

    /* top 10 largest shards by on-disk size, descending */
    cJSON *largest = cJSON_AddArrayToObject(out, "largest_shards");
    if (nshards > 0) {
        const report_shard **sorted =
            malloc(nshards * sizeof(*sorted));
        if (sorted) {
            for (size_t i = 0; i < nshards; i++) {
                sorted[i] = &shards[i];
            }
            qsort(sorted, nshards, sizeof(*sorted), cmp_shard_size_desc);
            size_t top = nshards < 10 ? nshards : 10;
            for (size_t i = 0; i < top; i++) {
                cJSON *o = cJSON_CreateObject();
                cJSON_AddStringToObject(o, "partition", sorted[i]->partition);
                cJSON_AddStringToObject(o, "keyspace", sorted[i]->keyspace);
                cJSON_AddNumberToObject(o, "size_bytes",
                                        (double)sorted[i]->size_bytes);
                cJSON_AddItemToArray(largest, o);
            }
            free(sorted);
        }
    }
    for (size_t i = 0; i < nslow; i++) {
        cJSON *o = cJSON_CreateObject();
        cJSON_AddStringToObject(o, "kind", slow[i].kind);
        cJSON_AddStringToObject(o, "partition", slow[i].partition);
        cJSON_AddStringToObject(o, "keyspace", slow[i].keyspace);
        cJSON_AddStringToObject(o, "filter", slow[i].filter_key);
        cJSON_AddNumberToObject(o, "count", (double)slow[i].count);
        cJSON_AddNumberToObject(o, "total_us", (double)slow[i].total_us);
        cJSON_AddNumberToObject(o, "max_us", (double)slow[i].max_us);
        cJSON_AddItemToArray(slow_json, o);
    }
    cJSON *cluster = cJSON_AddObjectToObject(out, "cluster");
    cJSON_AddNumberToObject(cluster, "pending_changes", (double)pending_changes);
    free(shards);
    free(slow);
    return out;
}
