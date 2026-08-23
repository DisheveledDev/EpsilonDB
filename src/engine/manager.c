#include "shard_internal.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>

#include "md5.h"

#define ZDB_SHARD_BUCKETS 512

struct shard_link {
    zdb_shard *shard;
    struct shard_link *next;
};

struct zdb_shard_manager {
    char *path;
    pthread_mutex_t lock;              /* guards buckets + bucket counts */
    struct shard_link *buckets[ZDB_SHARD_BUCKETS];

    pthread_t cleanup_thread;
    bool cleanup_running;
    pthread_mutex_t wakeup_lock;
    pthread_cond_t wakeup_cond;
};

static size_t bucket_for(const char *key)
{
    /* key is a 32-char lowercase hex md5; fold the first two hex digits. */
    unsigned int hi = (unsigned int)(key[0] <= '9' ? key[0] - '0'
                                                   : key[0] - 'a' + 10);
    unsigned int lo = (unsigned int)(key[1] <= '9' ? key[1] - '0'
                                                   : key[1] - 'a' + 10);
    return (size_t)((hi << 4) | lo) & (ZDB_SHARD_BUCKETS - 1);
}

static void shard_key(const char *partition, const char *keyspace,
                      char out[33])
{
    char combined[1024];
    snprintf(combined, sizeof(combined), "%s%s", partition, keyspace);
    zdb_md5_hex(combined, strlen(combined), out);
}

/* Look up a shard by key without opening it. Caller holds mgr->lock. */
static zdb_shard *find_locked(zdb_engine *mgr, const char *key)
{
    for (struct shard_link *l = mgr->buckets[bucket_for(key)]; l; l = l->next) {
        if (strcmp(l->shard->key, key) == 0) {
            return l->shard;
        }
    }
    return NULL;
}

/* Insert an opened shard. Takes ownership on success. Caller must not hold
 * mgr->lock. Returns the shard, or NULL after freeing it if the key was
 * inserted concurrently. */
static zdb_shard *insert_shard(zdb_engine *mgr, zdb_shard *sh)
{
    pthread_mutex_lock(&mgr->lock);
    zdb_shard *existing = find_locked(mgr, sh->key);
    if (existing) {
        pthread_mutex_unlock(&mgr->lock);
        zdb_shard_free(sh);
        return existing;
    }
    struct shard_link *link = malloc(sizeof(*link));
    if (!link) {
        pthread_mutex_unlock(&mgr->lock);
        zdb_shard_free(sh);
        return NULL;
    }
    link->shard = sh;
    link->next = mgr->buckets[bucket_for(sh->key)];
    mgr->buckets[bucket_for(sh->key)] = link;
    pthread_mutex_unlock(&mgr->lock);
    return sh;
}

/* Returns the shard for partition/keyspace, opening (and inserting) it on
 * first use. */
static zdb_shard *shard_for(zdb_engine *mgr, const char *partition,
                            const char *keyspace)
{
    char key[33];
    shard_key(partition, keyspace, key);

    pthread_mutex_lock(&mgr->lock);
    zdb_shard *sh = find_locked(mgr, key);
    pthread_mutex_unlock(&mgr->lock);
    if (sh) {
        return sh;
    }

    size_t len = strlen(mgr->path) + 1 + 32 + sizeof(".sqlite");
    char *path = malloc(len);
    if (!path) {
        return NULL;
    }
    snprintf(path, len, "%s/%s.sqlite", mgr->path, key);
    zdb_shard *opened = zdb_shard_open(path, key);
    free(path);
    if (!opened) {
        return NULL;
    }
    return insert_shard(mgr, opened);
}

static void close_all(zdb_engine *mgr)
{
    pthread_mutex_lock(&mgr->lock);
    for (size_t i = 0; i < ZDB_SHARD_BUCKETS; i++) {
        struct shard_link *l = mgr->buckets[i];
        while (l) {
            struct shard_link *next = l->next;
            zdb_shard_free(l->shard);
            free(l);
            l = next;
        }
        mgr->buckets[i] = NULL;
    }
    pthread_mutex_unlock(&mgr->lock);
}

static void cleanup_all_shards(zdb_engine *mgr)
{
    pthread_mutex_lock(&mgr->lock);
    for (size_t i = 0; i < ZDB_SHARD_BUCKETS; i++) {
        for (struct shard_link *l = mgr->buckets[i]; l; l = l->next) {
            zdb_shard_cleanup(l->shard);
        }
    }
    pthread_mutex_unlock(&mgr->lock);
}

static void *cleanup_thread_main(void *arg)
{
    zdb_engine *mgr = arg;

    pthread_mutex_lock(&mgr->wakeup_lock);
    while (mgr->cleanup_running) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += ZDB_CLEANUP_INTERVAL_SECONDS;
        int rc = 0;
        while (mgr->cleanup_running && rc != ETIMEDOUT) {
            rc = pthread_cond_timedwait(&mgr->wakeup_cond,
                                        &mgr->wakeup_lock, &ts);
        }
        if (!mgr->cleanup_running) {
            break;
        }
        pthread_mutex_unlock(&mgr->wakeup_lock);

        cleanup_all_shards(mgr);

        pthread_mutex_lock(&mgr->wakeup_lock);
    }
    pthread_mutex_unlock(&mgr->wakeup_lock);
    return NULL;
}

zdb_engine *zdb_engine_open(const char *path)
{
    if (!path || !*path) {
        return NULL;
    }

    zdb_engine *mgr = calloc(1, sizeof(*mgr));
    if (!mgr) {
        return NULL;
    }
    mgr->path = strdup(path);
    if (!mgr->path) {
        free(mgr);
        return NULL;
    }
    pthread_mutex_init(&mgr->lock, NULL);
    pthread_mutex_init(&mgr->wakeup_lock, NULL);
    pthread_cond_init(&mgr->wakeup_cond, NULL);

    if (mkdir(path, 0755) != 0 && errno != EEXIST) {
        fprintf(stderr, "zdb: cannot create data directory '%s': %s\n", path,
                strerror(errno));
        zdb_engine_close(mgr);
        return NULL;
    }

    DIR *dir = opendir(path);
    if (!dir) {
        fprintf(stderr, "zdb: cannot open data directory '%s': %s\n", path,
                strerror(errno));
        zdb_engine_close(mgr);
        return NULL;
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL) {
        size_t nlen = strlen(entry->d_name);
        if (nlen != 32 + sizeof(".sqlite") - 1 ||
            strcmp(entry->d_name + 32, ".sqlite") != 0) {
            continue;
        }
        char key[33];
        memcpy(key, entry->d_name, 32);
        key[32] = '\0';

        size_t plen = strlen(path) + 1 + nlen + 1;
        char *full = malloc(plen);
        if (!full) {
            continue;
        }
        snprintf(full, plen, "%s/%s", path, entry->d_name);
        zdb_shard *sh = zdb_shard_open(full, key);
        free(full);
        if (!sh) {
            fprintf(stderr,
                    "zdb: warning: failed to open existing shard '%s'\n",
                    entry->d_name);
            continue;
        }
        if (!insert_shard(mgr, sh)) {
            closedir(dir);
            zdb_engine_close(mgr);
            return NULL;
        }
    }
    closedir(dir);

    mgr->cleanup_running = true;
    if (pthread_create(&mgr->cleanup_thread, NULL, cleanup_thread_main,
                       mgr) != 0) {
        fprintf(stderr, "zdb: failed to start cleanup thread\n");
        mgr->cleanup_running = false;
        zdb_engine_close(mgr);
        return NULL;
    }

    return mgr;
}

void zdb_engine_close(zdb_engine *mgr)
{
    if (!mgr) {
        return;
    }
    if (mgr->cleanup_running) {
        pthread_mutex_lock(&mgr->wakeup_lock);
        mgr->cleanup_running = false;
        pthread_cond_broadcast(&mgr->wakeup_cond);
        pthread_mutex_unlock(&mgr->wakeup_lock);
        pthread_join(mgr->cleanup_thread, NULL);
    }
    close_all(mgr);
    pthread_mutex_destroy(&mgr->lock);
    pthread_mutex_destroy(&mgr->wakeup_lock);
    pthread_cond_destroy(&mgr->wakeup_cond);
    free(mgr->path);
    free(mgr);
}

const char *zdb_engine_path(zdb_engine *mgr)
{
    return mgr ? mgr->path : NULL;
}

bool zdb_put(zdb_engine *mgr, const char *partition, const char *keyspace,
             const char *id, const char *json_value, long long ttl_seconds,
             const char **filters, size_t nfilters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    return zdb_shard_put(sh, id, json_value, ttl_seconds, filters, nfilters);
}

cJSON *zdb_get(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char *id)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    return zdb_shard_get(sh, id);
}

bool zdb_delete(zdb_engine *mgr, const char *partition, const char *keyspace,
                const char *id)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    return zdb_shard_delete(sh, id);
}

char **zdb_ids(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char **filters, size_t nfilters, size_t *count_out)
{
    *count_out = 0;
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    return zdb_shard_ids(sh, filters, nfilters, count_out);
}

cJSON *zdb_all(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char **filters, size_t nfilters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    return zdb_shard_all(sh, filters, nfilters);
}

cJSON *zdb_query(zdb_engine *mgr, const char *partition, const char *keyspace,
                 const char **filters, size_t nfilters, const char **fields,
                 size_t nfields)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    return zdb_shard_query(sh, filters, nfilters, fields, nfields);
}

bool zdb_force_cleanup(zdb_engine *mgr, const char *partition,
                       const char *keyspace)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    return zdb_shard_cleanup(sh);
}

bool zdb_replica_put(zdb_engine *mgr, const char *partition,
                     const char *keyspace, const char *id,
                     const char *json_value, long long ttl_absolute,
                     long long timestamp, const char **filters,
                     size_t nfilters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    return zdb_shard_replica_put(sh, id, json_value, ttl_absolute,
                                 timestamp, filters, nfilters);
}

bool zdb_replica_delete(zdb_engine *mgr, const char *partition,
                        const char *keyspace, const char *id,
                        long long timestamp)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    return zdb_shard_replica_delete(sh, id, timestamp);
}

cJSON *zdb_get_ts(zdb_engine *mgr, const char *partition,
                  const char *keyspace, const char *id,
                  long long *timestamp_out)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    return zdb_shard_get_ts(sh, id, timestamp_out);
}

cJSON *zdb_all_ts(zdb_engine *mgr, const char *partition,
                  const char *keyspace, const char **filters,
                  size_t nfilters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    return zdb_shard_all_ts(sh, filters, nfilters);
}

cJSON *zdb_query_ts(zdb_engine *mgr, const char *partition,
                    const char *keyspace, const char **filters,
                    size_t nfilters, const char **fields, size_t nfields)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    return zdb_shard_query_ts(sh, filters, nfilters, fields, nfields);
}

void zdb_free_strings(char **strings)
{
    if (!strings) {
        return;
    }
    for (size_t i = 0; strings[i]; i++) {
        free(strings[i]);
    }
    free(strings);
}
