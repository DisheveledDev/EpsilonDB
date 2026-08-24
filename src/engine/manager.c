#include "shard_internal.h"

#include <dirent.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

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
    char partition_hash[33];
    char keyspace_hash[33];
    char framed[66];
    zdb_md5_hex(partition, strlen(partition), partition_hash);
    zdb_md5_hex(keyspace, strlen(keyspace), keyspace_hash);
    snprintf(framed, sizeof(framed), "%s:%s", partition_hash,
             keyspace_hash);
    zdb_md5_hex(framed, strlen(framed), out);
}

static void legacy_shard_key(const char *partition, const char *keyspace,
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
static zdb_shard *insert_shard(zdb_engine *mgr, zdb_shard *sh,
                               bool acquire)
{
    pthread_mutex_lock(&mgr->lock);
    zdb_shard *existing = find_locked(mgr, sh->key);
    if (existing) {
        if (acquire) {
            existing->refs++;
        }
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
    sh->refs = acquire ? 1 : 0;
    link->shard = sh;
    link->next = mgr->buckets[bucket_for(sh->key)];
    mgr->buckets[bucket_for(sh->key)] = link;
    pthread_mutex_unlock(&mgr->lock);
    return sh;
}

static bool migrate_legacy_shard(zdb_engine *mgr, const char *partition,
                                 const char *keyspace, const char *new_key)
{
    char legacy_key[33];
    legacy_shard_key(partition, keyspace, legacy_key);
    if (strcmp(legacy_key, new_key) == 0) {
        return true;
    }
    char new_path[1024];
    char legacy_path[1024];
    if (snprintf(new_path, sizeof(new_path), "%s/%s.sqlite", mgr->path,
                 new_key) >= (int)sizeof(new_path) ||
        snprintf(legacy_path, sizeof(legacy_path), "%s/%s.sqlite", mgr->path,
                 legacy_key) >= (int)sizeof(legacy_path)) {
        return false;
    }
    struct stat file_stat;
    if (stat(new_path, &file_stat) == 0 ||
        (stat(legacy_path, &file_stat) != 0 && errno == ENOENT)) {
        return true;
    }

    pthread_mutex_lock(&mgr->lock);
    struct shard_link **link = &mgr->buckets[bucket_for(legacy_key)];
    while (*link && strcmp((*link)->shard->key, legacy_key) != 0) {
        link = &(*link)->next;
    }
    zdb_shard *legacy = NULL;
    if (*link) {
        if ((*link)->shard->refs > 0) {
            pthread_mutex_unlock(&mgr->lock);
            return false;
        }
        struct shard_link *removed = *link;
        legacy = removed->shard;
        *link = removed->next;
        free(removed);
    }
    if (legacy) {
        zdb_shard_free(legacy);
    }
    bool migrated = rename(legacy_path, new_path) == 0 || errno == ENOENT;
    pthread_mutex_unlock(&mgr->lock);
    return migrated;
}

static zdb_shard *shard_for(zdb_engine *mgr, const char *partition,
                            const char *keyspace)
{
    char key[33];
    shard_key(partition, keyspace, key);
    if (!migrate_legacy_shard(mgr, partition, keyspace, key)) {
        return NULL;
    }

    pthread_mutex_lock(&mgr->lock);
    zdb_shard *sh = find_locked(mgr, key);
    if (sh) {
        sh->refs++;
    }
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
    return insert_shard(mgr, opened, true);
}

static void shard_release(zdb_engine *mgr, zdb_shard *sh)
{
    bool destroy = false;
    pthread_mutex_lock(&mgr->lock);
    if (sh->refs > 0) {
        sh->refs--;
    }
    destroy = sh->retired && sh->refs == 0;
    pthread_mutex_unlock(&mgr->lock);
    if (destroy) {
        zdb_shard_free(sh);
    }
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
        if (!insert_shard(mgr, sh, false)) {
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

size_t zdb_engine_shard_keys(zdb_engine *mgr, char (*keys)[33], size_t cap)
{
    if (!mgr || !keys || cap == 0) {
        return 0;
    }
    size_t n = 0;
    DIR *dir = opendir(mgr->path);
    if (!dir) {
        return 0;
    }
    struct dirent *entry;
    while ((entry = readdir(dir)) != NULL && n < cap) {
        size_t nlen = strlen(entry->d_name);
        if (nlen != 32 + sizeof(".sqlite") - 1 ||
            strcmp(entry->d_name + 32, ".sqlite") != 0) {
            continue;
        }
        memcpy(keys[n], entry->d_name, 32);
        keys[n][32] = '\0';
        n++;
    }
    closedir(dir);
    return n;
}

bool zdb_shard_gc(zdb_engine *mgr, const char key[33])
{
    if (!mgr || !key || strlen(key) != 32) {
        return false;
    }

    /* drop the cached handle (if any) before removing the file so a
     * later reopen starts from a clean, empty shard */
    zdb_shard *sh = NULL;
    pthread_mutex_lock(&mgr->lock);
    struct shard_link **pp = &mgr->buckets[bucket_for(key)];
    while (*pp && strcmp((*pp)->shard->key, key) != 0) {
        pp = &(*pp)->next;
    }
    if (*pp) {
        struct shard_link *victim = *pp;
        sh = victim->shard;
        *pp = victim->next;
        free(victim);
        sh->retired = true;
        sh->refs++;
    }
    pthread_mutex_unlock(&mgr->lock);
    if (sh) {
        pthread_mutex_lock(&sh->lock);
    }

    /* remove the file and any SQLite sidecars */
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s.sqlite", mgr->path, key);
    int rc = unlink(path);
    char side[1060];
    snprintf(side, sizeof(side), "%s-wal", path);
    unlink(side);
    snprintf(side, sizeof(side), "%s-journal", path);
    unlink(side);
    if (rc != 0 && errno == ENOENT) {
        rc = 0;   /* already gone */
    }
    if (sh) {
        pthread_mutex_unlock(&sh->lock);
        shard_release(mgr, sh);
    }
    return rc == 0;
}

bool zdb_shard_path(zdb_engine *mgr, const char *partition,
                    const char *keyspace, char *path_out, size_t cap,
                    char key_out[33])
{
    if (!mgr || !partition || !keyspace || !path_out || cap == 0) {
        return false;
    }
    char key[33];
    shard_key(partition, keyspace, key);
    if (snprintf(path_out, cap, "%s/%s.sqlite", mgr->path, key) >=
        (int)cap) {
        return false;
    }
    if (key_out) {
        memcpy(key_out, key, sizeof(key));
    }
    return true;
}

/* True when the sqlite database behind an open shard passes
 * "PRAGMA integrity_check". Caller does not hold sh->lock.
 * (Currently unused: invalidate runs integrity_check via sqlite3_exec
 * on the replaced handle; kept for the stage 6c delta-catch-up work.) */
#if 0
static bool shard_integrity_ok(zdb_shard *sh)
{
    pthread_mutex_lock(&sh->lock);
    sqlite3_stmt *stmt = NULL;
    bool ok = false;
    if (sqlite3_prepare_v2(sh->db, "PRAGMA integrity_check", -1, &stmt,
                           NULL) == SQLITE_OK &&
        stmt) {
        ok = sqlite3_step(stmt) == SQLITE_ROW;
        const char *result =
            ok ? (const char *)sqlite3_column_text(stmt, 0) : NULL;
        ok = result && strcmp(result, "ok") == 0;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&sh->lock);
    return ok;
}
#endif

bool zdb_shard_invalidate(zdb_engine *mgr, const char *partition,
                          const char *keyspace)
{
    if (!mgr) {
        return false;
    }
    char key[33];
    shard_key(partition, keyspace, key);

    /* open the replacement handle first, outside the manager lock:
     * SQLite may block on busy timeouts */
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s.sqlite", mgr->path, key);
    zdb_shard *fresh = zdb_shard_open(path, key);

    bool ok = false;
    if (fresh) {
        pthread_mutex_lock(&fresh->lock);
        int rc = sqlite3_exec(fresh->db, "PRAGMA integrity_check;", NULL,
                              NULL, NULL);
        pthread_mutex_unlock(&fresh->lock);
        if (rc == SQLITE_OK) {
            ok = true;
        } else {
            zdb_shard_free(fresh);
            fresh = NULL;
        }
    }

    /* swap the new connection in under mgr->lock so a concurrent
     * shard_for() either misses (reopens from disk later) or sees the
     * replaced handle */
    sqlite3 *old_db = NULL;
    pthread_mutex_lock(&mgr->lock);
    zdb_shard *sh = find_locked(mgr, key);
    if (sh && fresh) {
        pthread_mutex_lock(&sh->lock);
        for (int i = 0; i < sh->cache_count; i++) {
            sqlite3_finalize(sh->cache[i].stmt);
        }
        old_db = sh->db;
        sh->db = fresh->db;
        sh->cache_count = 0;
        memset(sh->cache, 0, sizeof(sh->cache));
        sh->expired_since_vacuum = 0;
        sh->vacuum_pending = false;
        pthread_mutex_unlock(&sh->lock);
        /* adopt the live connection; free the temporary shell */
        free(fresh->path);
        pthread_mutex_destroy(&fresh->lock);
        free(fresh);
        fresh = NULL;
    }
    pthread_mutex_unlock(&mgr->lock);

    if (fresh) {
        /* swap did not happen (no cached handle or integrity failure):
         * discard the replacement */
        zdb_shard_free(fresh);
    }
    if (old_db) {
        /* close_v2 is safe with outstanding prepared statements: the
         * connection is destroyed once the last statement releases */
        sqlite3_close_v2(old_db);
    }
    return ok;
}

bool zdb_shard_is_open(zdb_engine *mgr, const char *partition,
                       const char *keyspace)
{
    if (!mgr) {
        return false;
    }
    char key[33];
    shard_key(partition, keyspace, key);
    pthread_mutex_lock(&mgr->lock);
    zdb_shard *sh = find_locked(mgr, key);
    pthread_mutex_unlock(&mgr->lock);
    return sh != NULL;
}

bool zdb_shard_validate(zdb_engine *mgr, const char *partition,
                        const char *keyspace)
{
    if (!mgr || !partition || !keyspace) {
        return false;
    }
    char path[1024];
    char key[33];
    if (!zdb_shard_path(mgr, partition, keyspace, path, sizeof(path), key)) {
        return false;
    }
    struct stat file_stat;
    if (stat(path, &file_stat) != 0 || file_stat.st_size <= 0) {
        return false;
    }
    sqlite3 *db = NULL;
    if (sqlite3_open_v2(path, &db,
                        SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX,
                        NULL) != SQLITE_OK) {
        if (db) {
            sqlite3_close(db);
        }
        return false;
    }
    sqlite3_stmt *stmt = NULL;
    bool valid = sqlite3_prepare_v2(db, "PRAGMA integrity_check", -1, &stmt,
                                    NULL) == SQLITE_OK &&
                 stmt && sqlite3_step(stmt) == SQLITE_ROW;
    const char *result = valid ? (const char *)sqlite3_column_text(stmt, 0)
                               : NULL;
    valid = result && strcmp(result, "ok") == 0;
    sqlite3_finalize(stmt);
    sqlite3_close(db);
    return valid;
}


bool zdb_put(zdb_engine *mgr, const char *partition, const char *keyspace,
             const char *id, const char *json_value, long long ttl_seconds,
             const char **filters, size_t nfilters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = zdb_shard_put(sh, id, json_value, ttl_seconds, filters,
                            nfilters);
    shard_release(mgr, sh);
    return ok;
}

cJSON *zdb_get(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char *id)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_get(sh, id);
    shard_release(mgr, sh);
    return result;
}

bool zdb_delete(zdb_engine *mgr, const char *partition, const char *keyspace,
                const char *id)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = zdb_shard_delete(sh, id);
    shard_release(mgr, sh);
    return ok;
}

char **zdb_ids(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char **filters, size_t nfilters, size_t *count_out)
{
    *count_out = 0;
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    char **result = zdb_shard_ids(sh, filters, nfilters, count_out);
    shard_release(mgr, sh);
    return result;
}

cJSON *zdb_all(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char **filters, size_t nfilters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_all(sh, filters, nfilters);
    shard_release(mgr, sh);
    return result;
}

cJSON *zdb_query(zdb_engine *mgr, const char *partition, const char *keyspace,
                 const char **filters, size_t nfilters, const char **fields,
                 size_t nfields)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_query(sh, filters, nfilters, fields, nfields);
    shard_release(mgr, sh);
    return result;
}

bool zdb_force_cleanup(zdb_engine *mgr, const char *partition,
                       const char *keyspace)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = zdb_shard_cleanup(sh);
    shard_release(mgr, sh);
    return ok;
}

bool zdb_replica_put(zdb_engine *mgr, const char *partition,
                     const char *keyspace, const char *id,
                     const char *json_value, long long ttl_absolute,
                     long long timestamp, const char **filters,
                     size_t nfilters)
{
    return zdb_replica_put_origin(mgr, partition, keyspace, id, json_value,
                                  ttl_absolute, timestamp, "", filters,
                                  nfilters);
}

bool zdb_replica_put_origin(zdb_engine *mgr, const char *partition,
                            const char *keyspace, const char *id,
                            const char *json_value, long long ttl_absolute,
                            long long timestamp, const char *origin,
                            const char **filters, size_t nfilters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = zdb_shard_replica_put(sh, id, json_value, ttl_absolute,
                                    timestamp, origin, filters, nfilters);
    shard_release(mgr, sh);
    return ok;
}


bool zdb_replica_delete(zdb_engine *mgr, const char *partition,
                        const char *keyspace, const char *id,
                        long long timestamp)
{
    return zdb_replica_delete_origin(mgr, partition, keyspace, id, timestamp,
                                     "");
}

bool zdb_replica_delete_origin(zdb_engine *mgr, const char *partition,
                               const char *keyspace, const char *id,
                               long long timestamp, const char *origin)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = zdb_shard_replica_delete(sh, id, timestamp, origin);
    shard_release(mgr, sh);
    return ok;
}


cJSON *zdb_get_ts(zdb_engine *mgr, const char *partition,
                  const char *keyspace, const char *id,
                  long long *timestamp_out)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_get_ts(sh, id, timestamp_out);
    shard_release(mgr, sh);
    return result;
}

cJSON *zdb_all_ts(zdb_engine *mgr, const char *partition,
                  const char *keyspace, const char **filters,
                  size_t nfilters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_all_ts(sh, filters, nfilters);
    shard_release(mgr, sh);
    return result;
}

cJSON *zdb_query_ts(zdb_engine *mgr, const char *partition,
                    const char *keyspace, const char **filters,
                    size_t nfilters, const char **fields, size_t nfields)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_query_ts(sh, filters, nfilters, fields,
                                       nfields);
    shard_release(mgr, sh);
    return result;
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
