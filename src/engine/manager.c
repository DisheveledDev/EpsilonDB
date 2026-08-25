#include "shard_internal.h"

#include <dirent.h>
#include "../zesty_log.h"
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

    zdb_shard_settings_fn settings_fn;
    void *settings_ctx;

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

void zdb_engine_set_settings_provider(zdb_engine *mgr,
                                      zdb_shard_settings_fn fn, void *ctx)
{
    if (!mgr) {
        return;
    }
    pthread_mutex_lock(&mgr->lock);
    mgr->settings_fn = fn;
    mgr->settings_ctx = ctx;
    pthread_mutex_unlock(&mgr->lock);
}

static void resolve_settings(zdb_engine *mgr, const char *partition,
                             zdb_shard_settings *out)
{
    zdb_shard_settings_default(out);
    zdb_shard_settings_fn fn;
    void *ctx;
    pthread_mutex_lock(&mgr->lock);
    fn = mgr->settings_fn;
    ctx = mgr->settings_ctx;
    pthread_mutex_unlock(&mgr->lock);
    if (fn) {
        fn(ctx, partition, out);
    }
}

static bool settings_differ(const zdb_shard_settings *a,
                            const zdb_shard_settings *b)
{
    return a->cache_size != b->cache_size ||
           strcmp(a->journal_mode, b->journal_mode) != 0 ||
           a->vacuum_seconds != b->vacuum_seconds ||
           a->reindex_seconds != b->reindex_seconds;
}

static zdb_shard *shard_for(zdb_engine *mgr, const char *partition,
                            const char *keyspace)
{
    char key[33];
    shard_key(partition, keyspace, key);
    if (!migrate_legacy_shard(mgr, partition, keyspace, key)) {
        return NULL;
    }

    zdb_shard_settings desired;
    resolve_settings(mgr, partition, &desired);

    pthread_mutex_lock(&mgr->lock);
    zdb_shard *sh = find_locked(mgr, key);
    if (sh) {
        sh->refs++;
    }
    pthread_mutex_unlock(&mgr->lock);

    if (!sh) {
        size_t len = strlen(mgr->path) + 1 + 32 + sizeof(".sqlite");
        char *path = malloc(len);
        if (!path) {
            return NULL;
        }
        snprintf(path, len, "%s/%s.sqlite", mgr->path, key);
        zdb_shard *opened =
            zdb_shard_open(path, key, partition, keyspace, &desired);
        free(path);
        if (!opened) {
            return NULL;
        }
        return insert_shard(mgr, opened, true);
    }

    /* Cached handle: adopt the (now known) partition name and reopen when
     * the settings have changed since the handle was opened (e.g. a startup
     * scan opened it with defaults before the config layer was attached). */
    bool reopen = false;
    pthread_mutex_lock(&sh->lock);
    if (sh->partition[0] == '\0') {
        snprintf(sh->partition, sizeof(sh->partition), "%s", partition);
        snprintf(sh->keyspace, sizeof(sh->keyspace), "%s", keyspace);
    }
    if (settings_differ(&sh->settings, &desired)) {
        reopen = true;
    }
    pthread_mutex_unlock(&sh->lock);
    if (reopen) {
        zdb_shard_reopen(sh, &desired);
    }
    return sh;
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
        zdb_log("ERROR", "cannot create data directory '%s': %s", path,
                strerror(errno));
        zdb_engine_close(mgr);
        return NULL;
    }

    DIR *dir = opendir(path);
    if (!dir) {
        zdb_log("ERROR", "cannot open data directory '%s': %s", path,
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
        zdb_shard *sh = zdb_shard_open(full, key, "", "", NULL);
        free(full);
        if (!sh) {
            zdb_log("WARN",
                    "failed to open existing shard '%s'",
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
        zdb_log("ERROR", "failed to start cleanup thread");
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
    snprintf(side, sizeof(side), "%s-shm", path);
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
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s.sqlite", mgr->path, key);
    zdb_shard_settings settings;
    resolve_settings(mgr, partition, &settings);

    /* Close the old connection first (checkpointing its WAL) and drop any
     * stale sidecars so a WAL left behind by the replaced file cannot be
     * replayed against the new database. Hold sh->lock across the swap so a
     * concurrent accessor never sees a half-replaced handle. */
    zdb_shard *sh = NULL;
    pthread_mutex_lock(&mgr->lock);
    sh = find_locked(mgr, key);
    if (sh) {
        pthread_mutex_lock(&sh->lock);
    }
    pthread_mutex_unlock(&mgr->lock);

    if (sh) {
        for (int i = 0; i < sh->cache_count; i++) {
            sqlite3_finalize(sh->cache[i].stmt);
        }
        sh->cache_count = 0;
        if (sh->db) {
            sqlite3_close_v2(sh->db);
            sh->db = NULL;
        }
    }

    char side[1060];
    snprintf(side, sizeof(side), "%s-wal", path);
    unlink(side);
    snprintf(side, sizeof(side), "%s-shm", path);
    unlink(side);

    zdb_shard *fresh = zdb_shard_open(path, key, partition, keyspace,
                                      &settings);
    bool ok = false;
    if (fresh) {
        pthread_mutex_lock(&fresh->lock);
        int rc = sqlite3_exec(fresh->db, "PRAGMA integrity_check;", NULL,
                              NULL, NULL);
        pthread_mutex_unlock(&fresh->lock);
        ok = rc == SQLITE_OK;
    }

    if (sh) {
        if (ok && fresh) {
            sh->db = fresh->db;
            sh->expired_since_vacuum = 0;
            sh->last_vacuum_ts = (long long)time(NULL);
            sh->last_reindex_ts = (long long)time(NULL);
            free(fresh->path);
            pthread_mutex_destroy(&fresh->lock);
            free(fresh);
            fresh = NULL;
        }
        /* on failure sh->db stays NULL: the file was replaced and there is
         * nothing to fall back to */
        pthread_mutex_unlock(&sh->lock);
    }
    if (fresh) {
        zdb_shard_free(fresh);
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

int zdb_engine_reload_partition(zdb_engine *mgr, const char *partition)
{
    if (!mgr || !partition || !*partition) {
        return 0;
    }
    zdb_shard_settings settings;
    resolve_settings(mgr, partition, &settings);

    int reloaded = 0;
    pthread_mutex_lock(&mgr->lock);
    for (size_t i = 0; i < ZDB_SHARD_BUCKETS; i++) {
        for (struct shard_link *l = mgr->buckets[i]; l; l = l->next) {
            zdb_shard *sh = l->shard;
            pthread_mutex_lock(&sh->lock);
            bool match = strcmp(sh->partition, partition) == 0;
            pthread_mutex_unlock(&sh->lock);
            if (!match) {
                continue;
            }
            if (zdb_shard_reopen(sh, &settings)) {
                reloaded++;
            }
        }
    }
    pthread_mutex_unlock(&mgr->lock);
    return reloaded;
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
             const char *id, const char *json_value, long long ttl_seconds)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = zdb_shard_put(sh, id, json_value, ttl_seconds);
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
               const cJSON *filters, size_t *count_out)
{
    *count_out = 0;
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    char **result = zdb_shard_ids(sh, filters, count_out);
    shard_release(mgr, sh);
    return result;
}

cJSON *zdb_all(zdb_engine *mgr, const char *partition, const char *keyspace,
               const cJSON *filters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_all(sh, filters);
    shard_release(mgr, sh);
    return result;
}

cJSON *zdb_query(zdb_engine *mgr, const char *partition, const char *keyspace,
                 const cJSON *filters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_query(sh, filters);
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
                     long long timestamp)
{
    return zdb_replica_put_origin(mgr, partition, keyspace, id, json_value,
                                  ttl_absolute, timestamp, "");
}

bool zdb_replica_put_origin(zdb_engine *mgr, const char *partition,
                            const char *keyspace, const char *id,
                            const char *json_value, long long ttl_absolute,
                            long long timestamp, const char *origin)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = zdb_shard_replica_put(sh, id, json_value, ttl_absolute,
                                    timestamp, origin);
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
                  const char *keyspace, const cJSON *filters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_all_ts(sh, filters);
    shard_release(mgr, sh);
    return result;
}

cJSON *zdb_query_ts(zdb_engine *mgr, const char *partition,
                    const char *keyspace, const cJSON *filters)
{
    zdb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = zdb_shard_query_ts(sh, filters);
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
