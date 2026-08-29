#include "shard_internal.h"

#include <dirent.h>
#include "../epsilon_log.h"
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "md5.h"

#define EDB_SHARD_BUCKETS 512

struct shard_link {
    edb_shard *shard;
    struct shard_link *next;
};

struct edb_shard_manager {
    char *path;
    pthread_mutex_t lock;              /* guards buckets + bucket counts */
    struct shard_link *buckets[EDB_SHARD_BUCKETS];

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
    return (size_t)((hi << 4) | lo) & (EDB_SHARD_BUCKETS - 1);
}

static void shard_key(const char *partition, const char *keyspace,
                      char out[33])
{
    char partition_hash[33];
    char keyspace_hash[33];
    char framed[66];
    edb_md5_hex(partition, strlen(partition), partition_hash);
    edb_md5_hex(keyspace, strlen(keyspace), keyspace_hash);
    snprintf(framed, sizeof(framed), "%s:%s", partition_hash,
             keyspace_hash);
    edb_md5_hex(framed, strlen(framed), out);
}

/* Look up a shard by key without opening it. Caller holds mgr->lock. */
static edb_shard *find_locked(edb_engine *mgr, const char *key)
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
static edb_shard *insert_shard(edb_engine *mgr, edb_shard *sh,
                               bool acquire)
{
    pthread_mutex_lock(&mgr->lock);
    edb_shard *existing = find_locked(mgr, sh->key);
    if (existing) {
        if (acquire) {
            existing->refs++;
        }
        pthread_mutex_unlock(&mgr->lock);
        edb_shard_free(sh);
        return existing;
    }
    struct shard_link *link = malloc(sizeof(*link));
    if (!link) {
        pthread_mutex_unlock(&mgr->lock);
        edb_shard_free(sh);
        return NULL;
    }
    sh->refs = acquire ? 1 : 0;
    link->shard = sh;
    link->next = mgr->buckets[bucket_for(sh->key)];
    mgr->buckets[bucket_for(sh->key)] = link;
    pthread_mutex_unlock(&mgr->lock);
    return sh;
}

static edb_shard *shard_for(edb_engine *mgr, const char *partition,
                            const char *keyspace)
{
    char key[33];
    shard_key(partition, keyspace, key);

    pthread_mutex_lock(&mgr->lock);
    edb_shard *sh = find_locked(mgr, key);
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
        edb_shard *opened = edb_shard_open(path, key, partition, keyspace);
        free(path);
        if (!opened) {
            return NULL;
        }
        return insert_shard(mgr, opened, true);
    }

    /* Cached handle: adopt the (now known) partition name. */
    pthread_mutex_lock(&sh->lock);
    if (sh->partition[0] == '\0') {
        snprintf(sh->partition, sizeof(sh->partition), "%s", partition);
        snprintf(sh->keyspace, sizeof(sh->keyspace), "%s", keyspace);
    }
    pthread_mutex_unlock(&sh->lock);
    return sh;
}

static void shard_release(edb_engine *mgr, edb_shard *sh)
{
    bool destroy = false;
    pthread_mutex_lock(&mgr->lock);
    if (sh->refs > 0) {
        sh->refs--;
    }
    destroy = sh->retired && sh->refs == 0;
    pthread_mutex_unlock(&mgr->lock);
    if (destroy) {
        edb_shard_free(sh);
    }
}

static void shard_release(edb_engine *mgr, edb_shard *sh);

static void close_all(edb_engine *mgr)
{
    pthread_mutex_lock(&mgr->lock);
    for (size_t i = 0; i < EDB_SHARD_BUCKETS; i++) {
        struct shard_link *l = mgr->buckets[i];
        while (l) {
            struct shard_link *next = l->next;
            edb_shard_free(l->shard);
            free(l);
            l = next;
        }
        mgr->buckets[i] = NULL;
    }
    pthread_mutex_unlock(&mgr->lock);
}

static void cleanup_all_shards(edb_engine *mgr)
{
    size_t count = 0;
    pthread_mutex_lock(&mgr->lock);
    for (size_t i = 0; i < EDB_SHARD_BUCKETS; i++) {
        for (struct shard_link *l = mgr->buckets[i]; l; l = l->next) {
            count++;
        }
    }
    edb_shard **shards = count ? malloc(count * sizeof(*shards)) : NULL;
    if (count && !shards) {
        pthread_mutex_unlock(&mgr->lock);
        return;
    }
    size_t used = 0;
    for (size_t i = 0; i < EDB_SHARD_BUCKETS; i++) {
        for (struct shard_link *l = mgr->buckets[i]; l; l = l->next) {
            l->shard->refs++;
            shards[used++] = l->shard;
        }
    }
    pthread_mutex_unlock(&mgr->lock);

    for (size_t i = 0; i < used; i++) {
        edb_shard_cleanup(shards[i]);
        shard_release(mgr, shards[i]);
    }
    free(shards);
}

static void *cleanup_thread_main(void *arg)
{
    edb_engine *mgr = arg;

    pthread_mutex_lock(&mgr->wakeup_lock);
    while (mgr->cleanup_running) {
        struct timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts);
        ts.tv_sec += EDB_CLEANUP_INTERVAL_SECONDS;
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

edb_engine *edb_engine_open(const char *path)
{
    if (!path || !*path) {
        return NULL;
    }

    edb_engine *mgr = calloc(1, sizeof(*mgr));
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
        edb_log("ERROR", "cannot create data directory '%s': %s", path,
                strerror(errno));
        edb_engine_close(mgr);
        return NULL;
    }

    DIR *dir = opendir(path);
    if (!dir) {
        edb_log("ERROR", "cannot open data directory '%s': %s", path,
                strerror(errno));
        edb_engine_close(mgr);
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
        edb_shard *sh = edb_shard_open(full, key, "", "");
        free(full);
        if (!sh) {
            edb_log("WARN",
                    "failed to open existing shard '%s'",
                    entry->d_name);
            continue;
        }
        if (!insert_shard(mgr, sh, false)) {
            closedir(dir);
            edb_engine_close(mgr);
            return NULL;
        }
    }
    closedir(dir);

    mgr->cleanup_running = true;
    if (pthread_create(&mgr->cleanup_thread, NULL, cleanup_thread_main,
                       mgr) != 0) {
        edb_log("ERROR", "failed to start cleanup thread");
        mgr->cleanup_running = false;
        edb_engine_close(mgr);
        return NULL;
    }

    return mgr;
}

void edb_engine_close(edb_engine *mgr)
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

const char *edb_engine_path(edb_engine *mgr)
{
    return mgr ? mgr->path : NULL;
}

size_t edb_engine_shard_keys(edb_engine *mgr, char (*keys)[33], size_t cap)
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

long long edb_engine_shard_size(edb_engine *mgr, const char *partition,
                                const char *keyspace)
{
    if (!mgr || !partition || !keyspace) {
        return 0;
    }
    char path[1024];
    if (!edb_shard_path(mgr, partition, keyspace, path, sizeof(path), NULL)) {
        return 0;
    }
    struct stat st;
    long long total = 0;
    if (stat(path, &st) == 0 && st.st_size > 0) {
        total += (long long)st.st_size;
    }
    /* WAL-mode data may live in the -wal sidecar; count it too */
    char side[1060];
    snprintf(side, sizeof(side), "%s-wal", path);
    if (stat(side, &st) == 0 && st.st_size > 0) {
        total += (long long)st.st_size;
    }
    return total;
}

bool edb_shard_gc(edb_engine *mgr, const char key[33])
{
    if (!mgr || !key || strlen(key) != 32) {
        return false;
    }

    /* drop the cached handle (if any) before removing the file so a
     * later reopen starts from a clean, empty shard */
    edb_shard *sh = NULL;
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

bool edb_shard_path(edb_engine *mgr, const char *partition,
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
 * on the replaced handle; kept for the delta-catch-up work.) */
#if 0
static bool shard_integrity_ok(edb_shard *sh)
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

bool edb_shard_invalidate(edb_engine *mgr, const char *partition,
                          const char *keyspace)
{
    if (!mgr || !partition || !keyspace) {
        return false;
    }
    char key[33];
    shard_key(partition, keyspace, key);
    char path[1024];
    if (snprintf(path, sizeof(path), "%s/%s.sqlite", mgr->path, key) >=
        (int)sizeof(path)) {
        return false;
    }
    /* wipe all non-system shards (restore path) */
    edb_shard *sh = NULL;
    pthread_mutex_lock(&mgr->lock);
    sh = find_locked(mgr, key);
    if (sh) {
        pthread_mutex_lock(&sh->lock);
    }
    pthread_mutex_unlock(&mgr->lock);

    char side[1060];
    snprintf(side, sizeof(side), "%s-wal", path);
    unlink(side);
    snprintf(side, sizeof(side), "%s-shm", path);
    unlink(side);

    edb_shard *fresh = edb_shard_open(path, key, partition, keyspace);
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
            for (int i = 0; i < sh->cache_count; i++) {
                sqlite3_finalize(sh->cache[i].stmt);
            }
            sh->cache_count = 0;
            sqlite3 *old = sh->db;
            sh->db = fresh->db;
            sh->partition[0] = '\0';
            snprintf(sh->partition, sizeof(sh->partition), "%s", partition);
            sh->keyspace[0] = '\0';
            snprintf(sh->keyspace, sizeof(sh->keyspace), "%s", keyspace);
            sh->expired_since_vacuum = 0;
            fresh->db = NULL;
            if (old) {
                sqlite3_close_v2(old);
            }
        }
        pthread_mutex_unlock(&sh->lock);
    }
    if (fresh) {
        edb_shard_free(fresh);
    }
    return ok;
}

bool edb_shard_is_open(edb_engine *mgr, const char *partition,
                       const char *keyspace)
{
    if (!mgr) {
        return false;
    }
    char key[33];
    shard_key(partition, keyspace, key);
    pthread_mutex_lock(&mgr->lock);
    edb_shard *sh = find_locked(mgr, key);
    pthread_mutex_unlock(&mgr->lock);
    return sh != NULL;
}

/* Runs `fn` on every currently open shard belonging to `partition`.
 * Shards are ref-counted so they stay alive while maintained. */
static int maintain_partition(edb_engine *mgr, const char *partition,
                              bool (*fn)(edb_shard *))
{
    if (!mgr || !partition || !*partition || !fn) {
        return -1;
    }

    int processed = 0;
    pthread_mutex_lock(&mgr->lock);
    size_t count = 0;
    for (size_t i = 0; i < EDB_SHARD_BUCKETS; i++) {
        for (struct shard_link *l = mgr->buckets[i]; l; l = l->next) {
            pthread_mutex_lock(&l->shard->lock);
            bool match = strcmp(l->shard->partition, partition) == 0 &&
                         l->shard->partition[0] != '\0';
            pthread_mutex_unlock(&l->shard->lock);
            if (match) {
                count++;
            }
        }
    }
    edb_shard **shards = count ? malloc(count * sizeof(*shards)) : NULL;
    if (count && !shards) {
        pthread_mutex_unlock(&mgr->lock);
        return -1;
    }
    size_t used = 0;
    for (size_t i = 0; i < EDB_SHARD_BUCKETS; i++) {
        for (struct shard_link *l = mgr->buckets[i]; l; l = l->next) {
            pthread_mutex_lock(&l->shard->lock);
            bool match = strcmp(l->shard->partition, partition) == 0 &&
                         l->shard->partition[0] != '\0';
            pthread_mutex_unlock(&l->shard->lock);
            if (match && used < count) {
                shards[used++] = l->shard;
                l->shard->refs++;
            }
        }
    }
    pthread_mutex_unlock(&mgr->lock);

    for (size_t i = 0; i < used; i++) {
        if (fn(shards[i])) {
            processed++;
        }
        shard_release(mgr, shards[i]);
    }
    free(shards);
    return processed;
}

int edb_engine_vacuum_partition(edb_engine *mgr, const char *partition)
{
    return maintain_partition(mgr, partition, edb_shard_vacuum);
}

int edb_engine_reindex_partition(edb_engine *mgr, const char *partition)
{
    return maintain_partition(mgr, partition, edb_shard_reindex);
}

bool edb_shard_validate(edb_engine *mgr, const char *partition,
                        const char *keyspace)
{
    if (!mgr || !partition || !keyspace) {
        return false;
    }
    char path[1024];
    char key[33];
    if (!edb_shard_path(mgr, partition, keyspace, path, sizeof(path), key)) {
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


bool edb_put(edb_engine *mgr, const char *partition, const char *keyspace,
             const char *id, const char *json_value, long long ttl_seconds)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = edb_shard_put(sh, id, json_value, ttl_seconds);
    shard_release(mgr, sh);
    return ok;
}

cJSON *edb_get(edb_engine *mgr, const char *partition, const char *keyspace,
               const char *id)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = edb_shard_get(sh, id);
    shard_release(mgr, sh);
    return result;
}

bool edb_delete(edb_engine *mgr, const char *partition, const char *keyspace,
                const char *id)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = edb_shard_delete(sh, id);
    shard_release(mgr, sh);
    return ok;
}

char **edb_ids(edb_engine *mgr, const char *partition, const char *keyspace,
               const cJSON *filters, size_t *count_out)
{
    *count_out = 0;
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    char **result = edb_shard_ids(sh, filters, count_out);
    shard_release(mgr, sh);
    return result;
}

cJSON *edb_all(edb_engine *mgr, const char *partition, const char *keyspace,
               const cJSON *filters)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = edb_shard_all(sh, filters);
    shard_release(mgr, sh);
    return result;
}

cJSON *edb_query(edb_engine *mgr, const char *partition, const char *keyspace,
                 const cJSON *filters)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = edb_shard_query(sh, filters);
    shard_release(mgr, sh);
    return result;
}

bool edb_force_cleanup(edb_engine *mgr, const char *partition,
                       const char *keyspace)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = edb_shard_cleanup(sh);
    shard_release(mgr, sh);
    return ok;
}

bool edb_replica_put(edb_engine *mgr, const char *partition,
                     const char *keyspace, const char *id,
                     const char *json_value, long long ttl_absolute,
                     long long timestamp)
{
    return edb_replica_put_origin(mgr, partition, keyspace, id, json_value,
                                  ttl_absolute, timestamp, "");
}

bool edb_replica_put_origin(edb_engine *mgr, const char *partition,
                            const char *keyspace, const char *id,
                            const char *json_value, long long ttl_absolute,
                            long long timestamp, const char *origin)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = edb_shard_replica_put(sh, id, json_value, ttl_absolute,
                                    timestamp, origin);
    shard_release(mgr, sh);
    return ok;
}


bool edb_replica_delete(edb_engine *mgr, const char *partition,
                        const char *keyspace, const char *id,
                        long long timestamp)
{
    return edb_replica_delete_origin(mgr, partition, keyspace, id, timestamp,
                                     "");
}

bool edb_replica_delete_origin(edb_engine *mgr, const char *partition,
                               const char *keyspace, const char *id,
                               long long timestamp, const char *origin)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return false;
    }
    bool ok = edb_shard_replica_delete(sh, id, timestamp, origin);
    shard_release(mgr, sh);
    return ok;
}


cJSON *edb_get_ts(edb_engine *mgr, const char *partition,
                  const char *keyspace, const char *id,
                  long long *timestamp_out)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = edb_shard_get_ts(sh, id, timestamp_out);
    shard_release(mgr, sh);
    return result;
}

cJSON *edb_all_ts(edb_engine *mgr, const char *partition,
                  const char *keyspace, const cJSON *filters)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = edb_shard_all_ts(sh, filters);
    shard_release(mgr, sh);
    return result;
}

cJSON *edb_query_ts(edb_engine *mgr, const char *partition,
                    const char *keyspace, const cJSON *filters)
{
    edb_shard *sh = shard_for(mgr, partition, keyspace);
    if (!sh) {
        return NULL;
    }
    cJSON *result = edb_shard_query_ts(sh, filters);
    shard_release(mgr, sh);
    return result;
}

void edb_free_strings(char **strings)
{
    if (!strings) {
        return;
    }
    for (size_t i = 0; strings[i]; i++) {
        free(strings[i]);
    }
    free(strings);
}
