#include "shard_internal.h"

#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "md5.h"

#define ZDB_BUSY_TIMEOUT_MS 5000

static int64_t now_epoch(void)
{
    return (int64_t)time(NULL);
}

static void log_sqlite(zdb_shard *sh, const char *context)
{
    fprintf(stderr, "zdb: shard %s: %s: %s\n", sh->key, context,
            sqlite3_errmsg(sh->db));
}

/* Prepared statement cache. All callers hold sh->lock. */
static sqlite3_stmt *stmt_for(zdb_shard *sh, const char *sql)
{
    for (int i = 0; i < sh->cache_count; i++) {
        if (strcmp(sh->cache[i].sql, sql) == 0) {
            sqlite3_reset(sh->cache[i].stmt);
            sqlite3_clear_bindings(sh->cache[i].stmt);
            return sh->cache[i].stmt;
        }
    }
    if (sh->cache_count == ZDB_STMT_CACHE_SIZE) {
        return NULL;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(sh->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_sqlite(sh, "prepare");
        return NULL;
    }
    zdb_cached_stmt *slot = &sh->cache[sh->cache_count++];
    snprintf(slot->sql, sizeof(slot->sql), "%s", sql);
    slot->stmt = stmt;
    return stmt;
}

/* Builds the shared filter clause:
 *   AND id IN (SELECT id FROM DataFilter WHERE filter_hash IN (...)
 *              GROUP BY id HAVING COUNT(DISTINCT filter_hash) = ?)
 * Returns a malloc'd SQL fragment (or NULL when nfilters == 0) and fills
 * hash_out (array of nfilters 33-byte buffers) with the md5 hashes. */
static char *build_filter_clause(const char **filters, size_t nfilters,
                                 char (*hash_out)[33])
{
    if (nfilters == 0) {
        return NULL;
    }
    size_t cap = 1;
    for (size_t i = 0; i < nfilters; i++) {
        zdb_md5_hex(filters[i], strlen(filters[i]), hash_out[i]);
        cap += strlen("?,") + 1;
    }
    char *clause = malloc(cap + 128);
    if (!clause) {
        return NULL;
    }
    strcpy(clause, " AND id IN (SELECT id FROM DataFilter WHERE filter_hash IN (");
    size_t pos = strlen(clause);
    for (size_t i = 0; i < nfilters; i++) {
        clause[pos++] = '?';
        if (i + 1 < nfilters) {
            clause[pos++] = ',';
        }
    }
    pos += (size_t)snprintf(clause + pos, cap + 128 - pos,
                            ") GROUP BY id HAVING COUNT(DISTINCT filter_hash)"
                            " = ?)");
    return clause;
}

static void bind_hashes(sqlite3_stmt *stmt, char (*hashes)[33],
                        size_t nfilters, int start_index)
{
    for (size_t i = 0; i < nfilters; i++) {
        sqlite3_bind_text(stmt, (int)(start_index + i), hashes[i], -1,
                          SQLITE_STATIC);
    }
    sqlite3_bind_int64(stmt, (int)(start_index + nfilters),
                       (sqlite3_int64)nfilters);
}

zdb_shard *zdb_shard_open(const char *path, const char *key)
{
    zdb_shard *sh = calloc(1, sizeof(*sh));
    if (!sh) {
        return NULL;
    }
    sh->path = strdup(path);
    snprintf(sh->key, sizeof(sh->key), "%s", key);
    pthread_mutex_init(&sh->lock, NULL);

    int rc = sqlite3_open_v2(path, &sh->db,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                 SQLITE_OPEN_FULLMUTEX,
                             NULL);
    if (rc != SQLITE_OK) {
        if (sh->db) {
            log_sqlite(sh, "open");
            sqlite3_close(sh->db);
        }
        pthread_mutex_destroy(&sh->lock);
        free(sh->path);
        free(sh);
        return NULL;
    }

    sqlite3_busy_timeout(sh->db, ZDB_BUSY_TIMEOUT_MS);
    char *err = NULL;
    if (sqlite3_exec(sh->db, "PRAGMA journal_mode=DELETE;"
                             "PRAGMA synchronous=FULL;",
                     NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "zdb: shard %s: pragma failed: %s\n", sh->key,
                err ? err : "?");
        sqlite3_free(err);
        zdb_shard_free(sh);
        return NULL;
    }

    err = NULL;
    if (sqlite3_exec(sh->db,
                     "CREATE TABLE IF NOT EXISTS Data ("
                     " id TEXT PRIMARY KEY,"
                     " value BLOB,"
                     " ttl INTEGER,"
                     " timestamp INT,"
                     " filter TEXT"
                     ");"
                     "CREATE TABLE IF NOT EXISTS DataFilter ("
                     " id TEXT NOT NULL,"
                     " filter_hash TEXT NOT NULL,"
                     " PRIMARY KEY (id, filter_hash)"
                     ");"
                     "CREATE INDEX IF NOT EXISTS idx_ttl ON Data (ttl);"
                     "CREATE INDEX IF NOT EXISTS idx_filter_lookup"
                     " ON DataFilter (filter_hash);",
                     NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "zdb: shard %s: schema failed: %s\n", sh->key,
                err ? err : "?");
        sqlite3_free(err);
        zdb_shard_free(sh);
        return NULL;
    }

    return sh;
}

void zdb_shard_free(zdb_shard *sh)
{
    if (!sh) {
        return;
    }
    pthread_mutex_lock(&sh->lock);
    for (int i = 0; i < sh->cache_count; i++) {
        sqlite3_finalize(sh->cache[i].stmt);
    }
    sh->cache_count = 0;
    if (sh->db) {
        sqlite3_close(sh->db);
        sh->db = NULL;
    }
    pthread_mutex_unlock(&sh->lock);
    pthread_mutex_destroy(&sh->lock);
    free(sh->path);
    free(sh);
}

/* Core write path. Assumes sh->lock held. ttl_absolute is an epoch
 * expiry or -1 for none; timestamp is stored verbatim as last-modified.
 * Replaces any existing row and rebuilds its filter index. */
static bool do_put_locked(zdb_shard *sh, const char *id,
                          const char *json_value, long long ttl_absolute,
                          long long timestamp, const char **filters,
                          size_t nfilters)
{
    char *err = NULL;
    if (sqlite3_exec(sh->db, "BEGIN IMMEDIATE", NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "zdb: shard %s: begin failed: %s\n", sh->key,
                err ? err : "?");
        sqlite3_free(err);
        return false;
    }

    bool ok = false;
    sqlite3_stmt *stmt = stmt_for(
        sh, "INSERT OR REPLACE INTO Data (id, value, ttl, timestamp, filter)"
            " VALUES (?, ?, ?, ?, ?)");
    if (stmt) {
        sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, json_value, -1, SQLITE_TRANSIENT);
        if (ttl_absolute >= 0) {
            sqlite3_bind_int64(stmt, 3, ttl_absolute);
        } else {
            sqlite3_bind_null(stmt, 3);
        }
        sqlite3_bind_int64(stmt, 4, timestamp);

        char hashes[16][33];
        size_t n = nfilters < 16 ? nfilters : 16;
        size_t filter_len = 0;
        for (size_t i = 0; i < n; i++) {
            zdb_md5_hex(filters[i], strlen(filters[i]), hashes[i]);
            filter_len += 33;
        }
        char *filter_col_dyn = NULL;
        if (n > 0) {
            filter_col_dyn = malloc(filter_len);
            if (filter_col_dyn) {
                filter_col_dyn[0] = '\0';
                for (size_t i = 0; i < n; i++) {
                    if (i > 0) {
                        strcat(filter_col_dyn, ",");
                    }
                    strcat(filter_col_dyn, hashes[i]);
                }
            }
            sqlite3_bind_text(stmt, 5, filter_col_dyn ? filter_col_dyn : "",
                              -1, SQLITE_TRANSIENT);
        } else {
            sqlite3_bind_text(stmt, 5, "", -1, SQLITE_STATIC);
        }

        ok = (sqlite3_step(stmt) == SQLITE_DONE);
        if (!ok) {
            log_sqlite(sh, "put");
        }
        free(filter_col_dyn);
    }

    if (ok) {
        stmt = stmt_for(sh, "DELETE FROM DataFilter WHERE id = ?");
        if (stmt) {
            sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
            ok = (sqlite3_step(stmt) == SQLITE_DONE);
            if (!ok) {
                log_sqlite(sh, "put: clear filters");
            }
        } else {
            ok = false;
        }
    }

    if (ok) {
        stmt = stmt_for(sh, "INSERT INTO DataFilter (id, filter_hash)"
                            " VALUES (?, ?)");
        for (size_t i = 0; ok && i < nfilters; i++) {
            char hash[33];
            zdb_md5_hex(filters[i], strlen(filters[i]), hash);
            sqlite3_reset(stmt);
            sqlite3_clear_bindings(stmt);
            sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, 2, hash, -1, SQLITE_STATIC);
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                log_sqlite(sh, "put: filter index");
                ok = false;
            }
        }
    }

    err = NULL;
    if (ok) {
        ok = (sqlite3_exec(sh->db, "COMMIT", NULL, NULL, &err) == SQLITE_OK);
        if (!ok) {
            fprintf(stderr, "zdb: shard %s: commit failed: %s\n", sh->key,
                    err ? err : "?");
        }
    } else {
        sqlite3_exec(sh->db, "ROLLBACK", NULL, NULL, NULL);
    }
    sqlite3_free(err);
    return ok;
}

/* Current stored timestamp for id (any row, including soft-deleted).
 * Caller holds sh->lock. Returns false when the row does not exist. */
static bool stored_timestamp_locked(zdb_shard *sh, const char *id,
                                    long long *ts_out)
{
    sqlite3_stmt *stmt =
        stmt_for(sh, "SELECT timestamp FROM Data WHERE id = ?");
    if (!stmt) {
        return false;
    }
    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
    bool found = false;
    if (sqlite3_step(stmt) == SQLITE_ROW &&
        sqlite3_column_type(stmt, 0) != SQLITE_NULL) {
        *ts_out = (long long)sqlite3_column_int64(stmt, 0);
        found = true;
    }
    return found;
}

bool zdb_shard_put(zdb_shard *sh, const char *id, const char *json_value,
                   long long ttl_seconds, const char **filters,
                   size_t nfilters)
{
    pthread_mutex_lock(&sh->lock);

    char filter_col[1] = "";
    (void)filter_col;

    int64_t now = now_epoch();
    long long abs_ttl = ttl_seconds >= 0 ? (long long)now + ttl_seconds : -1;
    bool ok = do_put_locked(sh, id, json_value, abs_ttl, (long long)now,
                            filters, nfilters);
    pthread_mutex_unlock(&sh->lock);
    return ok;
}

bool zdb_shard_replica_put(zdb_shard *sh, const char *id,
                           const char *json_value, long long ttl_absolute,
                           long long timestamp, const char **filters,
                           size_t nfilters)
{
    pthread_mutex_lock(&sh->lock);

    /* last-write-wins: skip when we already hold a newer version */
    long long stored_ts = 0;
    if (stored_timestamp_locked(sh, id, &stored_ts) && stored_ts > timestamp) {
        pthread_mutex_unlock(&sh->lock);
        return true;
    }

    bool ok = do_put_locked(sh, id, json_value, ttl_absolute, timestamp,
                            filters, nfilters);
    pthread_mutex_unlock(&sh->lock);
    return ok;
}

bool zdb_shard_replica_delete(zdb_shard *sh, const char *id,
                              long long timestamp)
{
    pthread_mutex_lock(&sh->lock);

    long long stored_ts = 0;
    if (stored_timestamp_locked(sh, id, &stored_ts) && stored_ts > timestamp) {
        pthread_mutex_unlock(&sh->lock);
        return true;
    }

    bool ok = false;
    sqlite3_stmt *stmt = stmt_for(
        sh, "UPDATE Data SET ttl = ?, timestamp = ? WHERE id = ?");
    if (stmt) {
        sqlite3_bind_int64(stmt, 1, timestamp - 5);
        sqlite3_bind_int64(stmt, 2, timestamp);
        sqlite3_bind_text(stmt, 3, id, -1, SQLITE_TRANSIENT);
        ok = (sqlite3_step(stmt) == SQLITE_DONE);
        if (!ok) {
            log_sqlite(sh, "replica delete");
        }
    }
    pthread_mutex_unlock(&sh->lock);
    return ok;
}

cJSON *zdb_shard_get_ts(zdb_shard *sh, const char *id,
                        long long *timestamp_out)
{
    pthread_mutex_lock(&sh->lock);
    cJSON *result = NULL;

    sqlite3_stmt *stmt =
        stmt_for(sh, "SELECT value, timestamp FROM Data WHERE id = ?"
                     " AND (ttl IS NULL OR ttl >= ?)");
    if (stmt) {
        sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, now_epoch());
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *json =
                (const char *)sqlite3_column_blob(stmt, 0);
            int len = sqlite3_column_bytes(stmt, 0);
            if (json && len > 0 &&
                sqlite3_column_type(stmt, 1) != SQLITE_NULL && timestamp_out) {
                *timestamp_out =
                    (long long)sqlite3_column_int64(stmt, 1);
            }
            if (json && len > 0) {
                result = cJSON_ParseWithLength(json, (size_t)len);
                if (!result) {
                    fprintf(stderr,
                            "zdb: shard %s: stored value for '%s' is not"
                            " valid JSON\n",
                            sh->key, id);
                }
            }
        } else if (sqlite3_errcode(sh->db) != SQLITE_OK &&
                   sqlite3_errcode(sh->db) != SQLITE_DONE &&
                   sqlite3_errcode(sh->db) != SQLITE_ROW) {
            log_sqlite(sh, "get");
        }
    }
    pthread_mutex_unlock(&sh->lock);
    return result;
}

cJSON *zdb_shard_get(zdb_shard *sh, const char *id)
{
    pthread_mutex_lock(&sh->lock);
    cJSON *result = NULL;

    sqlite3_stmt *stmt =
        stmt_for(sh, "SELECT value FROM Data WHERE id = ?"
                     " AND (ttl IS NULL OR ttl >= ?)");
    if (stmt) {
        sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, now_epoch());
        if (sqlite3_step(stmt) == SQLITE_ROW) {
            const char *json =
                (const char *)sqlite3_column_blob(stmt, 0);
            int len = sqlite3_column_bytes(stmt, 0);
            if (json && len > 0) {
                result = cJSON_ParseWithLength(json, (size_t)len);
                if (!result) {
                    fprintf(stderr,
                            "zdb: shard %s: stored value for '%s' is not"
                            " valid JSON\n",
                            sh->key, id);
                }
            }
        } else if (sqlite3_errcode(sh->db) != SQLITE_OK &&
                   sqlite3_errcode(sh->db) != SQLITE_DONE &&
                   sqlite3_errcode(sh->db) != SQLITE_ROW) {
            log_sqlite(sh, "get");
        }
    }
    pthread_mutex_unlock(&sh->lock);
    return result;
}

bool zdb_shard_delete(zdb_shard *sh, const char *id)
{
    pthread_mutex_lock(&sh->lock);
    bool ok = false;

    sqlite3_stmt *stmt = stmt_for(
        sh, "UPDATE Data SET ttl = ?, timestamp = ? WHERE id = ?");
    if (stmt) {
        int64_t now = now_epoch();
        sqlite3_bind_int64(stmt, 1, now - 5);
        sqlite3_bind_int64(stmt, 2, now);
        sqlite3_bind_text(stmt, 3, id, -1, SQLITE_TRANSIENT);
        ok = (sqlite3_step(stmt) == SQLITE_DONE);
        if (!ok) {
            log_sqlite(sh, "delete");
        }
    }
    pthread_mutex_unlock(&sh->lock);
    return ok;
}

static sqlite3_stmt *prepare_live_query(zdb_shard *sh, const char *select,
                                        const char **filters,
                                        size_t nfilters,
                                        char (*hashes)[33],
                                        bool order_by_timestamp)
{
    char *clause = build_filter_clause(filters, nfilters, hashes);
    if (nfilters > 0 && !clause) {
        return NULL;
    }
    size_t len = strlen(select) + (clause ? strlen(clause) : 0) + 64;
    char *sql = malloc(len);
    if (!sql) {
        free(clause);
        return NULL;
    }
    snprintf(sql, len, "%s (ttl IS NULL OR ttl >= ?)%s%s", select,
             clause ? clause : "",
             order_by_timestamp ? " ORDER BY timestamp ASC" : "");
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(sh->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_sqlite(sh, "prepare query");
        stmt = NULL;
    }
    free(sql);
    free(clause);
    return stmt;
}

char **zdb_shard_ids(zdb_shard *sh, const char **filters, size_t nfilters,
                     size_t *count_out)
{
    *count_out = 0;
    char **results = NULL;
    size_t count = 0;
    size_t cap = 0;

    char hashes[16][33];
    size_t n = nfilters < 16 ? nfilters : 16;

    pthread_mutex_lock(&sh->lock);
    sqlite3_stmt *stmt = prepare_live_query(sh, "SELECT id FROM Data WHERE",
                                            filters, n, hashes, true);
    if (!stmt) {
        pthread_mutex_unlock(&sh->lock);
        return NULL;
    }
    sqlite3_bind_int64(stmt, 1, now_epoch());
    bind_hashes(stmt, hashes, n, 2);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (count == cap) {
            size_t newcap = cap ? cap * 2 : 16;
            char **grown = realloc(results, newcap * sizeof(char *));
            if (!grown) {
                goto done;
            }
            results = grown;
            cap = newcap;
        }
        const unsigned char *id = sqlite3_column_text(stmt, 0);
        char *copy = id ? strdup((const char *)id) : NULL;
        if (!copy) {
            goto done;
        }
        results[count++] = copy;
    }
    if (sqlite3_errcode(sh->db) != SQLITE_OK &&
        sqlite3_errcode(sh->db) != SQLITE_DONE) {
        log_sqlite(sh, "ids");
    }

done:
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&sh->lock);
    results = realloc(results, (count + 1) * sizeof(char *));
    if (results) {
        results[count] = NULL;
    }
    *count_out = count;
    return results;
}

/* meta=true returns [{"id":..,"timestamp":..,"value":..}, ...] for
 * replica merging instead of a plain array of values. */
static cJSON *collect_values(zdb_shard *sh, const char **filters,
                             size_t nfilters, const char **fields,
                             size_t nfields, bool meta)
{
    cJSON *array = cJSON_CreateArray();
    if (!array) {
        return NULL;
    }
    char hashes[16][33];
    size_t n = nfilters < 16 ? nfilters : 16;

    pthread_mutex_lock(&sh->lock);
    sqlite3_stmt *stmt =
        prepare_live_query(sh,
                           meta ? "SELECT id, value, timestamp FROM Data WHERE"
                                : "SELECT value FROM Data WHERE",
                           filters, n, hashes, false);
    if (!stmt) {
        pthread_mutex_unlock(&sh->lock);
        cJSON_Delete(array);
        return NULL;
    }
    sqlite3_bind_int64(stmt, 1, now_epoch());
    bind_hashes(stmt, hashes, n, 2);

    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_blob(stmt, meta ? 1 : 0);
        int len = sqlite3_column_bytes(stmt, meta ? 1 : 0);
        if (!json || len <= 0) {
            continue;
        }
        cJSON *doc = cJSON_ParseWithLength(json, (size_t)len);
        if (!doc) {
            continue;
        }
        bool match = true;
        for (size_t i = 0; match && i < nfields; i++) {
            const char *eq = strchr(fields[i], '=');
            if (!eq) {
                match = false;
                break;
            }
            size_t name_len = (size_t)(eq - fields[i]);
            char field[128];
            if (name_len >= sizeof(field)) {
                match = false;
                break;
            }
            memcpy(field, fields[i], name_len);
            field[name_len] = '\0';
            const char *expected = eq + 1;

            cJSON *item =
                cJSON_GetObjectItemCaseSensitive(doc, field);
            if (cJSON_IsString(item)) {
                match = (strcmp(item->valuestring, expected) == 0);
            } else if (cJSON_IsNumber(item)) {
                char *end = NULL;
                double want = strtod(expected, &end);
                match = (end && *end == '\0' &&
                         item->valuedouble == want);
            } else {
                match = false;
            }
        }
        if (!match) {
            cJSON_Delete(doc);
            continue;
        }
        if (meta) {
            cJSON *wrap = cJSON_CreateObject();
            const char *rid =
                (const char *)sqlite3_column_text(stmt, 0);
            cJSON_AddStringToObject(wrap, "id", rid ? rid : "");
            cJSON_AddNumberToObject(
                wrap, "timestamp",
                (double)sqlite3_column_int64(stmt, 2));
            cJSON_AddItemToObject(wrap, "value", doc);
            cJSON_AddItemToArray(array, wrap);
        } else {
            cJSON_AddItemToArray(array, doc);
        }
    }
    if (sqlite3_errcode(sh->db) != SQLITE_OK &&
        sqlite3_errcode(sh->db) != SQLITE_DONE) {
        log_sqlite(sh, "query");
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&sh->lock);
    return array;
}

cJSON *zdb_shard_all(zdb_shard *sh, const char **filters, size_t nfilters)
{
    return collect_values(sh, filters, nfilters, NULL, 0, false);
}

cJSON *zdb_shard_query(zdb_shard *sh, const char **filters, size_t nfilters,
                       const char **fields, size_t nfields)
{
    return collect_values(sh, filters, nfilters, fields, nfields, false);
}

cJSON *zdb_shard_all_ts(zdb_shard *sh, const char **filters,
                        size_t nfilters)
{
    return collect_values(sh, filters, nfilters, NULL, 0, true);
}

cJSON *zdb_shard_query_ts(zdb_shard *sh, const char **filters,
                          size_t nfilters, const char **fields,
                          size_t nfields)
{
    return collect_values(sh, filters, nfilters, fields, nfields, true);
}

bool zdb_shard_cleanup(zdb_shard *sh)
{
    pthread_mutex_lock(&sh->lock);
    bool ok = true;
    char *err = NULL;

    if (sqlite3_exec(sh->db, "BEGIN IMMEDIATE", NULL, NULL, &err) != SQLITE_OK) {
        fprintf(stderr, "zdb: shard %s: cleanup begin failed: %s\n", sh->key,
                err ? err : "?");
        sqlite3_free(err);
        pthread_mutex_unlock(&sh->lock);
        return false;
    }

    sqlite3_stmt *stmt = stmt_for(
        sh, "DELETE FROM Data WHERE ttl IS NOT NULL AND ttl < ?");
    if (stmt) {
        sqlite3_bind_int64(stmt, 1, now_epoch() - ZDB_CLEANUP_GRACE_SECONDS);
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            log_sqlite(sh, "cleanup");
            ok = false;
        } else {
            sh->expired_since_vacuum += sqlite3_changes(sh->db);
        }
    } else {
        ok = false;
    }

    if (ok) {
        stmt = stmt_for(
            sh, "DELETE FROM DataFilter WHERE id NOT IN (SELECT id FROM Data)");
        if (stmt) {
            if (sqlite3_step(stmt) != SQLITE_DONE) {
                log_sqlite(sh, "cleanup filters");
                ok = false;
            }
        } else {
            ok = false;
        }
    }

    err = NULL;
    if (ok) {
        ok = (sqlite3_exec(sh->db, "COMMIT", NULL, NULL, &err) == SQLITE_OK);
    } else {
        sqlite3_exec(sh->db, "ROLLBACK", NULL, NULL, NULL);
    }
    sqlite3_free(err);

    if (ok && !sh->vacuum_pending &&
        sh->expired_since_vacuum >= ZDB_VACUUM_THRESHOLD) {
        sh->vacuum_pending = true;
    }

    if (ok && sh->vacuum_pending) {
        err = NULL;
        if (sqlite3_exec(sh->db, "VACUUM", NULL, NULL, &err) == SQLITE_OK) {
            sh->vacuum_pending = false;
            sh->expired_since_vacuum = 0;
        } else {
            fprintf(stderr, "zdb: shard %s: vacuum failed: %s\n", sh->key,
                    err ? err : "?");
            sqlite3_free(err);
        }
    }

    pthread_mutex_unlock(&sh->lock);
    return ok;
}
