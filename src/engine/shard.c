#include "shard_internal.h"

#include <inttypes.h>
#include <sys/stat.h>
#include "../epsilon_log.h"
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#define EDB_BUSY_TIMEOUT_MS 5000

static int64_t now_epoch(void)
{
    return (int64_t)time(NULL);
}

static void log_sqlite(edb_shard *sh, const char *context)
{
    edb_log("ERROR", "shard %s: %s: %s", sh->key, context,
            sqlite3_errmsg(sh->db));
}

/* Prepared statement cache. All callers hold sh->lock. */
static sqlite3_stmt *stmt_for(edb_shard *sh, const char *sql)
{
    for (int i = 0; i < sh->cache_count; i++) {
        if (strcmp(sh->cache[i].sql, sql) == 0) {
            sqlite3_reset(sh->cache[i].stmt);
            sqlite3_clear_bindings(sh->cache[i].stmt);
            return sh->cache[i].stmt;
        }
    }
    if (sh->cache_count == EDB_STMT_CACHE_SIZE) {
        sqlite3_finalize(sh->cache[0].stmt);
        memmove(&sh->cache[0], &sh->cache[1],
                (EDB_STMT_CACHE_SIZE - 1) * sizeof(sh->cache[0]));
        sh->cache_count--;
    }
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(sh->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_sqlite(sh, "prepare");
        return NULL;
    }
    edb_cached_stmt *slot = &sh->cache[sh->cache_count++];
    snprintf(slot->sql, sizeof(slot->sql), "%s", sql);
    slot->stmt = stmt;
    return stmt;
}

static size_t filter_count(const cJSON *filters)
{
    if (!filters) {
        return 0;
    }
    return cJSON_IsArray(filters) ? (size_t)cJSON_GetArraySize(filters) : 1;
}

static const cJSON *filter_at(const cJSON *filters, size_t index)
{
    return cJSON_IsArray(filters)
               ? cJSON_GetArrayItem(filters, (int)index)
               : (index == 0 ? filters : NULL);
}

static bool filter_operator_valid(const char *operator_name)
{
    return operator_name &&
           (strcmp(operator_name, "eq") == 0 ||
            strcmp(operator_name, "ne") == 0 ||
            strcmp(operator_name, "gt") == 0 ||
            strcmp(operator_name, "gte") == 0 ||
            strcmp(operator_name, "lt") == 0 ||
            strcmp(operator_name, "lte") == 0);
}

static bool filter_key_valid(const char *key)
{
    if (!key || !*key || strlen(key) > 512 || key[0] == '.' ||
        key[strlen(key) - 1] == '.') {
        return false;
    }
    bool segment = false;
    for (const unsigned char *c = (const unsigned char *)key; *c; c++) {
        if (*c == '.') {
            if (!segment) {
                return false;
            }
            segment = false;
        } else {
            if (*c < 0x20) {
                return false;
            }
            segment = true;
        }
    }
    return segment;
}

static bool filter_valid(const cJSON *filter)
{
    if (!cJSON_IsObject(filter)) {
        return false;
    }
    const cJSON *key = cJSON_GetObjectItemCaseSensitive(filter, "key");
    const cJSON *operator_item =
        cJSON_GetObjectItemCaseSensitive(filter, "operator");
    const cJSON *value = cJSON_GetObjectItemCaseSensitive(filter, "value");
    if (!cJSON_IsString(key) || !filter_key_valid(key->valuestring) ||
        !cJSON_IsString(operator_item) ||
        !filter_operator_valid(operator_item->valuestring) || !value) {
        return false;
    }
    if (strcmp(operator_item->valuestring, "eq") != 0 &&
        strcmp(operator_item->valuestring, "ne") != 0 &&
        !cJSON_IsNumber(value)) {
        return false;
    }
    return true;
}

bool edb_filters_valid(const cJSON *filters)
{
    if (!filters) {
        return true;
    }
    if (!cJSON_IsArray(filters) && !cJSON_IsObject(filters)) {
        return false;
    }
    size_t count = filter_count(filters);
    for (size_t i = 0; i < count; i++) {
        if (!filter_valid(filter_at(filters, i))) {
            return false;
        }
    }
    return true;
}

static char *json_path_for_key(const char *key)
{
    size_t length = strlen(key);
    if (length > (SIZE_MAX - 4) / 2) {
        return NULL;
    }
    char *path = malloc(length * 2 + 4);
    if (!path) {
        return NULL;
    }
    size_t position = 0;
    path[position++] = '$';
    const char *segment = key;
    for (;;) {
        const char *dot = strchr(segment, '.');
        size_t segment_length = dot ? (size_t)(dot - segment) : strlen(segment);
        path[position++] = '.';
        path[position++] = '"';
        for (size_t i = 0; i < segment_length; i++) {
            if (segment[i] == '"' || segment[i] == '\\') {
                path[position++] = '\\';
            }
            path[position++] = segment[i];
        }
        path[position++] = '"';
        if (!dot) {
            break;
        }
        segment = dot + 1;
    }
    path[position] = '\0';
    return path;
}

static const char *sql_operator(const char *operator_name)
{
    if (strcmp(operator_name, "eq") == 0) return "=";
    if (strcmp(operator_name, "ne") == 0) return "<>";
    if (strcmp(operator_name, "gt") == 0) return ">";
    if (strcmp(operator_name, "gte") == 0) return ">=";
    if (strcmp(operator_name, "lt") == 0) return "<";
    return "<=";
}

void edb_shard_settings_default(edb_shard_settings *out)
{
    if (!out) {
        return;
    }
    out->cache_size = 0;          /* 0 = auto: scaled from the shard size */
    snprintf(out->journal_mode, sizeof(out->journal_mode), "TRUNCATE");
    out->vacuum_seconds = 604800;  /* weekly */
    out->reindex_seconds = 86400;  /* daily */
}

/* Automatic page-cache floors and caps: never below the SQLite default
 * (2 MB) and never above 256 MB so a single large shard cannot pin an
 * unbounded amount of memory. */
#define EDB_CACHE_AUTO_MIN_KB 2048
#define EDB_CACHE_AUTO_MAX_KB 262144

/* Effective page-cache size (KiB) for a shard connection: the configured
 * value when set (negative = SQLite default, no pragma), otherwise 10% of
 * the on-disk shard size so bigger shards get proportionally bigger
 * caches. The file may not exist yet on first open; the floor applies. */
static long long effective_cache_kb(const char *path,
                                    const edb_shard_settings *s)
{
    if (!s) {
        return 0;
    }
    if (s->cache_size > 0) {
        return s->cache_size;
    }
    if (s->cache_size < 0) {
        return 0;
    }
    long long kb = EDB_CACHE_AUTO_MIN_KB;
    if (path) {
        struct stat st;
        if (stat(path, &st) == 0 && st.st_size > 0) {
            long long want = (long long)st.st_size / 10 / 1024;
            if (want > kb) {
                kb = want;
            }
            if (kb > EDB_CACHE_AUTO_MAX_KB) {
                kb = EDB_CACHE_AUTO_MAX_KB;
            }
        }
    }
    return kb;
}

/* Applies journal_mode + synchronous + cache_size to an open connection.
 * cache_size is a positive kibibyte count, expressed to SQLite via the
 * negative-value convention (abs(N) * 1024 bytes). */
static int configure_connection(sqlite3 *db, const char *key,
                                const char *path,
                                const edb_shard_settings *s)
{
    char sql[256];
    char *err = NULL;
    const char *mode = (s && s->journal_mode[0]) ? s->journal_mode
                                                 : "TRUNCATE";
    if (strcmp(mode, "DELETE") != 0 && strcmp(mode, "TRUNCATE") != 0 &&
        strcmp(mode, "WAL") != 0) {
        mode = "TRUNCATE";
    }
    snprintf(sql, sizeof(sql), "PRAGMA journal_mode=%s;"
                               "PRAGMA synchronous=FULL;", mode);
    if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
        edb_log("ERROR", "shard %s: journal pragma failed: %s", key,
                err ? err : "?");
        sqlite3_free(err);
        return -1;
    }
    long long cache_kb = effective_cache_kb(path, s);
    if (cache_kb > 0) {
        snprintf(sql, sizeof(sql), "PRAGMA cache_size=-%lld;",
                 (long long)cache_kb);
        if (sqlite3_exec(db, sql, NULL, NULL, &err) != SQLITE_OK) {
            edb_log("ERROR", "shard %s: cache_size pragma failed: %s", key,
                    err ? err : "?");
            sqlite3_free(err);
            return -1;
        }
    }
    return 0;
}

/* Runs the idempotent schema setup on an open connection. */
static int setup_schema(sqlite3 *db, const char *key)
{
    char *err = NULL;
    if (sqlite3_exec(db,
                     "CREATE TABLE IF NOT EXISTS Data ("
                     " id TEXT PRIMARY KEY,"
                     " value BLOB,"
                     " ttl INTEGER,"
                     " timestamp INT,"
                     " origin TEXT"
                     ");"
                     "CREATE INDEX IF NOT EXISTS idx_ttl ON Data (ttl);"
                     "DROP TABLE IF EXISTS DataFilter;",
                     NULL, NULL, &err) != SQLITE_OK) {
        edb_log("ERROR", "shard %s: schema failed: %s", key,
                err ? err : "?");
        sqlite3_free(err);
        return -1;
    }
    const char *migrations[] = {
        "ALTER TABLE Data DROP COLUMN model;",
        "ALTER TABLE Data DROP COLUMN version;",
        "ALTER TABLE Data ADD COLUMN origin TEXT;",
    };
    for (size_t i = 0; i < sizeof(migrations) / sizeof(migrations[0]); i++) {
        err = NULL;
        if (sqlite3_exec(db, migrations[i], NULL, NULL, &err) != SQLITE_OK) {
            sqlite3_free(err);
        }
    }
    return 0;
}

edb_shard *edb_shard_open(const char *path, const char *key,
                          const char *partition, const char *keyspace,
                          const edb_shard_settings *settings)
{
    edb_shard *sh = calloc(1, sizeof(*sh));
    if (!sh) {
        return NULL;
    }
    sh->path = strdup(path);
    snprintf(sh->key, sizeof(sh->key), "%s", key);
    snprintf(sh->partition, sizeof(sh->partition), "%s",
             partition ? partition : "");
    snprintf(sh->keyspace, sizeof(sh->keyspace), "%s",
             keyspace ? keyspace : "");
    pthread_mutex_init(&sh->lock, NULL);
    if (settings) {
        sh->settings = *settings;
    } else {
        edb_shard_settings_default(&sh->settings);
    }

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

    sqlite3_busy_timeout(sh->db, EDB_BUSY_TIMEOUT_MS);
    if (configure_connection(sh->db, sh->key, path, &sh->settings) != 0 ||
        setup_schema(sh->db, sh->key) != 0) {
        edb_shard_free(sh);
        return NULL;
    }

    sh->last_vacuum_ts = now_epoch();
    sh->last_reindex_ts = now_epoch();
    return sh;
}

/* Reopens an existing shard connection with new settings. */
bool edb_shard_reopen(edb_shard *sh, const edb_shard_settings *settings)
{
    if (!sh || !settings) {
        return false;
    }
    pthread_mutex_lock(&sh->lock);
    for (int i = 0; i < sh->cache_count; i++) {
        sqlite3_finalize(sh->cache[i].stmt);
    }
    sh->cache_count = 0;

    sqlite3 *fresh = NULL;
    if (sqlite3_open_v2(sh->path, &fresh,
                        SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                            SQLITE_OPEN_FULLMUTEX,
                        NULL) != SQLITE_OK) {
        if (fresh) {
            sqlite3_close(fresh);
        }
        pthread_mutex_unlock(&sh->lock);
        return false;
    }
    sqlite3_busy_timeout(fresh, EDB_BUSY_TIMEOUT_MS);
    if (configure_connection(fresh, sh->key, sh->path, settings) != 0 ||
        setup_schema(fresh, sh->key) != 0) {
        sqlite3_close(fresh);
        pthread_mutex_unlock(&sh->lock);
        return false;
    }
    sqlite3 *old = sh->db;
    sh->db = fresh;
    sh->settings = *settings;
    sh->last_vacuum_ts = now_epoch();
    sh->last_reindex_ts = now_epoch();
    if (old) {
        sqlite3_close_v2(old);
    }
    pthread_mutex_unlock(&sh->lock);
    return true;
}

void edb_shard_free(edb_shard *sh)
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
 * Replaces any existing row. */
static bool do_put_locked(edb_shard *sh, const char *id,
                          const char *json_value, long long ttl_absolute,
                          long long timestamp, const char *origin)
{
    sqlite3_stmt *stmt = stmt_for(
        sh, "INSERT OR REPLACE INTO Data"
            " (id, value, ttl, timestamp, origin)"
            " VALUES (?, ?, ?, ?, ?)");
    if (!stmt) {
        return false;
    }
    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
    sqlite3_bind_text(stmt, 2, json_value, -1, SQLITE_TRANSIENT);
    if (ttl_absolute >= 0) {
        sqlite3_bind_int64(stmt, 3, ttl_absolute);
    } else {
        sqlite3_bind_null(stmt, 3);
    }
    sqlite3_bind_int64(stmt, 4, timestamp);
    sqlite3_bind_text(stmt, 5, origin ? origin : "", -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_DONE) {
        log_sqlite(sh, "put");
        return false;
    }
    return true;
}

/* Current stored timestamp for id (any row, including soft-deleted).
 * Caller holds sh->lock. Returns false when the row does not exist. */
static bool stored_version_locked(edb_shard *sh, const char *id,
                                  long long *ts_out, char origin_out[64])
{
    sqlite3_stmt *stmt = stmt_for(
        sh, "SELECT timestamp, COALESCE(origin, '') FROM Data WHERE id = ?");
    if (!stmt) {
        return false;
    }
    sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
    if (sqlite3_step(stmt) != SQLITE_ROW ||
        sqlite3_column_type(stmt, 0) == SQLITE_NULL) {
        return false;
    }
    *ts_out = (long long)sqlite3_column_int64(stmt, 0);
    const char *origin = (const char *)sqlite3_column_text(stmt, 1);
    snprintf(origin_out, 64, "%s", origin ? origin : "");
    return true;
}

bool edb_shard_put(edb_shard *sh, const char *id, const char *json_value,
                   long long ttl_seconds)
{
    pthread_mutex_lock(&sh->lock);
    int64_t now = now_epoch();
    long long absolute_ttl = ttl_seconds >= 0
                                 ? (long long)now + ttl_seconds
                                 : -1;
    bool ok = do_put_locked(sh, id, json_value, absolute_ttl,
                            (long long)now, "");
    pthread_mutex_unlock(&sh->lock);
    return ok;
}

bool edb_shard_replica_put(edb_shard *sh, const char *id,
                           const char *json_value, long long ttl_absolute,
                           long long timestamp, const char *origin)
{
    pthread_mutex_lock(&sh->lock);
    long long stored_ts = 0;
    char stored_origin[64] = "";
    if (stored_version_locked(sh, id, &stored_ts, stored_origin) &&
        (stored_ts > timestamp ||
         (stored_ts == timestamp &&
          strcmp(stored_origin, origin ? origin : "") > 0))) {
        pthread_mutex_unlock(&sh->lock);
        return true;
    }
    bool ok = do_put_locked(sh, id, json_value, ttl_absolute, timestamp,
                            origin);
    pthread_mutex_unlock(&sh->lock);
    return ok;
}

bool edb_shard_replica_delete(edb_shard *sh, const char *id,
                              long long timestamp, const char *origin)
{
    pthread_mutex_lock(&sh->lock);

    long long stored_ts = 0;
    char stored_origin[64] = "";
    if (stored_version_locked(sh, id, &stored_ts, stored_origin) &&
        (stored_ts > timestamp ||
         (stored_ts == timestamp &&
          strcmp(stored_origin, origin ? origin : "") > 0))) {
        pthread_mutex_unlock(&sh->lock);
        return true;
    }

    sqlite3_stmt *stmt = stmt_for(
        sh, "INSERT INTO Data (id,value,ttl,timestamp,origin)"
            " VALUES (?,NULL,?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET value=NULL,ttl=excluded.ttl,"
            " timestamp=excluded.timestamp,"
            " origin=excluded.origin");
    bool ok = false;
    if (stmt) {
        sqlite3_bind_text(stmt, 1, id, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, timestamp - 5);
        sqlite3_bind_int64(stmt, 3, timestamp);
        sqlite3_bind_text(stmt, 4, origin ? origin : "", -1,
                          SQLITE_TRANSIENT);
        ok = sqlite3_step(stmt) == SQLITE_DONE;
        if (!ok) {
            log_sqlite(sh, "replica delete");
        }
    }
    pthread_mutex_unlock(&sh->lock);
    return ok;
}

cJSON *edb_shard_get_ts(edb_shard *sh, const char *id,
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
                    edb_log("WARN",
                            "shard %s: stored value for '%s' is not"
                            " valid JSON",
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

cJSON *edb_shard_get(edb_shard *sh, const char *id)
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
                    edb_log("WARN",
                            "shard %s: stored value for '%s' is not"
                            " valid JSON",
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

bool edb_shard_delete(edb_shard *sh, const char *id)
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

static sqlite3_stmt *prepare_live_query(edb_shard *sh, const char *select,
                                        const cJSON *filters,
                                        bool order_by_timestamp)
{
    if (!edb_filters_valid(filters)) {
        return NULL;
    }
    size_t count = filter_count(filters);
    if (count > (SIZE_MAX - strlen(select) - 96) / 256) {
        return NULL;
    }
    size_t capacity = strlen(select) + count * 256 + 96;
    char *sql = malloc(capacity);
    if (!sql) {
        return NULL;
    }
    size_t length = (size_t)snprintf(sql, capacity,
                                     "%s (ttl IS NULL OR ttl >= ?)", select);
    for (size_t i = 0; i < count; i++) {
        const cJSON *filter = filter_at(filters, i);
        const char *operator_name =
            cJSON_GetObjectItemCaseSensitive(filter, "operator")->valuestring;
        const char *fragment;
        if (strcmp(operator_name, "eq") == 0) {
            fragment = " AND json_type(value, ?) IS json_type(?, '$')"
                       " AND json_extract(value, ?) IS json_extract(?, '$')";
        } else if (strcmp(operator_name, "ne") == 0) {
            fragment = " AND json_type(value, ?) IS NOT NULL"
                       " AND (json_type(value, ?) IS NOT json_type(?, '$')"
                       " OR json_extract(value, ?) IS NOT json_extract(?, '$'))";
        } else {
            char comparison[160];
            snprintf(comparison, sizeof(comparison),
                     " AND json_type(value, ?) IN ('integer','real')"
                     " AND json_extract(value, ?) %s json_extract(?, '$')",
                     sql_operator(operator_name));
            size_t needed = strlen(comparison);
            if (needed >= capacity - length) {
                free(sql);
                return NULL;
            }
            memcpy(sql + length, comparison, needed + 1);
            length += needed;
            continue;
        }
        size_t needed = strlen(fragment);
        if (needed >= capacity - length) {
            free(sql);
            return NULL;
        }
        memcpy(sql + length, fragment, needed + 1);
        length += needed;
    }
    if (order_by_timestamp) {
        snprintf(sql + length, capacity - length, " ORDER BY timestamp ASC");
    }

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(sh->db, sql, -1, &stmt, NULL) != SQLITE_OK) {
        log_sqlite(sh, "prepare JSON filter query");
        free(sql);
        return NULL;
    }
    free(sql);
    sqlite3_bind_int64(stmt, 1, now_epoch());
    int parameter = 2;
    for (size_t i = 0; i < count; i++) {
        const cJSON *filter = filter_at(filters, i);
        const cJSON *key = cJSON_GetObjectItemCaseSensitive(filter, "key");
        const cJSON *operator_item =
            cJSON_GetObjectItemCaseSensitive(filter, "operator");
        const cJSON *value = cJSON_GetObjectItemCaseSensitive(filter, "value");
        char *path = json_path_for_key(key->valuestring);
        char *encoded_value = cJSON_PrintUnformatted(value);
        if (!path || !encoded_value) {
            free(path);
            free(encoded_value);
            sqlite3_finalize(stmt);
            return NULL;
        }
        if (strcmp(operator_item->valuestring, "eq") == 0) {
            sqlite3_bind_text(stmt, parameter++, path, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, encoded_value, -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, path, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, encoded_value, -1,
                              SQLITE_TRANSIENT);
        } else if (strcmp(operator_item->valuestring, "ne") == 0) {
            sqlite3_bind_text(stmt, parameter++, path, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, path, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, encoded_value, -1,
                              SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, path, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, encoded_value, -1,
                              SQLITE_TRANSIENT);
        } else {
            sqlite3_bind_text(stmt, parameter++, path, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, path, -1, SQLITE_TRANSIENT);
            sqlite3_bind_text(stmt, parameter++, encoded_value, -1,
                              SQLITE_TRANSIENT);
        }
        free(path);
        free(encoded_value);
    }
    return stmt;
}

char **edb_shard_ids(edb_shard *sh, const cJSON *filters,
                     size_t *count_out)
{
    *count_out = 0;
    char **results = NULL;
    size_t count = 0;
    size_t capacity = 0;
    pthread_mutex_lock(&sh->lock);
    sqlite3_stmt *stmt = prepare_live_query(sh, "SELECT id FROM Data WHERE",
                                            filters, true);
    if (!stmt) {
        pthread_mutex_unlock(&sh->lock);
        return NULL;
    }
    bool ok = true;
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        if (count == capacity) {
            size_t new_capacity = capacity ? capacity * 2 : 16;
            char **grown = realloc(results, new_capacity * sizeof(char *));
            if (!grown) {
                ok = false;
                break;
            }
            results = grown;
            capacity = new_capacity;
        }
        const char *id = (const char *)sqlite3_column_text(stmt, 0);
        results[count] = id ? strdup(id) : NULL;
        if (!results[count]) {
            ok = false;
            break;
        }
        count++;
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&sh->lock);
    if (!ok) {
        for (size_t i = 0; i < count; i++) free(results[i]);
        free(results);
        return NULL;
    }
    char **terminated = realloc(results, (count + 1) * sizeof(char *));
    if (!terminated) {
        for (size_t i = 0; i < count; i++) free(results[i]);
        free(results);
        return NULL;
    }
    terminated[count] = NULL;
    *count_out = count;
    return terminated;
}

/* meta=true returns [{"id":..,"timestamp":..,"value":..}, ...] for
 * replica merging instead of a plain array of values. */
static cJSON *collect_values(edb_shard *sh, const cJSON *filters, bool meta)
{
    cJSON *array = cJSON_CreateArray();
    if (!array) {
        return NULL;
    }
    pthread_mutex_lock(&sh->lock);
    sqlite3_stmt *stmt = prepare_live_query(
        sh, meta ? "SELECT id, value, timestamp FROM Data WHERE"
                 : "SELECT value FROM Data WHERE",
        filters, false);
    if (!stmt) {
        pthread_mutex_unlock(&sh->lock);
        cJSON_Delete(array);
        return NULL;
    }
    while (sqlite3_step(stmt) == SQLITE_ROW) {
        const char *json = (const char *)sqlite3_column_blob(stmt, meta ? 1 : 0);
        int length = sqlite3_column_bytes(stmt, meta ? 1 : 0);
        cJSON *document = json && length > 0
                              ? cJSON_ParseWithLength(json, (size_t)length)
                              : NULL;
        if (!document) {
            continue;
        }
        if (meta) {
            cJSON *row = cJSON_CreateObject();
            const char *id = (const char *)sqlite3_column_text(stmt, 0);
            if (!row) {
                cJSON_Delete(document);
                continue;
            }
            cJSON_AddStringToObject(row, "id", id ? id : "");
            cJSON_AddNumberToObject(row, "timestamp",
                                    (double)sqlite3_column_int64(stmt, 2));
            cJSON_AddItemToObject(row, "value", document);
            cJSON_AddItemToArray(array, row);
        } else {
            cJSON_AddItemToArray(array, document);
        }
    }
    sqlite3_finalize(stmt);
    pthread_mutex_unlock(&sh->lock);
    return array;
}

cJSON *edb_shard_all(edb_shard *sh, const cJSON *filters)
{
    return collect_values(sh, filters, false);
}

cJSON *edb_shard_query(edb_shard *sh, const cJSON *filters)
{
    return collect_values(sh, filters, false);
}

cJSON *edb_shard_all_ts(edb_shard *sh, const cJSON *filters)
{
    return collect_values(sh, filters, true);
}

cJSON *edb_shard_query_ts(edb_shard *sh, const cJSON *filters)
{
    return collect_values(sh, filters, true);
}

bool edb_shard_cleanup(edb_shard *sh)
{
    pthread_mutex_lock(&sh->lock);
    bool ok = true;
    char *err = NULL;

    if (sqlite3_exec(sh->db, "BEGIN IMMEDIATE", NULL, NULL, &err) != SQLITE_OK) {
        edb_log("ERROR", "shard %s: cleanup begin failed: %s", sh->key,
                err ? err : "?");
        sqlite3_free(err);
        pthread_mutex_unlock(&sh->lock);
        return false;
    }

    sqlite3_stmt *stmt = stmt_for(
        sh, "DELETE FROM Data WHERE ttl IS NOT NULL AND ttl < ?");
    if (stmt) {
        sqlite3_bind_int64(stmt, 1, now_epoch() - EDB_CLEANUP_GRACE_SECONDS);
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            log_sqlite(sh, "cleanup");
            ok = false;
        } else {
            sh->expired_since_vacuum += sqlite3_changes(sh->db);
        }
    } else {
        ok = false;
    }

    err = NULL;
    if (ok) {
        ok = (sqlite3_exec(sh->db, "COMMIT", NULL, NULL, &err) == SQLITE_OK);
    } else {
        sqlite3_exec(sh->db, "ROLLBACK", NULL, NULL, NULL);
    }
    sqlite3_free(err);

    /* time-based maintenance, gated on the per-partition intervals */
    int64_t now = now_epoch();
    if (ok && sh->settings.vacuum_seconds > 0 &&
        now - sh->last_vacuum_ts >= sh->settings.vacuum_seconds) {
        err = NULL;
        if (sqlite3_exec(sh->db, "VACUUM", NULL, NULL, &err) == SQLITE_OK) {
            sh->last_vacuum_ts = now;
            sh->expired_since_vacuum = 0;
        } else {
            edb_log("ERROR", "shard %s: vacuum failed: %s", sh->key,
                    err ? err : "?");
            sqlite3_free(err);
        }
    }
    if (ok && sh->settings.reindex_seconds > 0 &&
        now - sh->last_reindex_ts >= sh->settings.reindex_seconds) {
        err = NULL;
        if (sqlite3_exec(sh->db, "REINDEX", NULL, NULL, &err) == SQLITE_OK) {
            sh->last_reindex_ts = now;
        } else {
            edb_log("ERROR", "shard %s: reindex failed: %s", sh->key,
                    err ? err : "?");
            sqlite3_free(err);
        }
    }

    pthread_mutex_unlock(&sh->lock);
    return ok;
}
