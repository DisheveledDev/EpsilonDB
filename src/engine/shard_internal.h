/* Internal structures shared between manager.c and shard.c. */

#ifndef ZDB_SHARD_INTERNAL_H
#define ZDB_SHARD_INTERNAL_H

#include <pthread.h>
#include <stdbool.h>

#include "zesty_engine.h"

#define ZDB_STMT_CACHE_SIZE 8
#define ZDB_VACUUM_THRESHOLD 10000
typedef struct {
    char sql[192];
    sqlite3_stmt *stmt;
} zdb_cached_stmt;

typedef struct zdb_shard {
    char *path;
    char key[33];              /* framed partition/keyspace digest */
    char partition[256];       /* owning partition ("" until lazily known) */
    char keyspace[128];
    sqlite3 *db;
    pthread_mutex_t lock;
    size_t refs;
    bool retired;

    zdb_cached_stmt cache[ZDB_STMT_CACHE_SIZE];
    int cache_count;

    long long expired_since_vacuum;
    zdb_shard_settings settings;
    long long last_vacuum_ts;
    long long last_reindex_ts;
} zdb_shard;

zdb_shard *zdb_shard_open(const char *path, const char *key,
                          const char *partition, const char *keyspace,
                          const zdb_shard_settings *settings);
void zdb_shard_free(zdb_shard *sh);

/* Closes and reopens the shard connection with new settings. Returns true
 * on success (the shard keeps its old connection on failure). */
bool zdb_shard_reopen(zdb_shard *sh, const zdb_shard_settings *settings);

bool zdb_shard_put(zdb_shard *sh, const char *id, const char *json_value,
                   long long ttl_seconds);
cJSON *zdb_shard_get(zdb_shard *sh, const char *id);
bool zdb_shard_delete(zdb_shard *sh, const char *id);
char **zdb_shard_ids(zdb_shard *sh, const cJSON *filters,
                     size_t *count_out);
cJSON *zdb_shard_all(zdb_shard *sh, const cJSON *filters);
cJSON *zdb_shard_query(zdb_shard *sh, const cJSON *filters);
bool zdb_shard_cleanup(zdb_shard *sh);

/* stage 5: replication-aware variants (see zesty_engine.h) */
bool zdb_shard_replica_put(zdb_shard *sh, const char *id,
                           const char *json_value, long long ttl_absolute,
                           long long timestamp, const char *origin);
bool zdb_shard_replica_delete(zdb_shard *sh, const char *id,
                              long long timestamp, const char *origin);
cJSON *zdb_shard_get_ts(zdb_shard *sh, const char *id,
                        long long *timestamp_out);
cJSON *zdb_shard_all_ts(zdb_shard *sh, const cJSON *filters);
cJSON *zdb_shard_query_ts(zdb_shard *sh, const cJSON *filters);

/* Free a NULL-terminated list of malloc'd strings. Declared here so both
 * engine translation units see the same signature. */
void zdb_free_strings(char **strings);

#endif
