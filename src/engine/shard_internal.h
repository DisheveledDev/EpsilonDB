/* Internal structures shared between manager.c and shard.c. */

#ifndef EDB_SHARD_INTERNAL_H
#define EDB_SHARD_INTERNAL_H

#include <pthread.h>
#include <stdbool.h>

#include "epsilon_engine.h"

#define EDB_STMT_CACHE_SIZE 8
#define EDB_VACUUM_THRESHOLD 10000
typedef struct {
    char sql[192];
    sqlite3_stmt *stmt;
} edb_cached_stmt;

typedef struct edb_shard {
    char *path;
    char key[33];              /* framed partition/keyspace digest */
    char partition[256];       /* owning partition ("" until lazily known) */
    char keyspace[128];
    sqlite3 *db;
    pthread_mutex_t lock;
    size_t refs;
    bool retired;

    edb_cached_stmt cache[EDB_STMT_CACHE_SIZE];
    int cache_count;

    long long expired_since_vacuum;
    edb_shard_settings settings;
    long long last_vacuum_ts;
    long long last_reindex_ts;
    long long last_cache_ts;   /* last Auto Cache re-evaluation */
    long long applied_cache_kb;/* page-cache size currently in force */
} edb_shard;

edb_shard *edb_shard_open(const char *path, const char *key,
                          const char *partition, const char *keyspace,
                          const edb_shard_settings *settings);
void edb_shard_free(edb_shard *sh);

/* Closes and reopens the shard connection with new settings. Returns true
 * on success (the shard keeps its old connection on failure). */
bool edb_shard_reopen(edb_shard *sh, const edb_shard_settings *settings);

bool edb_shard_put(edb_shard *sh, const char *id, const char *json_value,
                   long long ttl_seconds);
cJSON *edb_shard_get(edb_shard *sh, const char *id);
bool edb_shard_delete(edb_shard *sh, const char *id);
char **edb_shard_ids(edb_shard *sh, const cJSON *filters,
                     size_t *count_out);
cJSON *edb_shard_all(edb_shard *sh, const cJSON *filters);
cJSON *edb_shard_query(edb_shard *sh, const cJSON *filters);
bool edb_shard_cleanup(edb_shard *sh);

/* stage 5: replication-aware variants (see epsilon_engine.h) */
bool edb_shard_replica_put(edb_shard *sh, const char *id,
                           const char *json_value, long long ttl_absolute,
                           long long timestamp, const char *origin);
bool edb_shard_replica_delete(edb_shard *sh, const char *id,
                              long long timestamp, const char *origin);
cJSON *edb_shard_get_ts(edb_shard *sh, const char *id,
                        long long *timestamp_out);
cJSON *edb_shard_all_ts(edb_shard *sh, const cJSON *filters);
cJSON *edb_shard_query_ts(edb_shard *sh, const cJSON *filters);

/* Free a NULL-terminated list of malloc'd strings. Declared here so both
 * engine translation units see the same signature. */
void edb_free_strings(char **strings);

#endif
