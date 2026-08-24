/* zesty_engine.h - ZestyDB core shard storage engine.
 *
 * One SQLite database file per (partition, keyspace) pair, filename is
 * md5(md5(partition) + ":" + md5(keyspace)) + ".sqlite". Documents are JSON values.
 *
 * Semantics inherited from the Switchblade SQLiteShardProvider:
 *   - delete() is a soft delete: ttl is set to now-5 so replicas can
 *     distinguish deletion from expiry during replication.
 *   - a background cleanup pass runs periodically and removes rows expired
 *     more than ZDB_CLEANUP_GRACE_SECONDS ago; the grace window lets nodes
 *     that were offline replay changes they missed when they come back.
 *   - shards accumulating many expirations are marked dirty and re-indexed
 *     (VACUUM) by the cleanup thread.
 *
 * All functions returning a document return a cJSON object/array that the
 * caller must free with cJSON_Delete(). String lists are returned as
 * NULL-terminated arrays of malloc'd strings; release with
 * zdb_free_strings().
 */

#ifndef ZESTY_ENGINE_H
#define ZESTY_ENGINE_H

#include <stdbool.h>
#include <stddef.h>

#include "../sqlite/sqlite3.h"
#include "../../vendor/cjson/cJSON.h"

#define ZDB_CLEANUP_INTERVAL_SECONDS 60
#define ZDB_CLEANUP_GRACE_SECONDS    7200

typedef struct zdb_shard_manager zdb_engine;

/* Open (or create) the shard store rooted at path. The directory is created
 * if missing and any existing "*.sqlite" files are opened. Returns NULL on
 * failure. */
zdb_engine *zdb_engine_open(const char *path);

/* Close all shards and free the manager. */
void zdb_engine_close(zdb_engine *mgr);

/* The data directory backing this shard store (for tools that need to
 * touch shard files directly, e.g. snapshot transfer). */
const char *zdb_engine_path(zdb_engine *mgr);

/* --- stage 6b: shard snapshot support --------------------------------- */

/* Fills path_out with the shard file path for partition/keyspace
 * (the framed partition/keyspace digest under the store root) and key_out
 * with the 32-char hex shard key. Returns false on allocation/argument
 * failure. The file may not exist yet. */
bool zdb_shard_path(zdb_engine *mgr, const char *partition,
                    const char *keyspace, char *path_out, size_t cap,
                    char key_out[33]);

/* Drops the cached open handle for partition/keyspace so the next use
 * reopens the file from disk. Call after a snapshot replaces the shard
 * file underneath the engine; concurrent users block until the close
 * finishes and then reopen cleanly. Returns true when a handle existed.
 * Also runs an integrity check on the new file: returns false when the
 * reopened database fails "PRAGMA integrity_check". */
bool zdb_shard_invalidate(zdb_engine *mgr, const char *partition,
                          const char *keyspace);

/* True when partition/keyspace currently has an open shard handle.
 * Test hook for invalidate semantics. */
bool zdb_shard_is_open(zdb_engine *mgr, const char *partition,
                       const char *keyspace);
bool zdb_shard_validate(zdb_engine *mgr, const char *partition,
                        const char *keyspace);

/* --- stage 6d: shard GC ------------------------------------------------ */

/* Fills keys[] with up to cap 32-char md5 shard keys present on disk in
 * the store root. Returns the number written. */
size_t zdb_engine_shard_keys(zdb_engine *mgr, char (*keys)[33], size_t cap);

/* Removes the shard file identified by a 32-char md5 key (and its cached
 * handle, if open) from disk. Returns true when the file is gone. Used
 * to GC redundant shards after a rebalance moves them to another node. */
bool zdb_shard_gc(zdb_engine *mgr, const char key[33]);

/* Store a JSON document under id in partition/keyspace.
 * ttl_seconds: seconds until expiry, or -1 for no expiry. */
bool zdb_put(zdb_engine *mgr, const char *partition, const char *keyspace,
             const char *id, const char *json_value, long long ttl_seconds);

/* Fetch a document as a parsed cJSON value, or NULL if absent/expired. */
cJSON *zdb_get(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char *id);

/* Soft-delete a document. Returns true on success (even if absent). */
bool zdb_delete(zdb_engine *mgr, const char *partition, const char *keyspace,
                const char *id);

/* Structured filters are one object or an array of objects shaped as
 * {"key":"manager.age","operator":"eq","value":42}. Multiple filters
 * use AND semantics. Supported operators: eq, ne, gt, gte, lt, lte. */
char **zdb_ids(zdb_engine *mgr, const char *partition, const char *keyspace,
               const cJSON *filters, size_t *count_out);
cJSON *zdb_all(zdb_engine *mgr, const char *partition, const char *keyspace,
               const cJSON *filters);
cJSON *zdb_query(zdb_engine *mgr, const char *partition,
                 const char *keyspace, const cJSON *filters);
bool zdb_filters_valid(const cJSON *filters);

/* Force a cleanup + reindex pass over one shard now (mainly for tests). */
bool zdb_force_cleanup(zdb_engine *mgr, const char *partition,
                       const char *keyspace);

/* --- stage 5: replication-aware variants ------------------------------ */

/* Apply a replicated write carrying an explicit origin timestamp.
 * Last-write-wins: the write is skipped when the locally stored row (if
 * any, including soft-deleted rows) already carries a NEWER timestamp.
 * ttl_absolute is an epoch expiry (pass -1 for no expiry). Returns true
 * when the record is now current locally (applied or already newer). */
bool zdb_replica_put(zdb_engine *mgr, const char *partition,
                     const char *keyspace, const char *id,
                     const char *json_value, long long ttl_absolute,
                     long long timestamp);
bool zdb_replica_put_origin(zdb_engine *mgr, const char *partition,
                            const char *keyspace, const char *id,
                            const char *json_value, long long ttl_absolute,
                            long long timestamp, const char *origin);

/* Apply a replicated soft-delete carrying an explicit origin timestamp.
 * LWW semantics as above; ttl becomes timestamp - 5 (tombstone). */
bool zdb_replica_delete(zdb_engine *mgr, const char *partition,
                        const char *keyspace, const char *id,
                        long long timestamp);
bool zdb_replica_delete_origin(zdb_engine *mgr, const char *partition,
                               const char *keyspace, const char *id,
                               long long timestamp, const char *origin);

/* Like zdb_get but also reports the row's last-modified timestamp
 * (needed for quorum comparison). NULL value => ts untouched. */
cJSON *zdb_get_ts(zdb_engine *mgr, const char *partition,
                  const char *keyspace, const char *id,
                  long long *timestamp_out);

/* Like zdb_all/zdb_query but each element is an object
 * {"id":..,"timestamp":..,"value":{..}} so replicas can be merged by
 * comparing timestamps. */
cJSON *zdb_all_ts(zdb_engine *mgr, const char *partition,
                  const char *keyspace, const cJSON *filters);
cJSON *zdb_query_ts(zdb_engine *mgr, const char *partition,
                    const char *keyspace, const cJSON *filters);

/* Free a NULL-terminated list of malloc'd strings. */
void zdb_free_strings(char **strings);

#endif
