/* epsilon_engine.h - EpsilonDB core shard storage engine.
 *
 * One SQLite database file per (partition, keyspace) pair, filename is
 * md5(md5(partition) + ":" + md5(keyspace)) + ".sqlite". Documents are JSON values.
 *
 * Semantics inherited from the Switchblade SQLiteShardProvider:
 *   - delete() is a soft delete: ttl is set to now-5 so replicas can
 *     distinguish deletion from expiry during replication.
 *   - a background cleanup pass runs periodically and removes rows expired
 *     more than EDB_CLEANUP_GRACE_SECONDS ago; the grace window lets nodes
 *     that were offline replay changes they missed when they come back.
 *   - shards accumulating many expirations are marked dirty and re-indexed
 *     (VACUUM) by the cleanup thread.
 *
 * All functions returning a document return a cJSON object/array that the
 * caller must free with cJSON_Delete(). String lists are returned as
 * NULL-terminated arrays of malloc'd strings; release with
 * edb_free_strings().
 */

#ifndef EPSILON_ENGINE_H
#define EPSILON_ENGINE_H

#include <stdbool.h>
#include <stddef.h>

#include "../../vendor/sqlite/sqlite3.h"
#include "../../vendor/cjson/cJSON.h"

#define EDB_CLEANUP_INTERVAL_SECONDS 60
#define EDB_CLEANUP_GRACE_SECONDS    7200

typedef struct edb_shard_manager edb_engine;

/* Open (or create) the shard store rooted at path. The directory is created
 * if missing and any existing "*.sqlite" files are opened. Returns NULL on
 * failure. */
edb_engine *edb_engine_open(const char *path);

/* Close all shards and free the manager. */
void edb_engine_close(edb_engine *mgr);

/* The data directory backing this shard store (for tools that need to
 * touch shard files directly, e.g. snapshot transfer). */
const char *edb_engine_path(edb_engine *mgr);

/* --- shard snapshot support ------------------------------------------- */

/* Fills path_out with the shard file path for partition/keyspace
 * (the framed partition/keyspace digest under the store root) and key_out
 * with the 32-char hex shard key. Returns false on allocation/argument
 * failure. The file may not exist yet. */
bool edb_shard_path(edb_engine *mgr, const char *partition,
                    const char *keyspace, char *path_out, size_t cap,
                    char key_out[33]);

/* Drops the cached open handle for partition/keyspace so the next use
 * reopens the file from disk. Call after a snapshot replaces the shard
 * file underneath the engine; concurrent users block until the close
 * finishes and then reopen cleanly. Returns true when a handle existed.
 * Also runs an integrity check on the new file: returns false when the
 * reopened database fails "PRAGMA integrity_check". */
bool edb_shard_invalidate(edb_engine *mgr, const char *partition,
                          const char *keyspace);

/* True when partition/keyspace currently has an open shard handle.
 * Test hook for invalidate semantics. */
bool edb_shard_is_open(edb_engine *mgr, const char *partition,
                       const char *keyspace);
bool edb_shard_validate(edb_engine *mgr, const char *partition,
                        const char *keyspace);

/* --- shard GC ---------------------------------------------------------- */

/* Fills keys[] with up to cap 32-char md5 shard keys present on disk in
 * the store root. Returns the number written. */
size_t edb_engine_shard_keys(edb_engine *mgr, char (*keys)[33], size_t cap);

/* On-disk size in bytes of the shard file for partition/keyspace
 * (including any -wal sidecar), or 0 when the file does not exist. */
long long edb_engine_shard_size(edb_engine *mgr, const char *partition,
                                const char *keyspace);

/* Removes the shard file identified by a 32-char md5 key (and its cached
 * handle, if open) from disk. Returns true when the file is gone. Used
 * to GC redundant shards after a rebalance moves them to another node. */
bool edb_shard_gc(edb_engine *mgr, const char key[33]);

/* Store a JSON document under id in partition/keyspace.
 * ttl_seconds: seconds until expiry, or -1 for no expiry. */
bool edb_put(edb_engine *mgr, const char *partition, const char *keyspace,
             const char *id, const char *json_value, long long ttl_seconds);

/* Fetch a document as a parsed cJSON value, or NULL if absent/expired. */
cJSON *edb_get(edb_engine *mgr, const char *partition, const char *keyspace,
               const char *id);

/* Soft-delete a document. Returns true on success (even if absent). */
bool edb_delete(edb_engine *mgr, const char *partition, const char *keyspace,
                const char *id);

/* Structured filters are one object or an array of objects shaped as
 * {"key":"manager.age","operator":"eq","value":42}. Multiple filters
 * use AND semantics. Supported operators: eq, ne, gt, gte, lt, lte. */
char **edb_ids(edb_engine *mgr, const char *partition, const char *keyspace,
               const cJSON *filters, size_t *count_out);
cJSON *edb_all(edb_engine *mgr, const char *partition, const char *keyspace,
               const cJSON *filters);
cJSON *edb_query(edb_engine *mgr, const char *partition,
                 const char *keyspace, const cJSON *filters);
bool edb_filters_valid(const cJSON *filters);

/* Force a cleanup + reindex pass over one shard now (mainly for tests). */
bool edb_force_cleanup(edb_engine *mgr, const char *partition,
                       const char *keyspace);

/* --- replication-aware variants --------------------------------------- */

/* Apply a replicated write carrying an explicit origin timestamp.
 * Last-write-wins: the write is skipped when the locally stored row (if
 * any, including soft-deleted rows) already carries a NEWER timestamp.
 * ttl_absolute is an epoch expiry (pass -1 for no expiry). Returns true
 * when the record is now current locally (applied or already newer). */
bool edb_replica_put(edb_engine *mgr, const char *partition,
                     const char *keyspace, const char *id,
                     const char *json_value, long long ttl_absolute,
                     long long timestamp);
bool edb_replica_put_origin(edb_engine *mgr, const char *partition,
                            const char *keyspace, const char *id,
                            const char *json_value, long long ttl_absolute,
                            long long timestamp, const char *origin);

/* Apply a replicated soft-delete carrying an explicit origin timestamp.
 * LWW semantics as above; ttl becomes timestamp - 5 (tombstone). */
bool edb_replica_delete(edb_engine *mgr, const char *partition,
                        const char *keyspace, const char *id,
                        long long timestamp);
bool edb_replica_delete_origin(edb_engine *mgr, const char *partition,
                               const char *keyspace, const char *id,
                               long long timestamp, const char *origin);

/* Like edb_get but also reports the row's last-modified timestamp
 * (needed for quorum comparison). NULL value => ts untouched. */
cJSON *edb_get_ts(edb_engine *mgr, const char *partition,
                  const char *keyspace, const char *id,
                  long long *timestamp_out);

/* Like edb_all/edb_query but each element is an object
 * {"id":..,"timestamp":..,"value":{..}} so replicas can be merged by
 * comparing timestamps. */
cJSON *edb_all_ts(edb_engine *mgr, const char *partition,
                  const char *keyspace, const cJSON *filters);
cJSON *edb_query_ts(edb_engine *mgr, const char *partition,
                    const char *keyspace, const cJSON *filters);

/* Free a NULL-terminated list of malloc'd strings. */
void edb_free_strings(char **strings);

/* Manual maintenance: run VACUUM / REINDEX on every currently open shard
 * belonging to `partition`. Returns the number of shards processed, or -1
 * on bad arguments. */
int edb_engine_vacuum_partition(edb_engine *mgr, const char *partition);
int edb_engine_reindex_partition(edb_engine *mgr, const char *partition);

#endif
