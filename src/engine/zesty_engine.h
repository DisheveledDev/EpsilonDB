/* zesty_engine.h - ZestyDB core shard storage engine.
 *
 * One SQLite database file per (partition, keyspace) pair, filename is
 * md5(partition + keyspace) + ".sqlite". Documents are JSON values.
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

/* Store a JSON document under id in partition/keyspace.
 * ttl_seconds: seconds until expiry, or -1 for no expiry.
 * filters: array of "key=value" strings used for later query filtering,
 *          may be NULL/empty.
 * Returns true on success. */
bool zdb_put(zdb_engine *mgr, const char *partition, const char *keyspace,
             const char *id, const char *json_value, long long ttl_seconds,
             const char **filters, size_t nfilters);

/* Fetch a document as a parsed cJSON value, or NULL if absent/expired. */
cJSON *zdb_get(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char *id);

/* Soft-delete a document. Returns true on success (even if absent). */
bool zdb_delete(zdb_engine *mgr, const char *partition, const char *keyspace,
                const char *id);

/* Return ids of all live documents matching ALL of the given
 * "key=value" filters (md5-hashed and matched via DataFilter). */
char **zdb_ids(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char **filters, size_t nfilters, size_t *count_out);

/* Return all live documents matching the filters as a cJSON array. */
cJSON *zdb_all(zdb_engine *mgr, const char *partition, const char *keyspace,
               const char **filters, size_t nfilters);

/* Return live documents matching the filters whose top-level object also
 * contains every given "field=value" string comparison, as a cJSON array.
 * Filter hashes select candidate rows cheaply; the field/value pairs are
 * then checked against the parsed JSON. */
cJSON *zdb_query(zdb_engine *mgr, const char *partition, const char *keyspace,
                 const char **filters, size_t nfilters,
                 const char **fields, size_t nfields);

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
                     long long timestamp, const char **filters,
                     size_t nfilters);

/* Apply a replicated soft-delete carrying an explicit origin timestamp.
 * LWW semantics as above; ttl becomes timestamp - 5 (tombstone). */
bool zdb_replica_delete(zdb_engine *mgr, const char *partition,
                        const char *keyspace, const char *id,
                        long long timestamp);

/* Like zdb_get but also reports the row's last-modified timestamp
 * (needed for quorum comparison). NULL value => ts untouched. */
cJSON *zdb_get_ts(zdb_engine *mgr, const char *partition,
                  const char *keyspace, const char *id,
                  long long *timestamp_out);

/* Like zdb_all/zdb_query but each element is an object
 * {"id":..,"timestamp":..,"value":{..}} so replicas can be merged by
 * comparing timestamps. */
cJSON *zdb_all_ts(zdb_engine *mgr, const char *partition,
                  const char *keyspace, const char **filters,
                  size_t nfilters);
cJSON *zdb_query_ts(zdb_engine *mgr, const char *partition,
                    const char *keyspace, const char **filters,
                    size_t nfilters, const char **fields, size_t nfields);

/* Free a NULL-terminated list of malloc'd strings. */
void zdb_free_strings(char **strings);

#endif
