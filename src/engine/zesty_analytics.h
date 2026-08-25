/* zesty_analytics.h - per-node workload and performance analytics.
 *
 * Each node maintains an in-memory set of counters (reads/writes/updates/
 * deletes per partition/keyspace, plus slow-query latency per
 * partition/keyspace/filter-key) and periodically flushes them as a single
 * JSON document into the reserved __system__ database (keyspace
 * "config_analytics", id = node id). Because those records flow through the
 * same replication machinery as the rest of the config store, every node in
 * the cluster ends up with a copy of every node's snapshot, so any node can
 * produce the full cluster picture via zdb_analytics_report().
 *
 * Snapshots are rewritten every ZDB_ANALYTICS_FLUSH_SECS and carry a
 * ZDB_ANALYTICS_TTL_SECS TTL so a node that goes away stops contributing
 * after 30 minutes without leaving unbounded data behind.
 */

#ifndef ZESTY_ANALYTICS_H
#define ZESTY_ANALYTICS_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "zesty_config.h"

#define ZDB_ANALYTICS_KEYSPACE    "config_analytics"
#define ZDB_ANALYTICS_FLUSH_SECS  10
#define ZDB_ANALYTICS_TTL_SECS    1800

typedef struct zdb_analytics zdb_analytics;

/* Flush callback: persist this node's snapshot. `json` is the serialized
 * snapshot object, `ttl_abs` is the absolute epoch expiry. Implementors
 * route this through the replication service (or a direct write when
 * clustering is disabled). Returns true on success. */
typedef bool (*zdb_analytics_flush_fn)(void *ctx, const char *node_id,
                                       const char *json, long long ttl_abs);

/* Optional cluster-metrics callback: returns a JSON object (or NULL) that is
 * merged into each node's snapshot under the "cluster" key. Used for metrics
 * that live outside the analytics module, e.g. replication backlog. */
typedef cJSON *(*zdb_analytics_cluster_fn)(void *ctx);

/* Starts the analytics recorder and its periodic flush thread. `node_id`
 * identifies this node's snapshot record; an empty value falls back to
 * "local". Returns NULL if the flush thread cannot be created. */
zdb_analytics *zdb_analytics_start(zdb_config *cfg, const char *node_id,
                                   zdb_analytics_flush_fn flush, void *ctx);

/* Registers the optional cluster-metrics callback (see
 * zdb_analytics_cluster_fn). Call before/after start; safe on NULL. */
void zdb_analytics_set_cluster_metrics(zdb_analytics *a,
                                       zdb_analytics_cluster_fn fn, void *ctx);

/* Stops the flush thread and frees the recorder. Safe to call with NULL. */
void zdb_analytics_stop(zdb_analytics *a);

/* --- recording (thread-safe, non-blocking) --------------------------- */

/* A point read (GET). */
void zdb_analytics_record_read(zdb_analytics *a, const char *partition,
                               const char *keyspace, long long latency_us);

/* A write (PUT). `update` distinguishes an overwrite from a create. */
void zdb_analytics_record_write(zdb_analytics *a, const char *partition,
                                const char *keyspace, bool update,
                                long long latency_us);

/* A delete (DELETE). */
void zdb_analytics_record_delete(zdb_analytics *a, const char *partition,
                                 const char *keyspace, long long latency_us);

/* A collection read (all/query/ids). `filter_keys`/`nkeys` are the JSON
 * filter property names used (values are deliberately not recorded). */
void zdb_analytics_record_query(zdb_analytics *a, const char *partition,
                                const char *keyspace,
                                const char *const *filter_keys, size_t nkeys,
                                long long latency_us);

/* --- reporting ------------------------------------------------------- */

/* Aggregates every node's latest snapshot into a report object:
 *   { "nodes": [..], "shards": [ {partition,keyspace,reads,writes,updates,
 *     deletes,read_us,write_us}, .. ], "slow": [ {kind,partition,keyspace,
 *     filter,count,total_us,max_us}, .. ], "cluster": {pending_changes} }
 * Caller frees with cJSON_Delete. */
cJSON *zdb_analytics_report(zdb_analytics *a);

#endif
