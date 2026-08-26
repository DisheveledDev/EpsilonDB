/* epsilon_repl.h - stage 5 replication: write fan-out, per-node change
 * caches and quorum reads on top of the stage 4 mesh.
 *
 * Write path (edb_repl_write):
 *   1. apply the change locally through the registered apply handler,
 *   2. send it as a ESTP_REPL frame to every other online peer (each
 *      opens its own short-lived connection; LWW on the receiving side
 *      makes retries idempotent),
 *   3. cache the change persistently for every peer that did not
 *      acknowledge (offline or timed out) in a local sqlite change log,
 *   4. require a quorum: writes are REJECTED when fewer than
 *      rf/2+1 nodes total (including this one) hold the change.
 *      A background thread replays cached changes to peers when they
 *      come back online, so an unavailable quorum is temporary but
 *      still refuses the client write (no accept-and-queue).
 *
 * Read path (edb_repl_read_*): offered when the database's replication
 * factor > 1. GET/all/ids/query fan out ESTP_QUERY frames to online
 * peers and merge responses by last-write-wins; only records that a
 * quorum of responding replicas agree on are returned. When no peer
 * responds the local view is served unchanged.
 *
 * All functions are thread-safe.
 */

#ifndef EPSILON_REPL_H
#define EPSILON_REPL_H

#include <stdbool.h>
#include <stddef.h>

#include "../engine/epsilon_config.h"
#include "epsilon_cluster.h"

typedef struct edb_repl edb_repl;

/* Applies a replicated change document:
 *   {"op":"put","db":..,"partition":..,"keyspace":..,"id":..,
 *    "value":{..},"ttl_abs":-1|epoch,"ts":epoch,"origin":node_id}
 *   {"op":"delete",...same minus value/ttl...}
 * Returns true when the change is current locally afterwards.
 * ud is the user-data pointer passed to edb_repl_set_handlers. */
typedef bool (*edb_repl_apply_fn)(void *ud, const cJSON *change);

/* Answers a quorum read request document:
 *   {"q":"get","db":..,"partition":..,"keyspace":..,"id":..}
 *   {"q":"all_ts"|"query_ts"|"ids",...,"filters":[{..},..]}
 * Returned JSON (malloc'd) shape:
 *   get:    {"row":{"id":..,"timestamp":..,"value":{..}}} or {"row":null}
 *   all/query: {"rows":[{"id":..,"timestamp":..,"value":{..}},..]}
 *   ids:    {"ids":["a","b",..]}
 */
typedef cJSON *(*edb_repl_read_fn)(void *ud, const cJSON *request);

/* Starts the replication service over the given cluster mesh. data_dir
 * is where the persistent change log ("changes.sqlite") lives. Installs
 * a per-cluster dispatcher so inbound REPL/QUERY frames are answered. */
edb_repl *edb_repl_start(edb_cluster *cluster, edb_config *cfg,
                         const char *data_dir);
void edb_repl_stop(edb_repl *rp);

/* Registers the engine-backed handlers for this node. ud is passed
 * through to both callbacks on every invocation. Must be called before
 * the node receives peer REPL/QUERY frames. */
void edb_repl_set_handlers(edb_repl *rp, edb_repl_apply_fn apply,
                           edb_repl_read_fn read, void *ud);

/* Result of edb_repl_write. */
typedef enum {
    EDB_REPL_OK = 0,        /* applied locally + quorum acknowledged */
    EDB_REPL_LOCAL_FAIL,    /* local application failed */
    EDB_REPL_QUORUM_LOST    /* fewer than rf/2+1 holders reached */
} edb_repl_status;

/* Replicates one change document (see edb_repl_apply_fn for shape).
 * Applies locally, fans out, caches for unreachable peers. blocks until
 * acknowledgements arrive or the (short) fan-out deadline passes. */
edb_repl_status edb_repl_write(edb_repl *rp, const char *db,
                               const char *change_json);

/* Quorum read helpers; return freshly built cJSON results (array/object)
 * or NULL on allocation failure. When clustering/replication does not
 * apply they fall back to the plain local calls via the read handler. */

/* Single-record read: returns the agreed {"id","timestamp","value"}
 * object (caller frees), or NULL when no replica (including local)
 * holds a live record. */
cJSON *edb_repl_read_get(edb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const char *id);

/* Merged collection reads over live replicas; shapes match the local
 * engine calls (plain values / id strings). */
cJSON *edb_repl_read_all(edb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const cJSON *filters);
cJSON *edb_repl_read_query(edb_repl *rp, const char *db,
                           const char *partition, const char *keyspace,
                           const cJSON *filters);

/* Like edb_repl_read_query but each row carries {"id","timestamp",
 * "value"} so a partition-wide query can reorder merged keyspaces. */
cJSON *edb_repl_read_query_meta(edb_repl *rp, const char *db,
                                const char *partition, const char *keyspace,
                                const cJSON *filters);
char **edb_repl_read_ids(edb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const cJSON *filters,
                         size_t *count_out);

/* --- stage 6c: delta catch-up ---------------------------------------- */

/* Puts this node into (or out of) the "syncing" state. While syncing,
 * inbound REPL frames are answered ok:false (rather than applied) so
 * writers cache those changes in their local change log for us instead;
 * the deltas are replayed after the shard snapshot lands, guaranteeing
 * the snapshot overwrite never clobbers an already-applied change. */
void edb_repl_set_syncing(edb_repl *rp, bool syncing);

/* Number of cached changes this node still owes `node_id`. */
size_t edb_repl_pending_for(edb_repl *rp, const char *node_id);

/* Total number of cached changes this node still owes across all peers
 * (replication backlog). Used for cluster health/analytics reporting. */
size_t edb_repl_pending_total(edb_repl *rp);

/* Force-drain this node's cached changes destined for `node_id` right
 * now (blocking, single-flight). Returns the number of changes still
 * queued afterwards (0 = fully caught up). Safe to call from any thread;
 * used by the FLUSH handler and by edb_repl_catchup. */
size_t edb_repl_drain_peer(edb_repl *rp, const char *node_id);

/* Flush every online peer's cached-change queue for this node, looping
 * until all report empty (or ~20s deadline). Returns true when fully
 * caught up. Fine-grained counterpart to edb_repl_catchup for callers
 * (tests, 6e) that need to interleave writes with the snapshot. */
bool edb_repl_flush(edb_repl *rp);

/* Catch up one shard on this node: take a snapshot of partition/keyspace
 * from owner_addr:owner_port, invalidate the local handle, then flush
 * every online peer's change cache for us until all queues are empty.
 * Returns true when the snapshot landed and all cached deltas for this
 * shard's peers have been drained (data is now at least as current as a
 * quorum). Sets syncing around the snapshot transfer so concurrent
 * writes are cached and replayed, never lost. */
bool edb_repl_catchup(edb_repl *rp, const char *owner_addr, int owner_port,
                      const char *partition, const char *keyspace);
bool edb_repl_catchup_required(edb_repl *rp, const char *owner_addr,
                               int owner_port, const char *partition,
                               const char *keyspace);

#endif
