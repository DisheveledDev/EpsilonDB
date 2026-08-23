/* zesty_repl.h - stage 5 replication: write fan-out, per-node change
 * caches and quorum reads on top of the stage 4 mesh.
 *
 * Write path (zdb_repl_write):
 *   1. apply the change locally through the registered apply handler,
 *   2. send it as a ZSTP_REPL frame to every other online peer (each
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
 * Read path (zdb_repl_read_*): offered when the database's replication
 * factor > 1. GET/all/ids/query fan out ZSTP_QUERY frames to online
 * peers and merge responses by last-write-wins; only records that a
 * quorum of responding replicas agree on are returned. When no peer
 * responds the local view is served unchanged.
 *
 * All functions are thread-safe.
 */

#ifndef ZESTY_REPL_H
#define ZESTY_REPL_H

#include <stdbool.h>
#include <stddef.h>

#include "../engine/zesty_config.h"
#include "zesty_cluster.h"

typedef struct zdb_repl zdb_repl;

/* Applies a replicated change document:
 *   {"op":"put","db":..,"partition":..,"keyspace":..,"id":..,
 *    "value":{..},"ttl_abs":-1|epoch,"ts":epoch,"filters":["k=v",..]}
 *   {"op":"delete",...same minus value/ttl...}
 * Returns true when the change is current locally afterwards.
 * ud is the user-data pointer passed to zdb_repl_set_handlers. */
typedef bool (*zdb_repl_apply_fn)(void *ud, const cJSON *change);

/* Answers a quorum read request document:
 *   {"q":"get","db":..,"partition":..,"keyspace":..,"id":..}
 *   {"q":"all"|"query"|...,"filters":[..],"fields":[..]}
 * Returned JSON (malloc'd) shape:
 *   get:    {"row":{"id":..,"timestamp":..,"value":{..}}} or {"row":null}
 *   all/query: {"rows":[{"id":..,"timestamp":..,"value":{..}},..]}
 *   ids:    {"ids":["a","b",..]}
 */
typedef cJSON *(*zdb_repl_read_fn)(void *ud, const cJSON *request);

/* Starts the replication service over the given cluster mesh. data_dir
 * is where the persistent change log ("changes.sqlite") lives. Installs
 * a per-cluster dispatcher so inbound REPL/QUERY frames are answered. */
zdb_repl *zdb_repl_start(zdb_cluster *cluster, zdb_config *cfg,
                         const char *data_dir);
void zdb_repl_stop(zdb_repl *rp);

/* Registers the engine-backed handlers for this node. ud is passed
 * through to both callbacks on every invocation. Must be called before
 * the node receives peer REPL/QUERY frames. */
void zdb_repl_set_handlers(zdb_repl *rp, zdb_repl_apply_fn apply,
                           zdb_repl_read_fn read, void *ud);

/* Result of zdb_repl_write. */
typedef enum {
    ZDB_REPL_OK = 0,        /* applied locally + quorum acknowledged */
    ZDB_REPL_LOCAL_FAIL,    /* local application failed */
    ZDB_REPL_QUORUM_LOST    /* fewer than rf/2+1 holders reached */
} zdb_repl_status;

/* Replicates one change document (see zdb_repl_apply_fn for shape).
 * Applies locally, fans out, caches for unreachable peers. blocks until
 * acknowledgements arrive or the (short) fan-out deadline passes. */
zdb_repl_status zdb_repl_write(zdb_repl *rp, const char *db,
                               const char *change_json);

/* Quorum read helpers; return freshly built cJSON results (array/object)
 * or NULL on allocation failure. When clustering/replication does not
 * apply they fall back to the plain local calls via the read handler. */

/* Single-record read: returns the agreed {"id","timestamp","value"}
 * object (caller frees), or NULL when no replica (including local)
 * holds a live record. */
cJSON *zdb_repl_read_get(zdb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const char *id);

/* Merged collection reads over live replicas; shapes match the local
 * engine calls (plain values / id strings). */
cJSON *zdb_repl_read_all(zdb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const char **filters,
                         size_t nfilters);
cJSON *zdb_repl_read_query(zdb_repl *rp, const char *db,
                           const char *partition, const char *keyspace,
                           const char **filters, size_t nfilters,
                           const char **fields, size_t nfields);
char **zdb_repl_read_ids(zdb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const char **filters,
                         size_t nfilters, size_t *count_out);

#endif
