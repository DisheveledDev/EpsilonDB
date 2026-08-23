/* zesty_cluster.h - stage 4 cluster mesh: membership, hash-range
 * placement registry, leader election, and the raw peer socket layer.
 *
 * Design (see AGENTS.md):
 *   - Every node serves a permanent TCP peer port (plaintext). The wire
 *     format is framed JSON: magic "ZSTP", version byte, message type,
 *     4-byte big-endian payload length, payload.
 *   - Membership and the hash-range placement registry live in the
 *     reserved "__system__" database (as settings documents), so they
 *     replicate through the same machinery as user data in later stages.
 *   - Nodes gossip their full state on every heartbeat; merges are
 *     last-write-wins by last_seen for node records, highest generation
 *     wins for the range table.
 *   - The leader is deterministically the lexicographically smallest
 *     online node id. Only the leader recomputes range assignments,
 *     bumping the generation each time.
 *
 * All functions are thread-safe unless noted. zdb_cluster_start spawns
 * the acceptor, connector and maintenance threads; zdb_cluster_stop
 * joins them.
 */

#ifndef ZESTY_CLUSTER_H
#define ZESTY_CLUSTER_H

#include <stdbool.h>
#include <stddef.h>

#include "../engine/zesty_config.h"
#include "zstp_wire.h"

#define ZDB_NODE_ID_MAX   64
#define ZDB_ADDR_MAX      64
#define ZDB_RANGE_HEX     33    /* 32 hex digits + NUL */

typedef struct zdb_cluster zdb_cluster;

typedef struct {
    char id[ZDB_NODE_ID_MAX];
    char addr[ZDB_ADDR_MAX];
    int port;
    long long last_seen;      /* epoch seconds, from gossip */
    bool online;              /* mesh connection alive */
} zdb_peer_info;

typedef struct {
    char node_id[ZDB_NODE_ID_MAX];
    char start[ZDB_RANGE_HEX];   /* inclusive md5 hex lower bound */
    char end[ZDB_RANGE_HEX];     /* exclusive upper bound */
} zdb_range_info;

/* Starts the cluster service: binds the peer port, restores persisted
 * membership/ranges from the config store, launches background threads.
 * advertise_addr is what remote peers should dial back to reach us.
 * Generates a stable node id derived from addr:port into node_id_out.
 * Returns NULL on failure (bind error, bad args). */
zdb_cluster *zdb_cluster_start(zdb_config *cfg, const char *advertise_addr,
                               int peer_port, char node_id_out[ZDB_NODE_ID_MAX]);

void zdb_cluster_stop(zdb_cluster *cl);

/* Snapshot of the current membership view (including this node). */
size_t zdb_cluster_peers(zdb_cluster *cl, zdb_peer_info *out, size_t cap);

/* Current leader's node id, or NULL when unknown. */
const char *zdb_cluster_leader(zdb_cluster *cl);

/* This node's own id. */
const char *zdb_cluster_self_id(zdb_cluster *cl);

bool zdb_cluster_is_leader(zdb_cluster *cl);

/* Range-table generation (0 = unassigned). */
long long zdb_cluster_generation(zdb_cluster *cl);

/* Snapshot of range assignments. */
size_t zdb_cluster_ranges(zdb_cluster *cl, zdb_range_info *out, size_t cap);

/* Snapshot of the TARGET range assignments (stage 6 rebalancing), or 0
 * when no rebalance is pending. Target generation > 0 means the cluster
 * is mid-rebalance: nodes must converge on the target before it is
 * promoted to live. */
size_t zdb_cluster_target_ranges(zdb_cluster *cl, zdb_range_info *out,
                                 size_t cap);
long long zdb_cluster_target_generation(zdb_cluster *cl);

/* Node owning a shard key given its md5(partition+keyspace) hex, or NULL
 * if no assignment covers it. */
const char *zdb_cluster_owner(zdb_cluster *cl, const char *md5hex);

/* Owner of the key under the TARGET table (NULL when no target pending). */
const char *zdb_cluster_target_owner(zdb_cluster *cl, const char *md5hex);

/* Contacts seed_addr:seed_port, exchanges membership so both sides join
 * the same mesh, then returns. The persistent mesh connection is
 * re-established by the background maintainer afterwards. Returns 0 on
 * success, -1 when the seed is unreachable, -2 when the seed refused
 * because a rebalance wave is pending (one join at a time; retry after
 * the wave completes). Blocks up to a few seconds. */
int zdb_cluster_join(zdb_cluster *cl, const char *seed_addr, int seed_port);

/* Installs a per-cluster dispatcher for inbound REPL/QUERY frames.
 * Overrides the process-wide fallback for this cluster's connections;
 * pass fn=NULL to fall back to the global dispatcher again. */
void zdb_cluster_set_dispatcher(zdb_cluster *cl, zstp_dispatch_fn fn,
                                void *ctx);

/* --- stage 6: rebalancing support ------------------------------------- */

/* Leader-only: computes a TARGET range table over the current online
 * membership (generation = live generation + 1) when the online member
 * set differs from the live owners. Does nothing while a target is
 * already pending. Returns true when a new target was published. */
bool zdb_cluster_publish_target(zdb_cluster *cl);

/* Leader-only: promotes the pending target to live (persisted +
 * gossiped), clears per-node compliance flags and releases the
 * rebalance lock. Returns true on promotion, false when no target is
 * pending or not all online nodes have reported compliance. */
bool zdb_cluster_promote_target(zdb_cluster *cl);

/* True when every online node (including self) has set its compliance
 * flag for the pending target. */
bool zdb_cluster_target_compliant(zdb_cluster *cl);

/* Marks this node compliant with the pending target generation. */
void zdb_cluster_mark_compliant(zdb_cluster *cl);

/* Acquires the global rebalance lock for the leader if free (or stale).
 * Returns false when another leader holds it. */
bool zdb_cluster_acquire_rebalance_lock(zdb_cluster *cl);

/* Releases the global rebalance lock (leader, after promotion). */
void zdb_cluster_release_rebalance_lock(zdb_cluster *cl);

#endif
