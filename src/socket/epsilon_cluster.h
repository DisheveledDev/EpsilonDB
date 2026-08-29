/* epsilon_cluster.h - cluster mesh: membership, hash-range
 * placement registry, leader election, and the raw peer socket layer.
 *
 * Design (see AGENTS.md):
 *   - Every node serves a permanent TCP peer port (plaintext). The wire
 *     format is framed JSON: magic "ESTP", version byte, message type,
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
 * All functions are thread-safe unless noted. edb_cluster_start spawns
 * the acceptor, connector and maintenance threads; edb_cluster_stop
 * joins them.
 */

#ifndef EPSILON_CLUSTER_H
#define EPSILON_CLUSTER_H

#include <stdbool.h>
#include <stddef.h>

#include "../engine/epsilon_config.h"
#include "estp_wire.h"

#define EDB_NODE_ID_MAX   64
#define EDB_ADDR_MAX      64
#define EDB_RANGE_HEX     33    /* 32 hex digits + NUL */

typedef struct edb_cluster edb_cluster;

typedef struct {
    char id[EDB_NODE_ID_MAX];
    char addr[EDB_ADDR_MAX];
    int port;                 /* mesh peer port */
    int http_port;            /* client-facing HTTP port (0 = unknown) */
    long long last_seen;      /* epoch seconds, from gossip */
    bool online;              /* mesh connection alive */
    bool removed;             /* tombstoned: permanently out of the cluster
                               * until explicitly re-joined */
    long long compliant_gen;  /* highest target gen this node
                               * has reported compliance for (0 = none) */
} edb_peer_info;

typedef struct {
    char node_id[EDB_NODE_ID_MAX];
    char start[EDB_RANGE_HEX];   /* inclusive md5 hex lower bound */
    char end[EDB_RANGE_HEX];     /* exclusive upper bound */
} edb_range_info;

/* Starts the cluster service: binds the peer port, restores persisted
 * membership/ranges from the config store, launches background threads.
 * advertise_addr is what remote peers should dial back to reach us.
 * Generates a stable node id derived from addr:port into node_id_out.
 * Returns NULL on failure (bind error, bad args). */
edb_cluster *edb_cluster_start(edb_config *cfg, const char *advertise_addr,
                               int peer_port, char node_id_out[EDB_NODE_ID_MAX]);

void edb_cluster_stop(edb_cluster *cl);

/* Records this node's client-facing HTTP port so it can be gossiped to
 * peers (the mesh itself only knows peer ports). 0 clears it. */
void edb_cluster_set_http_port(edb_cluster *cl, int http_port);

/* Snapshot of the current membership view (including this node). */
size_t edb_cluster_peers(edb_cluster *cl, edb_peer_info *out, size_t cap);

/* Current leader's node id, or NULL when unknown. */
const char *edb_cluster_leader(edb_cluster *cl);

/* This node's own id. */
const char *edb_cluster_self_id(edb_cluster *cl);

bool edb_cluster_is_leader(edb_cluster *cl);

/* Range-table generation (0 = unassigned). */
long long edb_cluster_generation(edb_cluster *cl);

/* Snapshot of range assignments. */
size_t edb_cluster_ranges(edb_cluster *cl, edb_range_info *out, size_t cap);

/* Snapshot of the TARGET range assignments (rebalancing), or 0
 * when no rebalance is pending. Target generation > 0 means the cluster
 * is mid-rebalance: nodes must converge on the target before it is
 * promoted to live. */
size_t edb_cluster_target_ranges(edb_cluster *cl, edb_range_info *out,
                                 size_t cap);
long long edb_cluster_target_generation(edb_cluster *cl);

/* Node owning a shard key given its framed shard digest, or NULL
 * if no assignment covers it. */
const char *edb_cluster_owner(edb_cluster *cl, const char *md5hex);

/* Owner of the key under the TARGET table (NULL when no target pending). */
const char *edb_cluster_target_owner(edb_cluster *cl, const char *md5hex);
size_t edb_cluster_holders(edb_cluster *cl, const char *md5hex,
                           char (*node_ids)[EDB_NODE_ID_MAX], size_t cap);

/* Contacts seed_addr:seed_port, exchanges membership so both sides join
 * the same mesh, then returns. The persistent mesh connection is
 * re-established by the background maintainer afterwards. Returns 0 on
 * success, -1 when the seed is unreachable, -2 when the seed refused
 * because a rebalance wave is pending (one join at a time; retry after
 * the wave completes). Blocks up to a few seconds. */
int edb_cluster_join(edb_cluster *cl, const char *seed_addr, int seed_port);

/* Marks a peer as removed (tombstoned) so it is excluded from placement
 * and refused on reconnect. If this node is the leader, the live structure
 * is immediately re-sharded over the remaining online members. Returns
 * true when the peer existed. Safe to call on any node; the leader reacts
 * to the gossiped tombstone otherwise. */
bool edb_cluster_remove_node(edb_cluster *cl, const char *node_id);

/* Installs a per-cluster dispatcher for inbound REPL/QUERY frames.
 * Overrides the process-wide fallback for this cluster's connections;
 * pass fn=NULL to fall back to the global dispatcher again. */
void edb_cluster_set_dispatcher(edb_cluster *cl, estp_dispatch_fn fn,
                                void *ctx);

/* --- rebalancing support ---------------------------------------------- */

/* Leader-only: computes a TARGET range table over the current online
 * membership (generation = live generation + 1) when the online member
 * set differs from the live owners. Does nothing while a target is
 * already pending. Returns true when a new target was published. */
bool edb_cluster_publish_target(edb_cluster *cl);

/* Leader-only: promotes the pending target to live (persisted +
 * gossiped), clears per-node compliance flags and releases the
 * rebalance lock. Returns true on promotion, false when no target is
 * pending or not all online nodes have reported compliance. */
bool edb_cluster_promote_target(edb_cluster *cl);

/* True when every online node (including self) has set its compliance
 * flag for the pending target. */
bool edb_cluster_target_compliant(edb_cluster *cl);

/* Marks this node compliant with the pending target generation. */
void edb_cluster_mark_compliant(edb_cluster *cl);

/* Acquires the global rebalance lock for the leader if free (or stale).
 * Returns false when another leader holds it. */
bool edb_cluster_acquire_rebalance_lock(edb_cluster *cl);

/* Releases the global rebalance lock (leader, after promotion). */
void edb_cluster_release_rebalance_lock(edb_cluster *cl);

/* --- promotion trigger + shard GC ------------------------------------- */

/* Leader-only: promote the pending target if every online node reports
 * compliance. Returns true when a promotion happened. */
bool edb_cluster_maybe_promote(edb_cluster *cl);

/* Remove one redundant local shard (no longer owned under the live
 * table) after confirming the new owner; reserved system shards are
 * never removed. Returns the number GC'd (0 or 1). */
size_t edb_cluster_gc_redundant(edb_cluster *cl);

/* --- end-to-end rebalance wiring -------------------------------------- */

/* True when this node still lacks data for a non-system shard the
 * pending target assigns to it (target owner is self, live owner is
 * not, and no local shard file exists yet). Always false when no wave
 * is pending. */
bool edb_cluster_needs_sync(edb_cluster *cl);

/* Enables/disables maintainer auto-compliance (default enabled).
 * Disabled while a join's snapshot/catch-up runs so a fresh node does
 * not report compliance before its data has landed. */
void edb_cluster_set_auto_compliant(edb_cluster *cl, bool enabled);

/* Leader-only: discard the pending target wave and release the
 * rebalance lock, returning the cluster to the live structure.
 * Returns true when a pending wave was actually voided. */
bool edb_cluster_void_target(edb_cluster *cl);

/* Ask the leader to void the pending target wave (ESTP_VOID over an
 * ephemeral connection). Returns true when the leader acknowledged a
 * void. Used by a joiner whose local catch-up failed so the cluster
 * rolls back to the live structure instead of waiting for promotion. */
bool edb_cluster_request_void(edb_cluster *cl);

/* --- mesh encryption key management ----------------------------------- */

/* Derives the 32-byte encryption and MAC keys from the cluster join
 * secret (HKDF-SHA256). Returns 0 on success, -1 on a bad argument. */
int edb_cluster_derive_keys(const char *secret, uint8_t enc_key[32],
                            uint8_t mac_key[32]);

/* Persist/load the mesh keys to/from data_dir/mesh.key (mode 0600). */
bool edb_cluster_persist_keys(const char *data_dir, const uint8_t enc_key[32],
                              const uint8_t mac_key[32]);
bool edb_cluster_load_keys(const char *data_dir, uint8_t enc_key[32],
                           uint8_t mac_key[32]);

#endif
