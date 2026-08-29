/* epsilon_cluster_internal.h - shared state and declarations across the
 * cluster module translation units (epsilon_cluster.c, estp_wire.c,
 * epsilon_cluster_rebalance.c). Not installed.
 */

#ifndef EPSILON_CLUSTER_INTERNAL_H
#define EPSILON_CLUSTER_INTERNAL_H

#include <pthread.h>

#include "epsilon_cluster.h"

#define HEARTBEAT_SECONDS     3
#define OFFLINE_AFTER_SECONDS 12
#define REMOVE_AFTER_SECONDS  7200   /* auto-remove a node offline this long */
#define MAINTAINER_TICK_MS    500
#define MAX_PEERS             64
#define MAX_CONN_THREADS      128
#define PEER_IO_TIMEOUT_MS    15000
#define PEER_CONNECT_TIMEOUT_MS 1500

/* ---- persisted settings keys ---- */
#define SETTING_MEMBERS "cluster.members"
#define SETTING_RANGES  "cluster.ranges"
#define SETTING_TARGET  "cluster.target_ranges"
#define SETTING_LOCK    "cluster.rebalance_lock"
#define SETTING_DONE_PREFIX "rebalance.done."

typedef struct peer_conn {
    int fd;
    bool outbound;                  /* we dialled them */
    char node_id[EDB_NODE_ID_MAX];  /* known after HELLO exchange */
    long long last_recv;
    struct edb_cluster *owner;
    struct peer_conn *next;
    /* lifecycle: the reader thread holds one reference. A thread that
     * wants to write on this socket takes another reference under
     * cl->lock first, so teardown can never free/close a connection
     * while a sender is inside a blocking write on it. */
    int refs;
    bool dead;
} peer_conn;

struct edb_cluster {
    edb_config *cfg;

    char self_id[EDB_NODE_ID_MAX];
    char self_addr[EDB_ADDR_MAX];
    int self_port;
    int self_http_port;      /* client-facing HTTP port (0 = unknown) */

    int listen_fd;

    pthread_mutex_t lock;           /* guards everything below */
    pthread_mutex_t send_lock;      /* serialises frame writes */
    pthread_cond_t conn_cv;         /* signalled when refs hit zero */
    size_t nconn_threads;           /* live reader threads (for stop) */
    peer_conn *pending_conns;
    peer_conn *conns;

    edb_peer_info *peers;           /* includes self */
    size_t npeers;
    size_t peers_cap;

    long long generation;
    long long gc_after;
    edb_range_info *ranges;
    size_t nranges;
    size_t ranges_cap;

    /* pending target structure (generation 0 = none pending) */
    long long target_generation;
    edb_range_info *target_ranges;
    size_t ntarget_ranges;
    size_t target_ranges_cap;

    /* when true (default), the maintainer marks this node
     * compliant as soon as it holds everything the target assigns to
     * it. Disabled around a join's snapshot/catch-up phase so a fresh
     * node cannot mark itself compliant before its data lands. */
    bool auto_compliant;

    char leader[EDB_NODE_ID_MAX];

    estp_dispatch_fn dispatch;      /* per-cluster REPL/QUERY handler */
    void *dispatch_ctx;
    size_t dispatch_inflight;

    bool running;
    pthread_t acceptor_thread;
    pthread_t maintainer_thread;
};

/* --- small helpers (estp_wire.c) ------------------------------------- */
long long epoch_now(void);
void sleep_ms(int ms);
void set_socket_timeouts(int fd, int ms);
int write_full(int fd, const void *buf, size_t len);
int read_full(int fd, void *buf, size_t len);
int estp_send(int fd, estp_type type, const char *json,
              pthread_mutex_t *send_lock);
int estp_recv(int fd, char **payload_out);

/* global REPL/QUERY dispatcher (estp_wire.c); conn_thread falls back to
 * it when the cluster has no per-cluster handler */
extern estp_dispatch_fn g_dispatcher;
extern void *g_dispatcher_ctx;
extern pthread_mutex_t g_dispatch_lock;
extern pthread_cond_t g_dispatch_done;
extern size_t g_dispatch_inflight;

/* --- shared state helpers (epsilon_cluster.c) ------------------------ */
bool leader_is_self_locked(const edb_cluster *cl);
bool is_online_view(const edb_cluster *cl, const char *id);
size_t build_slices(const char **ids, size_t n, edb_range_info *out,
                    size_t cap);
void persist_state(edb_cluster *cl);
void restore_state(edb_cluster *cl);
void gossip_state(edb_cluster *cl);
void set_tcp_nodelay(int fd);
int dial_peer(const char *addr, int port);
bool publish_target_locked(edb_cluster *cl);
void shrink_live_locked(edb_cluster *cl);

#endif
