/* epsilon_repl_internal.h - shared state and declarations across the
 * replication module translation units (epsilon_repl.c, the persisted
 * change cache, and the quorum read path). Not installed.
 */

#ifndef EPSILON_REPL_INTERNAL_H
#define EPSILON_REPL_INTERNAL_H

#include <pthread.h>

#include "epsilon_repl.h"
#include "../../vendor/sqlite/sqlite3.h"

#define MAX_PEERS_SNAPSHOT 64
#define REPL_CONNECT_TIMEOUT_MS 1500
#define REPL_FANOUT_DEADLINE_MS 2000
#define REPL_TICK_MS            500

/* persisted sqlite log of changes that could not be acknowledged */
typedef struct {
    sqlite3 *db;
    pthread_mutex_t lock;
} change_cache;

/* one cached change destined for an offline/acking-late peer */
typedef struct {
    char target[EDB_NODE_ID_MAX];
    char cid[96];
    char *payload;   /* malloc'd */
} pending_row;

struct edb_repl {
    edb_cluster *cluster;
    edb_config *cfg;
    edb_engine *cfg_engine;   /* engine behind cfg, for direct calls */

    change_cache cache;

    edb_repl_apply_fn apply;
    edb_repl_read_fn read;
    void *ud;

    char self_id[EDB_NODE_ID_MAX];
    char data_dir[512];
    long long change_seq;

    bool running;
    pthread_t maint_thread;

    /* stage 6c: syncing gate (see edb_repl_set_syncing) */
    pthread_mutex_t sync_lock;
    bool syncing;

    /* per-peer replay bookkeeping, guarded by replay_lock */
    pthread_mutex_t replay_lock;
    char replaying[EDB_NODE_ID_MAX];   /* node currently being drained */
};

/* --- small helpers (epsilon_repl.c) ---------------------------------- */
long long repl_epoch_now(void);
long long repl_mono_ms(void);
void repl_sleep_ms(int ms);
char *json_print(const cJSON *doc);
void repl_set_socket_timeouts(int fd, int ms);
int rpc_once(const char *addr, int port, estp_type send_type,
             const char *payload, estp_type want_type, char **reply_out);
bool ack_ok(const char *reply);
int replication_factor(edb_repl *rp, const char *db);
size_t holder_ids(edb_repl *rp, const char *partition,
                  const char *keyspace, int rf,
                  char (*out)[EDB_NODE_ID_MAX]);
bool id_in_holders(const char *id, char (*holders)[EDB_NODE_ID_MAX],
                   size_t n);
bool find_peer(edb_repl *rp, const char *node_id, edb_peer_info *out);
bool apply_change_local(edb_repl *rp, const cJSON *change);

/* --- change cache (epsilon_repl_cache.c) ----------------------------- */
bool cache_open(change_cache *cc, const char *data_dir);
void cache_close(change_cache *cc);
void cache_append(change_cache *cc, const char *target, const char *cid,
                  const char *payload);
void cache_remove(change_cache *cc, const char *target, const char *cid);
pending_row *cache_load(change_cache *cc, const char *target, size_t limit,
                        size_t *count_out);
void free_rows(pending_row *rows, size_t count);

#endif
