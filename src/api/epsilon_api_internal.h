/* epsilon_api_internal.h - shared state and helpers across the API
 * handler translation units. Not installed; every file in this module
 * (epsilon_api.c and its splits) includes it.
 */

#ifndef EPSILON_API_INTERNAL_H
#define EPSILON_API_INTERNAL_H

#include "epsilon_api.h"
#include "../engine/epsilon_analytics.h"

/* Handlers receive no user pointer; a single server per process. */
typedef struct {
    edb_engine *engine;
    edb_config *config;
} api_ctx;

extern api_ctx g_ctx;
extern edb_cluster *g_cluster;      /* may be NULL: clustering disabled */
extern edb_repl *g_repl;            /* may be NULL: replication disabled */
extern edb_analytics *g_analytics;  /* may be NULL: analytics disabled */

/* Restore lock: while set, every data read/write endpoint answers 503
 * and replicated changes are refused so a restore can wipe and re-place
 * shard files without clients seeing partial state. */
extern volatile bool g_restore_locked;

/* --- shared helpers (defined in epsilon_api.c) ---------------------- */

long long api_now_us(void);
bool restore_blocked(edb_http_response *res);
void restore_set_locked(bool locked);
void respond_json(edb_http_response *res, int status, cJSON *obj);
void respond_error(edb_http_response *res, int status, const char *message);
bool body_json(const edb_http_request *req, cJSON **out);
bool json_u64_value(const cJSON *item, uint64_t *out);
bool query_param(const edb_http_request *req, const char *name, char *out,
                 size_t cap);
uint64_t authenticate(const edb_http_request *req,
                      const edb_http_request *unused, bool *ok);
bool authorize_partition(edb_config *cfg, const char *database,
                         const char *partition, uint64_t user_groups,
                         edb_permission perm, edb_http_response *res);
bool split_data_path(const char *path, char db[128], char part[256],
                     char ks[128], char id[512]);
bool require_admin_auth(const edb_http_request *req, edb_http_response *res);

/* --- failed-credential throttling (epsilon_api.c) ------------------- */
/* True when the source has failed authentication too many times and is
 * locked out. Trusted/local sockets are never throttled. */
bool edb_auth_throttled(const char *ip);
/* Records a failed credential check for the source. */
void edb_auth_throttle_fail(const char *ip);
/* Clears the source's failure counter (call on successful auth). */
void edb_auth_throttle_reset(const char *ip);

/* --- route handlers (defined across the split files) ---------------- */

/* epsilon_api_data.c: data CRUD */
bool handle_data_put(const edb_http_request *req, edb_http_response *res);
bool handle_data_get(const edb_http_request *req, edb_http_response *res);
bool handle_data_delete(const edb_http_request *req, edb_http_response *res);
bool handle_data_collect(const edb_http_request *req, edb_http_response *res);

/* epsilon_api_admin.c: admin entity CRUD */
bool handle_admin_databases(const edb_http_request *req,
                            edb_http_response *res);
bool handle_admin_groups(const edb_http_request *req, edb_http_response *res);
bool handle_admin_users(const edb_http_request *req, edb_http_response *res);
bool handle_admin_partitions(const edb_http_request *req,
                             edb_http_response *res);
bool handle_partition_maintenance(const edb_http_request *req,
                                  edb_http_response *res);
bool handle_admin_delete(const edb_http_request *req, edb_http_response *res);
bool handle_admin_keyspaces(const edb_http_request *req,
                            edb_http_response *res);
bool handle_admin_analytics(const edb_http_request *req,
                            edb_http_response *res);
bool handle_admin_benchmark(const edb_http_request *req,
                            edb_http_response *res);

/* epsilon_api_cluster.c: cluster view, join/remove, backup/restore */
bool handle_admin_cluster(const edb_http_request *req, edb_http_response *res);
bool handle_backup_manifest(const edb_http_request *req,
                            edb_http_response *res);
bool handle_backup_shard_download(const edb_http_request *req,
                                  edb_http_response *res);
bool handle_restore_lock(const edb_http_request *req, edb_http_response *res);
bool handle_restore_unlock(const edb_http_request *req,
                           edb_http_response *res);
bool handle_restore_wipe(const edb_http_request *req, edb_http_response *res);
bool handle_restore_shard_upload(const edb_http_request *req,
                                 edb_http_response *res);
bool handle_admin_join(const edb_http_request *req, edb_http_response *res);
bool handle_admin_remove_node(const edb_http_request *req,
                              edb_http_response *res);

/* epsilon_api_settings.c: named settings + status */
/* epsilon_api_eql.c: SQL-over-HTTP surface */
bool handle_data_eql(const edb_http_request *req, edb_http_response *res);
bool handle_admin_eql(const edb_http_request *req, edb_http_response *res);

bool handle_settings(const edb_http_request *req, edb_http_response *res);
bool handle_status(const edb_http_request *req, edb_http_response *res);

/* epsilon_api_console.c: admin console auth */
bool session_lookup(const char *token, char username[128], uint64_t *groups);
void session_create(const char *username, uint64_t groups,
                    char token_out[65]);
void session_destroy(const char *token);
bool handle_console_state(const edb_http_request *req, edb_http_response *res);
bool handle_admin_login(const edb_http_request *req, edb_http_response *res);
bool handle_admin_setup(const edb_http_request *req, edb_http_response *res);
bool handle_admin_logout(const edb_http_request *req, edb_http_response *res);

#endif
