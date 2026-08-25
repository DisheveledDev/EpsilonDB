/* zesty_api.h - REST API handlers wiring the HTTP layer to the engine and
 * config store. */

#ifndef ZESTY_API_H
#define ZESTY_API_H

#include <stdbool.h>

#include "../engine/zesty_config.h"
#include "../httpd/zesty_http.h"
#include "../socket/zesty_cluster.h"
#include "../socket/zesty_repl.h"

typedef struct {
    zdb_engine *engine;
    zdb_config *config;
} zdb_api;

/* Registers all routes on the server:
 *   Data:  PUT/GET/DELETE /data/{db}/{partition}/{keyspace}/{id}
 *          POST /data/{db}/{partition}/{keyspace}/query|all|ids
 *   Admin: GET|POST|DELETE /admin/{databases|groups|users|partitions}[...]
 *          GET /admin/keyspaces, GET /admin/cluster, POST /admin/join
 *   Ops:   GET /status, POST /admin/auth (token -> group bitmask)
 *
 * Returns false if route registration fails. */
bool zdb_api_register(zdb_http_server *srv, zdb_engine *engine,
                      zdb_config *config);

/* Attaches the cluster service (may be NULL = clustering disabled).
 * Call before serving traffic; zestyd calls this on startup/shutdown. */
void zdb_api_set_cluster(zdb_cluster *cluster);

/* Attaches the replication service (may be NULL = replication
 * disabled). When set, data writes fan out to peers and reads compare
 * quorum copies. */
void zdb_api_set_repl(zdb_repl *repl);

/* Starts (or stops) the workload/performance analytics recorder. `node_id`
 * identifies this node's snapshot in the cluster-wide analytics store. */
void zdb_api_analytics_start(zdb_config *cfg, const char *node_id);
void zdb_api_analytics_stop(void);

#endif
