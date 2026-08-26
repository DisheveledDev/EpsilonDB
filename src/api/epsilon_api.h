/* epsilon_api.h - REST API handlers wiring the HTTP layer to the engine and
 * config store. */

#ifndef EPSILON_API_H
#define EPSILON_API_H

#include <stdbool.h>

#include "../engine/epsilon_config.h"
#include "../httpd/epsilon_http.h"
#include "../socket/epsilon_cluster.h"
#include "../socket/epsilon_repl.h"

typedef struct {
    edb_engine *engine;
    edb_config *config;
} edb_api;

/* Registers all routes on the server:
 *   Data:  PUT/GET/DELETE /data/{db}/{partition}/{keyspace}/{id}
 *          POST /data/{db}/{partition}/{keyspace}/query|all|ids
 *   Admin: GET|POST|DELETE /admin/{databases|groups|users|partitions}[...]
 *          GET /admin/keyspaces, GET /admin/cluster, POST /admin/join
 *   Ops:   GET /status, POST /admin/auth (token -> group bitmask)
 *
 * Returns false if route registration fails. */
bool edb_api_register(edb_http_server *srv, edb_engine *engine,
                      edb_config *config);

/* Attaches the cluster service (may be NULL = clustering disabled).
 * Call before serving traffic; epsilond calls this on startup/shutdown. */
void edb_api_set_cluster(edb_cluster *cluster);

/* Attaches the replication service (may be NULL = replication
 * disabled). When set, data writes fan out to peers and reads compare
 * quorum copies. */
void edb_api_set_repl(edb_repl *repl);

/* Starts (or stops) the workload/performance analytics recorder. `node_id`
 * identifies this node's snapshot in the cluster-wide analytics store. */
void edb_api_analytics_start(edb_config *cfg, const char *node_id);
void edb_api_analytics_stop(void);

#endif
