/* zesty_http.h - minimal HTTP/1.1 server for ZestyDB.
 *
 * Single-threaded accept loop with a worker thread per connection. Supports
 * keep-alive, Content-Length bodies, and serving static files from a
 * directory. Handlers are registered per (method, path prefix).
 */

#ifndef ZESTY_HTTP_H
#define ZESTY_HTTP_H

#include <stdbool.h>
#include <stddef.h>

#define ZDB_HTTP_MAX_HEADERS 32

typedef struct zdb_http_server zdb_http_server;

typedef struct {
    char method[16];
    char path[1024];          /* path without query string */
    char query[2048];         /* raw query string, "" if none */
    const char *header_names[ZDB_HTTP_MAX_HEADERS];
    const char *header_values[ZDB_HTTP_MAX_HEADERS];
    int nheaders;
    char *body;               /* NUL-terminated, may be empty */
    size_t body_len;
    void *user;               /* handler-private state */
    bool trusted;             /* arrived on the local admin socket: skip
                               * authentication, treat as admin */
} zdb_http_request;

typedef struct {
    int status;               /* e.g. 200 */
    const char *content_type; /* default "application/json" */
    char *body;               /* malloc'd or NULL for empty */
    size_t body_len;
} zdb_http_response;

/* Return true if the connection should be kept alive after this request. */
typedef bool (*zdb_http_handler)(const zdb_http_request *req,
                                 zdb_http_response *res);

zdb_http_server *zdb_http_start(const char *bind_addr, int port);
void zdb_http_stop(zdb_http_server *srv);

/* Adds a local admin listener on a Unix domain socket. Requests accepted
 * there are flagged trusted (no authentication; full admin rights). The
 * socket file is unlinked on stop. */
bool zdb_http_start_admin(zdb_http_server *srv, const char *sock_path);

/* Register handlers. Prefix match: longest matching prefix wins.
 * A prefix of "/" matches everything. */
bool zdb_http_add_handler(zdb_http_server *srv, const char *method,
                          const char *prefix, zdb_http_handler handler);

/* Serve static files under root_dir for GET requests matching prefix
 * (e.g. "/admin" -> root_dir). Path traversal is blocked. Registered as a
 * handler with lowest precedence: only used when no other handler matches. */
bool zdb_http_serve_static(zdb_http_server *srv, const char *prefix,
                           const char *root_dir);

/* Helpers for building responses inside handlers. Both allocate. */
char *zdb_http_body_printf(size_t *len_out, const char *fmt, ...);
void zdb_http_set_json(zdb_http_response *res, int status, char *body);

/* Request helpers. Case-insensitive header lookup, NULL if absent. */
const char *zdb_http_header(const zdb_http_request *req, const char *name);

#endif
