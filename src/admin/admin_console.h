#ifndef ZDB_ADMIN_CONSOLE_H
#define ZDB_ADMIN_CONSOLE_H

#include "../httpd/zesty_http.h"

/* Registers the embedded Bootstrap admin console at GET /admin. Returns
 * false when route registration fails. */
bool zdb_admin_console_register(zdb_http_server *srv);

#endif
