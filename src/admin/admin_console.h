#ifndef EDB_ADMIN_CONSOLE_H
#define EDB_ADMIN_CONSOLE_H

#include "../httpd/epsilon_http.h"

/* Registers the embedded Bootstrap admin console at GET /admin. Returns
 * false when route registration fails. */
bool edb_admin_console_register(edb_http_server *srv);

#endif
