/* epsilonbkup_internal.h - shared state and declarations across the
 * epsilonbkup translation units (tool + HTTP plumbing). Not installed.
 */

#ifndef EPSILONBKUP_INTERNAL_H
#define EPSILONBKUP_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>

#include "../vendor/cjson/cJSON.h"

extern const char *g_host;      /* set => TCP HTTP mode */
extern int g_port;
extern const char *g_user;
extern const char *g_sockpath;
extern bool g_quiet;

/* --- HTTP plumbing (epsilonbkup_http.c) ------------------------------ */
int send_all(int fd, const void *data, size_t len);
char *read_all(int fd, size_t *len_out);
char *http_request_raw(const char *method, const char *path,
                       const char *body, size_t body_len,
                       size_t *resp_len_out, int *status_out);
cJSON *http_json(const char *method, const char *path, const char *body,
                 int *status_out);
const char *json_error(const cJSON *json);

#endif
