/* epsilonbkup_http.c - HTTP plumbing for the epsilonbkup tool (admin
 * socket / TCP client with a small response parser). See
 * epsilonbkup_internal.h.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "epsilonbkup_internal.h"
#include "engine/md5.h"
#include "socket/epsilon_cluster.h"
#include "socket/epsilon_snap.h"

#define DEFAULT_ADMIN_SOCK "epsilon-admin.sock"
#define CHUNK_BYTES (8 * 1024 * 1024)
#define MAX_SHARDS 8192
#define MAX_NODES 64

const char *g_host = NULL;
int g_port = 8123;
const char *g_user = "";
const char *g_sockpath = DEFAULT_ADMIN_SOCK;
bool g_quiet = false;

/* ------------------------------------------------------------------ */
/* HTTP plumbing                                                       */

int send_all(int fd, const void *data, size_t len)
{
    const char *p = data;
    while (len > 0) {
        ssize_t n = send(fd, p, len, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

char *read_all(int fd, size_t *len_out)
{
    size_t cap = 65536, len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        return NULL;
    }
    for (;;) {
        if (cap - len < 2) {
            cap *= 2;
            char *grown = realloc(buf, cap);
            if (!grown) {
                free(buf);
                return NULL;
            }
            buf = grown;
        }
        ssize_t n = read(fd, buf + len, cap - len - 1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            free(buf);
            return NULL;
        }
        if (n == 0) {
            break;
        }
        len += (size_t)n;
    }
    buf[len] = '\0';
    if (len_out) {
        *len_out = len;
    }
    return buf;
}

/* Returns malloc'd body or NULL on connection failure. */
char *http_request_raw(const char *method, const char *path,
                              const char *body, size_t body_len,
                              size_t *resp_len_out, int *status_out)
{
    int fd;
    if (!g_host) {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            return NULL;
        }
        struct sockaddr_un addr = {0};
        addr.sun_family = AF_UNIX;
        snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", g_sockpath);
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            if (!g_quiet) {
                fprintf(stderr,
                        "epsilonbkup: cannot connect to admin socket '%s'\n"
                        "(use -h/-p for a remote node)\n", g_sockpath);
            }
            close(fd);
            return NULL;
        }
    } else {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return NULL;
        }
        struct sockaddr_in addr = {0};
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)g_port);
        if (inet_pton(AF_INET, g_host, &addr.sin_addr) != 1) {
            if (!g_quiet) {
                fprintf(stderr, "epsilonbkup: invalid host '%s'\n", g_host);
            }
            close(fd);
            return NULL;
        }
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            if (!g_quiet) {
                fprintf(stderr, "epsilonbkup: cannot connect to %s:%d\n",
                        g_host, g_port);
            }
            close(fd);
            return NULL;
        }
    }

    int n = snprintf(NULL, 0,
                     "%s %s HTTP/1.1\r\n"
                     "Host: %s:%d\r\n"
                     "Authorization: Bearer %s\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     method, path, g_host ? g_host : "local", g_port, g_user,
                     body ? body_len : 0);
    if (n < 0) {
        close(fd);
        return NULL;
    }
    char *head = malloc((size_t)n + 1);
    if (!head) {
        close(fd);
        return NULL;
    }
    snprintf(head, (size_t)n + 1,
             "%s %s HTTP/1.1\r\n"
             "Host: %s:%d\r\n"
             "Authorization: Bearer %s\r\n"
             "Content-Length: %zu\r\n"
             "Connection: close\r\n"
             "\r\n",
             method, path, g_host ? g_host : "local", g_port, g_user,
             body ? body_len : 0);
    if (send_all(fd, head, (size_t)n) != 0) {
        free(head);
        close(fd);
        return NULL;
    }
    free(head);
    if (body && body_len > 0 && send_all(fd, body, body_len) != 0) {
        close(fd);
        return NULL;
    }

    /* read the status line and headers */
    char line[1024];
    size_t llen = 0;
    int status = 0;
    size_t content_length = 0;
    bool header_done = false;
    while (!header_done) {
        char c;
        ssize_t r = read(fd, &c, 1);
        if (r != 1) {
            close(fd);
            return NULL;
        }
        if (c == '\n') {
            if (llen > 0 && line[llen - 1] == '\r') {
                line[llen - 1] = '\0';
            } else {
                line[llen] = '\0';
            }
            llen = 0;
            if (line[0] == '\0') {
                header_done = true;
                break;
            }
            if (strncmp(line, "HTTP/", 5) == 0) {
                status = atoi(line + 9);
            } else if (strncasecmp(line, "Content-Length:", 15) == 0) {
                content_length = strtoull(line + 15, NULL, 10);
            }
        } else {
            if (llen < sizeof(line) - 1) {
                line[llen++] = c;
            }
        }
    }
    if (status_out) {
        *status_out = status;
    }

    /* body */
    char *body_buf = NULL;
    if (content_length > 0) {
        body_buf = malloc(content_length + 1);
        if (!body_buf) {
            close(fd);
            return NULL;
        }
        size_t got = 0;
        while (got < content_length) {
            ssize_t r = read(fd, body_buf + got, content_length - got);
            if (r <= 0) {
                if (r < 0 && errno == EINTR) {
                    continue;
                }
                break;
            }
            got += (size_t)r;
        }
        body_buf[got] = '\0';
        if (resp_len_out) {
            *resp_len_out = got;
        }
    } else {
        body_buf = read_all(fd, resp_len_out);
    }
    close(fd);
    return body_buf;
}

/* Sends an HTTP request and returns parsed JSON (may be NULL). */
cJSON *http_json(const char *method, const char *path,
                        const char *body, int *status_out)
{
    char *resp = http_request_raw(method, path, body, body ? strlen(body) : 0,
                                  NULL, status_out);
    if (!resp) {
        return NULL;
    }
    cJSON *json = cJSON_Parse(resp);
    free(resp);
    return json;
}

const char *json_error(const cJSON *json)
{
    if (!json) {
        return NULL;
    }
    const cJSON *e = cJSON_GetObjectItemCaseSensitive(json, "error");
    return cJSON_IsString(e) && e->valuestring ? e->valuestring : NULL;
}

/* ------------------------------------------------------------------ */
/* shard key mapping (mirrors the engine's shard_key)                  */