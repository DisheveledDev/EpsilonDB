#include "epsilon_http.h"

#include "../epsilon_log.h"
#include <arpa/inet.h>
#include <sys/un.h>
#include <ctype.h>
#include <errno.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <poll.h>
#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <stdint.h>
#include <time.h>
#include <unistd.h>

#include <strings.h>
#include <stdarg.h>

#define EDB_HTTP_BACKLOG 64
#define EDB_HTTP_MAX_HEADER_BYTES (32 * 1024)
#define EDB_HTTP_MAX_BODY_BYTES (16 * 1024 * 1024)
#define EDB_HTTP_RECV_TIMEOUT_SEC 30
#define EDB_HTTP_MAX_ROUTES 64
#define EDB_HTTP_MAX_WORKERS 128
#define EDB_HTTP_REQUEST_TIMEOUT_MS 30000

typedef struct {
    char method[16];
    char prefix[256];
    edb_http_handler handler;
} http_route;

struct edb_http_server {
    int listen_fd;
    bool running;

    int admin_fd;                 /* -1 when no admin listener */
    char admin_path[108];         /* unix socket path (sun_path limit) */

    pthread_mutex_t routes_lock;
    pthread_mutex_t state_lock;
    pthread_cond_t workers_done;
    pthread_t accept_threads[2];
    size_t naccept_threads;
    size_t nworkers;
    struct conn_ctx *workers;
    http_route routes[EDB_HTTP_MAX_ROUTES];
    int nroutes;

    int static_route;             /* index into routes, -1 if none */
    char static_root[1024];
};

typedef struct conn_ctx {
    edb_http_server *srv;
    int fd;
    bool trusted;
    char peer_ip[64];          /* client address, "" if unavailable */
    struct conn_ctx *next;
} conn_ctx;

/* ------------------------------------------------------------------ */
/* response helpers                                                    */

char *edb_http_body_printf(size_t *len_out, const char *fmt, ...)
{
    va_list ap;
    va_start(ap, fmt);
    va_list ap2;
    va_copy(ap2, ap);
    int n = vsnprintf(NULL, 0, fmt, ap);
    va_end(ap);
    if (n < 0) {
        va_end(ap2);
        if (len_out) *len_out = 0;
        return NULL;
    }
    char *buf = malloc((size_t)n + 1);
    if (!buf) {
        va_end(ap2);
        if (len_out) *len_out = 0;
        return NULL;
    }
    vsnprintf(buf, (size_t)n + 1, fmt, ap2);
    va_end(ap2);
    if (len_out) *len_out = (size_t)n;
    return buf;
}

void edb_http_set_json(edb_http_response *res, int status, char *body)
{
    res->status = status;
    res->content_type = "application/json";
    res->body = body ? body : edb_http_body_printf(&res->body_len, "");
    res->body_len = body && res->body == body
                        ? strlen(body)
                        : res->body_len;
}

const char *edb_http_header(const edb_http_request *req, const char *name)
{
    for (int i = 0; i < req->nheaders; i++) {
        if (strcasecmp(req->header_names[i], name) == 0) {
            return req->header_values[i];
        }
    }
    return NULL;
}

static void send_all(int fd, const char *data, size_t len)
{
    size_t sent = 0;
    while (sent < len) {
        ssize_t n = send(fd, data + sent, len - sent, MSG_NOSIGNAL);
        if (n <= 0) {
            if (n < 0 && (errno == EINTR)) {
                continue;
            }
            return;
        }
        sent += (size_t)n;
    }
}

static const char *status_text(int status)
{
    switch (status) {
    case 200: return "OK";
    case 201: return "Created";
    case 204: return "No Content";
    case 400: return "Bad Request";
    case 401: return "Unauthorized";
    case 403: return "Forbidden";
    case 404: return "Not Found";
    case 405: return "Method Not Allowed";
    case 409: return "Conflict";
    case 500: return "Internal Server Error";
    default:  return "Unknown";
    }
}

static void write_response(int fd, const edb_http_response *res,
                           bool keep_alive)
{
    char head[512];
    size_t body_len = res->body ? res->body_len : 0;
    int n = snprintf(head, sizeof(head),
                     "HTTP/1.1 %d %s\r\n"
                     "Content-Type: %s\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: %s\r\n"
                     "\r\n",
                     res->status, status_text(res->status),
                     res->content_type ? res->content_type
                                       : "application/json",
                     body_len,
                     keep_alive ? "keep-alive" : "close");
    if (n > 0) {
        send_all(fd, head, (size_t)n);
        if (body_len) {
            send_all(fd, res->body, body_len);
        }
    }
}

/* ------------------------------------------------------------------ */
/* request parsing                                                     */

typedef struct {
    char *buf;
    size_t len;
    size_t cap;
} recv_buf;

static bool recv_reserve(recv_buf *rb, size_t need)
{
    if (need <= rb->cap) {
        return true;
    }
    size_t newcap = rb->cap ? rb->cap : 8192;
    while (newcap < need) {
        if (newcap > SIZE_MAX / 2) {
            return false;
        }
        newcap *= 2;
    }
    char *grown = realloc(rb->buf, newcap);
    if (!grown) {
        return false;
    }
    rb->buf = grown;
    rb->cap = newcap;
    return true;
}

static long long mono_ms(void)
{
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return (long long)now.tv_sec * 1000 + now.tv_nsec / 1000000;
}

static bool recv_append(recv_buf *rb, int fd, size_t *total_read,
                        long long deadline)
{
    if (rb->cap - rb->len < 4096 &&
        !recv_reserve(rb, rb->len + 4096)) {
        return false;
    }
    long long remaining = deadline - mono_ms();
    if (remaining <= 0) {
        return false;
    }
    struct pollfd socket_poll = { .fd = fd, .events = POLLIN };
    int ready;
    do {
        ready = poll(&socket_poll, 1, (int)remaining);
    } while (ready < 0 && errno == EINTR);
    if (ready <= 0) {
        return false;
    }
    ssize_t n;
    do {
        n = recv(fd, rb->buf + rb->len, rb->cap - rb->len, 0);
    } while (n < 0 && errno == EINTR);
    if (n <= 0) {
        return false;
    }
    rb->len += (size_t)n;
    *total_read += (size_t)n;
    return true;
}

static char *trim(char *s)
{
    while (*s == ' ' || *s == '\t') s++;
    size_t len = strlen(s);
    while (len > 0 && (s[len - 1] == ' ' || s[len - 1] == '\t' ||
                       s[len - 1] == '\r')) {
        s[--len] = '\0';
    }
    return s;
}

/* Parses one request from rb using fd for additional reads. On success
 * advances *pos past the consumed bytes. Returns false on parse error or
 * EOF. */
static char *buf_find(const char *hay, size_t hay_len, const char *needle,
                      size_t needle_len)
{
    if (needle_len == 0 || hay_len < needle_len) {
        return NULL;
    }
    for (size_t i = 0; i + needle_len <= hay_len; i++) {
        if (hay[i] == needle[0] &&
            memcmp(hay + i, needle, needle_len) == 0) {
            return (char *)(hay + i);
        }
    }
    return NULL;
}

static bool parse_request_full(recv_buf *rb, int fd, size_t *pos,
                               edb_http_request *req)
{
    memset(req, 0, sizeof(*req));
    size_t header_total = 0;
    long long deadline = mono_ms() + EDB_HTTP_REQUEST_TIMEOUT_MS;

    char *head_end = NULL;
    for (;;) {
        if (rb->len >= *pos + 4) {
            head_end = buf_find(rb->buf + *pos, rb->len - *pos, "\r\n\r\n", 4);
        }
        if (!head_end && rb->len >= *pos + 2) {
            head_end = buf_find(rb->buf + *pos, rb->len - *pos, "\n\n", 2);
        }
        if (head_end) {
            break;
        }
        if (header_total > EDB_HTTP_MAX_HEADER_BYTES) {
            return false;
        }
        if (!recv_append(rb, fd, &header_total, deadline)) {
            return false;
        }
    }

    size_t head_len;
    if (strncmp(head_end, "\r\n\r\n", 4) == 0) {
        head_len = (size_t)(head_end - (rb->buf + *pos)) + 4;
    } else {
        head_len = (size_t)(head_end - (rb->buf + *pos)) + 2;
    }
    if (head_len > EDB_HTTP_MAX_HEADER_BYTES) {
        return false;
    }

    rb->buf[*pos + head_len - 1] = '\0';   /* terminate headers block */

    /* request line: work on a copy so header parsing below sees the full
     * block unmodified */
    char *line = rb->buf + *pos;
    char *eol = strchr(line, '\n');
    if (!eol) {
        return false;
    }
    *eol = '\0';

    char target[3072];
    if (sscanf(line, "%15s %3071s", req->method, target) != 2) {
        return false;
    }
    *eol = '\n';   /* restore */

    /* split path / query */
    char *q = strchr(target, '?');
    if (q) {
        *q = '\0';
        snprintf(req->query, sizeof(req->query), "%s", q + 1);
    } else {
        req->query[0] = '\0';
    }
    snprintf(req->path, sizeof(req->path), "%s", target);

    /* headers */
    char *cursor = eol + 1;
    while ((eol = strchr(cursor, '\n')) != NULL && cursor[0] != '\r' &&
           cursor[0] != '\n') {
        *eol = '\0';
        if (req->nheaders >= EDB_HTTP_MAX_HEADERS) {
            return false;   /* overfull header table would desync the
                             * framing (Content-Length could be dropped) */
        }
        char *colon = strchr(cursor, ':');
        if (colon) {
            *colon = '\0';
            req->header_names[req->nheaders] = trim(cursor);
            req->header_values[req->nheaders] = trim(colon + 1);
            req->nheaders++;
        }
        cursor = eol + 1;
    }

    /* content length */
    size_t content_length = 0;
    int cl_count = 0;
    for (int i = 0; i < req->nheaders; i++) {
        if (strcasecmp(req->header_names[i], "Content-Length") == 0) {
            cl_count++;
        }
    }
    const char *cl = edb_http_header(req, "Content-Length");
    if (cl) {
        if (cl_count > 1) {
            return false;   /* conflicting framing: reject */
        }
        if (!*cl) {
            return false;
        }
        for (const unsigned char *digit = (const unsigned char *)cl;
             *digit; digit++) {
            if (!isdigit(*digit)) {
                return false;
            }
        }
        errno = 0;
        char *end = NULL;
        unsigned long long parsed = strtoull(cl, &end, 10);
        if (errno == ERANGE || !end || *end != '\0' ||
            parsed > EDB_HTTP_MAX_BODY_BYTES) {
            return false;
        }
        content_length = (size_t)parsed;
    }
    /* chunked bodies are not supported: rejecting up front keeps the
     * connection framing unambiguous (no smuggling via TE vs CL) */
    if (edb_http_header(req, "Transfer-Encoding")) {
        return false;
    }

    /* body starts after headers; may need more data */
    size_t name_offsets[EDB_HTTP_MAX_HEADERS];
    size_t value_offsets[EDB_HTTP_MAX_HEADERS];
    for (int i = 0; i < req->nheaders; i++) {
        name_offsets[i] = (size_t)(req->header_names[i] - rb->buf);
        value_offsets[i] = (size_t)(req->header_values[i] - rb->buf);
    }
    if (content_length > SIZE_MAX - *pos - head_len ||
        !recv_reserve(rb, *pos + head_len + content_length + 1)) {
        return false;
    }
    for (int i = 0; i < req->nheaders; i++) {
        req->header_names[i] = rb->buf + name_offsets[i];
        req->header_values[i] = rb->buf + value_offsets[i];
    }
    while (rb->len < *pos + head_len + content_length) {
        size_t dummy = 0;
        if (!recv_append(rb, fd, &dummy, deadline)) {
            return false;
        }
    }

    req->body = malloc(content_length + 1);
    if (!req->body) {
        return false;
    }
    memcpy(req->body, rb->buf + *pos + head_len, content_length);
    req->body[content_length] = '\0';
    req->body_len = content_length;

    *pos += head_len + content_length;
    return true;
}

/* ------------------------------------------------------------------ */
/* static files                                                        */

static bool path_is_safe(const char *rel)
{
    return strstr(rel, "..") == NULL;
}

static bool guess_content_type(const char *path, char *out, size_t cap)
{
    const char *ext = strrchr(path, '.');
    if (!ext) {
        snprintf(out, cap, "application/octet-stream");
        return true;
    }
    ext++;
    if (strcasecmp(ext, "html") == 0 || strcasecmp(ext, "htm") == 0) {
        snprintf(out, cap, "text/html");
    } else if (strcasecmp(ext, "js") == 0) {
        snprintf(out, cap, "text/javascript");
    } else if (strcasecmp(ext, "css") == 0) {
        snprintf(out, cap, "text/css");
    } else if (strcasecmp(ext, "json") == 0) {
        snprintf(out, cap, "application/json");
    } else if (strcasecmp(ext, "png") == 0) {
        snprintf(out, cap, "image/png");
    } else if (strcasecmp(ext, "jpg") == 0 || strcasecmp(ext, "jpeg") == 0) {
        snprintf(out, cap, "image/jpeg");
    } else if (strcasecmp(ext, "svg") == 0) {
        snprintf(out, cap, "image/svg+xml");
    } else if (strcasecmp(ext, "ico") == 0) {
        snprintf(out, cap, "image/x-icon");
    } else {
        snprintf(out, cap, "application/octet-stream");
    }
    return true;
}

static void serve_static_file(edb_http_server *srv,
                              const edb_http_request *req,
                              edb_http_response *res, bool *handled)
{
    *handled = false;
    const char *prefix = srv->routes[srv->static_route].prefix;
    size_t plen = strlen(prefix);

    if (strncmp(req->path, prefix, plen) != 0) {
        return;
    }
    const char *rel = req->path + plen;
    if (*rel == '/') {
        rel++;
    }
    if (!*rel) {
        rel = "index.html";
    }
    if (!path_is_safe(rel)) {
        res->status = 404;
        res->content_type = "text/plain";
        res->body = NULL;
        *handled = true;
        return;
    }

    char full[2048];
    snprintf(full, sizeof(full), "%s/%s", srv->static_root, rel);

    FILE *f = fopen(full, "rb");
    if (!f) {
        res->status = 404;
        res->content_type = "text/plain";
        res->body = edb_http_body_printf(&res->body_len, "not found\n");
        *handled = true;
        return;
    }
    fseek(f, 0, SEEK_END);
    long sz = ftell(f);
    fseek(f, 0, SEEK_SET);
    if (sz < 0 || sz > (long)EDB_HTTP_MAX_BODY_BYTES) {
        fclose(f);
        res->status = sz < 0 ? 500 : 404;
        *handled = true;
        return;
    }
    char *data = malloc((size_t)sz + 1);
    if (!data) {
        fclose(f);
        res->status = 500;
        *handled = true;
        return;
    }
    size_t got = fread(data, 1, (size_t)sz, f);
    fclose(f);
    data[got] = '\0';

    res->status = 200;
    char ctype[128];
    guess_content_type(full, ctype, sizeof(ctype));
    static __thread char ctype_tls[128];
    snprintf(ctype_tls, sizeof(ctype_tls), "%s", ctype);
    res->content_type = ctype_tls;
    res->body = data;
    res->body_len = got;
    *handled = true;
}

/* ------------------------------------------------------------------ */
/* connection worker                                                   */

static void *conn_main(void *arg)
{
    conn_ctx *ctx = arg;
    edb_http_server *srv = ctx->srv;
    int fd = ctx->fd;
    struct timeval tv = { .tv_sec = EDB_HTTP_RECV_TIMEOUT_SEC, .tv_usec = 0 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));

    recv_buf rb = {0};
    size_t pos = 0;
    bool keep_alive = true;

    for (;;) {
        pthread_mutex_lock(&srv->state_lock);
        bool running = srv->running;
        pthread_mutex_unlock(&srv->state_lock);
        if (!keep_alive || !running) {
            break;
        }

        edb_http_request req;
        if (!parse_request_full(&rb, fd, &pos, &req)) {
            break;
        }
        req.trusted = ctx->trusted;
        snprintf(req.peer_ip, sizeof(req.peer_ip), "%s", ctx->peer_ip);

        edb_http_response res = { .status = 500 };
        res.content_type = "application/json";

        int best = -1;
        size_t best_len = 0;
        pthread_mutex_lock(&srv->routes_lock);
        for (int i = 0; i < srv->nroutes; i++) {
            http_route *r = &srv->routes[i];
            if (i == srv->static_route) {
                continue;
            }
            size_t plen = strlen(r->prefix);
            if (strncmp(req.path, r->prefix, plen) == 0 &&
                strcmp(req.method, r->method) == 0 && plen >= best_len) {
                best = i;
                best_len = plen;
            }
        }
        bool handled = false;
        if (best >= 0) {
            http_route route_copy = srv->routes[best];
            pthread_mutex_unlock(&srv->routes_lock);
            keep_alive = route_copy.handler(&req, &res);
            handled = true;
        } else if (srv->static_route >= 0) {
            pthread_mutex_unlock(&srv->routes_lock);
            serve_static_file(srv, &req, &res, &handled);
        } else {
            pthread_mutex_unlock(&srv->routes_lock);
        }

        if (!handled) {
            res.status = 404;
            res.content_type = "application/json";
            res.body = edb_http_body_printf(
                &res.body_len,
                "{\"error\":\"not found\",\"path\":\"%s\"}", req.path);
        }

        const char *conn_hdr = edb_http_header(&req, "Connection");
        if (conn_hdr && strcasecmp(conn_hdr, "close") == 0) {
            keep_alive = false;
        }

        write_response(fd, &res, keep_alive);
        free(res.body);
        free(req.body);

        if (pos > 0) {
            rb.len -= pos;
            if (rb.len > 0) {
                memmove(rb.buf, rb.buf + pos, rb.len);
            }
            pos = 0;
        }
    }

    close(fd);
    free(rb.buf);
    pthread_mutex_lock(&srv->state_lock);
    conn_ctx **link = &srv->workers;
    while (*link && *link != ctx) {
        link = &(*link)->next;
    }
    if (*link == ctx) {
        *link = ctx->next;
    }
    if (srv->nworkers > 0) {
        srv->nworkers--;
    }
    pthread_cond_broadcast(&srv->workers_done);
    pthread_mutex_unlock(&srv->state_lock);
    free(ctx);
    return NULL;
}

/* ------------------------------------------------------------------ */

typedef struct {
    edb_http_server *srv;
    int listen_fd;
    bool trusted;
} accept_job;

static void *accept_main(void *arg)
{
    accept_job job = *(accept_job *)arg;
    free(arg);
    edb_http_server *srv = job.srv;

    for (;;) {
        pthread_mutex_lock(&srv->state_lock);
        bool running = srv->running;
        pthread_mutex_unlock(&srv->state_lock);
        if (!running) {
            break;
        }

        struct sockaddr_storage peer;
        socklen_t peer_len = sizeof(peer);
        int fd = accept(job.listen_fd, (struct sockaddr *)&peer, &peer_len);
        if (fd < 0) {
            pthread_mutex_lock(&srv->state_lock);
            running = srv->running;
            pthread_mutex_unlock(&srv->state_lock);
            if (!running) {
                break;
            }
            if (errno == EINTR || errno == ECONNABORTED) {
                continue;
            }
            edb_log("WARN", "accept failed: %s", strerror(errno));
            continue;
        }

        conn_ctx *ctx = calloc(1, sizeof(*ctx));
        if (!ctx) {
            close(fd);
            continue;
        }
        ctx->srv = srv;
        ctx->fd = fd;
        ctx->trusted = job.trusted;

        /* record the client address (for per-source auth throttling) */
        struct sockaddr_storage paddr;
        socklen_t paddr_len = sizeof(paddr);
        if (getpeername(fd, (struct sockaddr *)&paddr, &paddr_len) == 0) {
            char ipbuf[64];
            if (paddr.ss_family == AF_INET) {
                struct sockaddr_in *sin = (struct sockaddr_in *)&paddr;
                if (inet_ntop(AF_INET, &sin->sin_addr, ipbuf, sizeof(ipbuf))) {
                    snprintf(ctx->peer_ip, sizeof(ctx->peer_ip), "%s", ipbuf);
                }
            } else if (paddr.ss_family == AF_INET6) {
                struct sockaddr_in6 *sin6 = (struct sockaddr_in6 *)&paddr;
                if (inet_ntop(AF_INET6, &sin6->sin6_addr, ipbuf,
                              sizeof(ipbuf))) {
                    snprintf(ctx->peer_ip, sizeof(ctx->peer_ip), "%s", ipbuf);
                }
            }
        }

        pthread_mutex_lock(&srv->state_lock);
        if (!srv->running || srv->nworkers >= EDB_HTTP_MAX_WORKERS) {
            bool stopping = !srv->running;
            pthread_mutex_unlock(&srv->state_lock);
            close(fd);
            free(ctx);
            if (stopping) {
                break;
            }
            continue;
        }
        ctx->next = srv->workers;
        srv->workers = ctx;
        srv->nworkers++;
        pthread_mutex_unlock(&srv->state_lock);

        pthread_t worker;
        pthread_attr_t attr;
        pthread_attr_init(&attr);
        pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
        if (pthread_create(&worker, &attr, conn_main, ctx) != 0) {
            pthread_mutex_lock(&srv->state_lock);
            conn_ctx **link = &srv->workers;
            while (*link && *link != ctx) {
                link = &(*link)->next;
            }
            if (*link == ctx) {
                *link = ctx->next;
            }
            srv->nworkers--;
            pthread_cond_broadcast(&srv->workers_done);
            pthread_mutex_unlock(&srv->state_lock);
            close(fd);
            free(ctx);
        }
        pthread_attr_destroy(&attr);
    }
    return NULL;
}

static bool spawn_acceptor(edb_http_server *srv, int listen_fd, bool trusted)
{
    if (srv->naccept_threads >=
        sizeof(srv->accept_threads) / sizeof(srv->accept_threads[0])) {
        return false;
    }
    accept_job *job = malloc(sizeof(*job));
    if (!job) {
        return false;
    }
    job->srv = srv;
    job->listen_fd = listen_fd;
    job->trusted = trusted;

    pthread_t tid;
    if (pthread_create(&tid, NULL, accept_main, job) != 0) {
        free(job);
        return false;
    }
    srv->accept_threads[srv->naccept_threads++] = tid;
    return true;
}

bool edb_http_start_admin(edb_http_server *srv, const char *sock_path)
{
    if (!srv || !sock_path || !*sock_path || srv->admin_fd >= 0) {
        return false;
    }
    if (strlen(sock_path) >= sizeof(((struct sockaddr_un *)0)->sun_path)) {
        edb_log("ERROR", "admin socket path too long: %s", sock_path);
        return false;
    }

    int fd = socket(AF_UNIX, SOCK_STREAM, 0);
    if (fd < 0) {
        edb_log("ERROR", "admin socket() failed: %s", strerror(errno));
        return false;
    }

    unlink(sock_path);   /* remove stale socket from a previous run */
    struct sockaddr_un addr = {0};
    addr.sun_family = AF_UNIX;
    snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", sock_path);
    if (bind(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(fd, EDB_HTTP_BACKLOG) != 0) {
        edb_log("ERROR", "admin bind/listen on '%s' failed: %s",
                sock_path, strerror(errno));
        close(fd);
        return false;
    }

    if (!spawn_acceptor(srv, fd, true)) {
        close(fd);
        unlink(sock_path);
        return false;
    }
    srv->admin_fd = fd;
    snprintf(srv->admin_path, sizeof(srv->admin_path), "%s", sock_path);
    return true;
}

edb_http_server *edb_http_start(const char *bind_addr, int port)
{
    signal(SIGPIPE, SIG_IGN);

    edb_http_server *srv = calloc(1, sizeof(*srv));
    if (!srv) {
        return NULL;
    }
    srv->running = true;
    srv->static_route = -1;
    srv->admin_fd = -1;
    pthread_mutex_init(&srv->routes_lock, NULL);
    pthread_mutex_init(&srv->state_lock, NULL);
    pthread_cond_init(&srv->workers_done, NULL);

    srv->listen_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (srv->listen_fd < 0) {
        pthread_cond_destroy(&srv->workers_done);
        pthread_mutex_destroy(&srv->state_lock);
        pthread_mutex_destroy(&srv->routes_lock);
        free(srv);
        return NULL;
    }
    int one = 1;
    setsockopt(srv->listen_fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)port);
    if (!bind_addr || !*bind_addr ||
        inet_pton(AF_INET, bind_addr, &addr.sin_addr) != 1) {
        addr.sin_addr.s_addr = INADDR_ANY;
    }
    if (bind(srv->listen_fd, (struct sockaddr *)&addr, sizeof(addr)) != 0 ||
        listen(srv->listen_fd, EDB_HTTP_BACKLOG) != 0) {
        edb_log("ERROR", "http bind/listen on port %d failed: %s",
                port, strerror(errno));
        close(srv->listen_fd);
        pthread_cond_destroy(&srv->workers_done);
        pthread_mutex_destroy(&srv->state_lock);
        pthread_mutex_destroy(&srv->routes_lock);
        free(srv);
        return NULL;
    }

    if (!spawn_acceptor(srv, srv->listen_fd, false)) {
        edb_log("ERROR", "failed to start accept thread");
        close(srv->listen_fd);
        pthread_cond_destroy(&srv->workers_done);
        pthread_mutex_destroy(&srv->state_lock);
        pthread_mutex_destroy(&srv->routes_lock);
        free(srv);
        return NULL;
    }
    return srv;
}

bool edb_http_add_handler(edb_http_server *srv, const char *method,
                          const char *prefix, edb_http_handler handler)
{
    if (!srv || !method || !prefix || !handler ||
        srv->nroutes >= EDB_HTTP_MAX_ROUTES) {
        return false;
    }
    pthread_mutex_lock(&srv->routes_lock);
    http_route *r = &srv->routes[srv->nroutes++];
    snprintf(r->method, sizeof(r->method), "%s", method);
    snprintf(r->prefix, sizeof(r->prefix), "%s", prefix);
    r->handler = handler;
    pthread_mutex_unlock(&srv->routes_lock);
    return true;
}

bool edb_http_serve_static(edb_http_server *srv, const char *prefix,
                           const char *root_dir)
{
    if (!srv || !prefix || !root_dir || srv->static_route >= 0 ||
        strlen(prefix) >= sizeof(srv->routes[0].prefix) ||
        strlen(root_dir) >= sizeof(srv->static_root)) {
        return false;
    }
    pthread_mutex_lock(&srv->routes_lock);
    if (srv->nroutes >= EDB_HTTP_MAX_ROUTES) {
        pthread_mutex_unlock(&srv->routes_lock);
        return false;
    }
    http_route *route = &srv->routes[srv->nroutes];
    snprintf(route->method, sizeof(route->method), "GET");
    snprintf(route->prefix, sizeof(route->prefix), "%s", prefix);
    route->handler = NULL;
    srv->static_route = srv->nroutes;
    srv->nroutes++;
    snprintf(srv->static_root, sizeof(srv->static_root), "%s", root_dir);
    pthread_mutex_unlock(&srv->routes_lock);
    return true;
}

void edb_http_stop(edb_http_server *srv)
{
    if (!srv) {
        return;
    }

    pthread_mutex_lock(&srv->state_lock);
    srv->running = false;
    shutdown(srv->listen_fd, SHUT_RDWR);
    if (srv->admin_fd >= 0) {
        shutdown(srv->admin_fd, SHUT_RDWR);
    }
    for (conn_ctx *ctx = srv->workers; ctx; ctx = ctx->next) {
        shutdown(ctx->fd, SHUT_RDWR);
    }
    pthread_mutex_unlock(&srv->state_lock);

    close(srv->listen_fd);
    if (srv->admin_fd >= 0) {
        close(srv->admin_fd);
        unlink(srv->admin_path);
    }
    for (size_t i = 0; i < srv->naccept_threads; i++) {
        pthread_join(srv->accept_threads[i], NULL);
    }

    pthread_mutex_lock(&srv->state_lock);
    while (srv->nworkers > 0) {
        pthread_cond_wait(&srv->workers_done, &srv->state_lock);
    }
    pthread_mutex_unlock(&srv->state_lock);

    pthread_cond_destroy(&srv->workers_done);
    pthread_mutex_destroy(&srv->state_lock);
    pthread_mutex_destroy(&srv->routes_lock);
    free(srv);
}
