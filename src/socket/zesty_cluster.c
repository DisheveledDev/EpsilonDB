/* zesty_cluster.c - stage 4 cluster mesh implementation.
 *
 * Wire protocol (ZSTP):
 *   offset 0: magic 'Z''S''T''P'
 *   offset 4: version byte (1)
 *   offset 5: message type byte
 *   offset 6: payload length, 4-byte big-endian
 *   offset 10: JSON payload
 *
 * Message types:
 *   ZSTP_HELLO {node_id, addr, port}   first frame on any connection
 *   ZSTP_STATE {sender, nodes:[{id,addr,port,last_seen}],
 *                       ranges:{generation, assignments:[...]}}
 *
 * Both ends send HELLO immediately after connect, then gossip their full
 * state as heartbeats. Merges are last-write-wins by last_seen for node
 * records and highest-generation-wins for range assignments. The leader
 * (lexicographically smallest online node id) recomputes contiguous
 * range assignments whenever membership changes.
 *
 * Membership/ranges persist through the settings store ("cluster.members",
 * "cluster.ranges") so they live in the system database shards like every
 * other record and replicate with the same machinery later.
 */

#include "zesty_cluster.h"
#include "zesty_snap.h"
#include "zstp_wire.h"

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <pthread.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "../engine/md5.h"
#include "../../vendor/cjson/cJSON.h"

#define HEARTBEAT_SECONDS     3
#define OFFLINE_AFTER_SECONDS 12
#define MAINTAINER_TICK_MS    500
#define MAX_PEERS             64
#define MAX_CONN_THREADS      128
#define PEER_IO_TIMEOUT_MS    15000
#define PEER_CONNECT_TIMEOUT_MS 1500

/* ------------------------------------------------------------------ */
/* helpers                                                             */

static long long epoch_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec;
}

static void sleep_ms(int ms)
{
    struct timespec ts = { .tv_sec = ms / 1000,
                           .tv_nsec = (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

static void set_tcp_nodelay(int fd)
{
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

static void set_socket_timeouts(int fd, int ms)
{
    struct timeval timeout = { .tv_sec = ms / 1000,
                               .tv_usec = (ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

/* ------------------------------------------------------------------ */
/* cluster state                                                       */

typedef struct peer_conn {
    int fd;
    bool outbound;                  /* we dialled them */
    char node_id[ZDB_NODE_ID_MAX];  /* known after HELLO exchange */
    long long last_recv;
    struct zdb_cluster *owner;
    struct peer_conn *next;
    /* lifecycle: the reader thread holds one reference. A thread that
     * wants to write on this socket takes another reference under
     * cl->lock first, so teardown can never free/close a connection
     * while a sender is inside a blocking write on it. */
    int refs;
    bool dead;
} peer_conn;

struct zdb_cluster {
    zdb_config *cfg;

    char self_id[ZDB_NODE_ID_MAX];
    char self_addr[ZDB_ADDR_MAX];
    int self_port;

    int listen_fd;

    pthread_mutex_t lock;           /* guards everything below */
    pthread_mutex_t send_lock;      /* serialises frame writes */
    pthread_cond_t conn_cv;         /* signalled when refs hit zero */
    size_t nconn_threads;           /* live reader threads (for stop) */
    peer_conn *conns;

    zdb_peer_info *peers;           /* includes self */
    size_t npeers;
    size_t peers_cap;

    long long generation;
    long long gc_after;
    zdb_range_info *ranges;
    size_t nranges;
    size_t ranges_cap;

    /* stage 6: pending target structure (generation 0 = none pending) */
    long long target_generation;
    zdb_range_info *target_ranges;
    size_t ntarget_ranges;
    size_t target_ranges_cap;

    /* stage 6e: when true (default), the maintainer marks this node
     * compliant as soon as it holds everything the target assigns to
     * it. Disabled around a join's snapshot/catch-up phase so a fresh
     * node cannot mark itself compliant before its data lands. */
    bool auto_compliant;

    char leader[ZDB_NODE_ID_MAX];

    zstp_dispatch_fn dispatch;      /* per-cluster REPL/QUERY handler */
    void *dispatch_ctx;
    size_t dispatch_inflight;

    bool running;
    pthread_t acceptor_thread;
    pthread_t maintainer_thread;
};

static bool leader_is_self_locked(const zdb_cluster *cl)
{
    return cl->leader[0] && strcmp(cl->leader, cl->self_id) == 0;
}

/* ------------------------------------------------------------------ */
/* wire codec                                                          */

static zstp_dispatch_fn g_dispatcher;
static void *g_dispatcher_ctx;
static pthread_mutex_t g_dispatch_lock = PTHREAD_MUTEX_INITIALIZER;
static pthread_cond_t g_dispatch_done = PTHREAD_COND_INITIALIZER;
static size_t g_dispatch_inflight;

static int write_full(int fd, const void *buf, size_t len)
{
    const unsigned char *p = buf;
    while (len > 0) {
        ssize_t w = send(fd, p, len, MSG_NOSIGNAL);
        if (w <= 0) {
            if (w < 0 && errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += w;
        len -= (size_t)w;
    }
    return 0;
}

static int read_full(int fd, void *buf, size_t len)
{
    unsigned char *p = buf;
    while (len > 0) {
        ssize_t r = recv(fd, p, len, 0);
        if (r <= 0) {
            if (r < 0 && errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += r;
        len -= (size_t)r;
    }
    return 0;
}

bool zstp_type_valid(int type)
{
    return type >= ZSTP_HELLO && type <= ZSTP_VOID;
}

void zstp_set_dispatcher(zstp_dispatch_fn fn, void *ctx)
{
    pthread_mutex_lock(&g_dispatch_lock);
    g_dispatcher = NULL;
    g_dispatcher_ctx = NULL;
    while (g_dispatch_inflight > 0) {
        pthread_cond_wait(&g_dispatch_done, &g_dispatch_lock);
    }
    g_dispatcher = fn;
    g_dispatcher_ctx = ctx;
    pthread_mutex_unlock(&g_dispatch_lock);
}

int zstp_send_frame(int fd, zstp_type type, const char *json,
                    void *send_lock)
{
    size_t plen = json ? strlen(json) : 0;
    if (!zstp_type_valid((int)type) || plen > ZSTP_MAX_PAYLOAD) {
        return -1;
    }
    unsigned char hdr[ZSTP_HEADER_SIZE];
    hdr[0] = 'Z';
    hdr[1] = 'S';
    hdr[2] = 'T';
    hdr[3] = 'P';
    hdr[4] = ZSTP_VERSION;
    hdr[5] = (unsigned char)type;
    uint32_t be = htonl((uint32_t)plen);
    memcpy(hdr + 6, &be, 4);

    int rc = -1;
    if (send_lock) {
        pthread_mutex_lock(send_lock);
    }
    if (write_full(fd, hdr, sizeof(hdr)) == 0 &&
        (plen == 0 || write_full(fd, json, plen) == 0)) {
        rc = 0;
    }
    if (send_lock) {
        pthread_mutex_unlock(send_lock);
    }
    return rc;
}

int zstp_recv_frame_raw(int fd, char **payload_out, uint32_t *plen_out)
{
    *payload_out = NULL;
    if (plen_out) {
        *plen_out = 0;
    }
    unsigned char hdr[ZSTP_HEADER_SIZE];
    if (read_full(fd, hdr, sizeof(hdr)) != 0) {
        return -1;
    }
    if (hdr[0] != 'Z' || hdr[1] != 'S' || hdr[2] != 'T' ||
        hdr[3] != 'P' || hdr[4] != ZSTP_VERSION ||
        !zstp_type_valid(hdr[5])) {
        return -1;
    }
    uint32_t be;
    memcpy(&be, hdr + 6, 4);
    uint32_t plen = ntohl(be);
    if (plen > ZSTP_MAX_PAYLOAD) {
        return -1;
    }
    char *payload = NULL;
    if (plen) {
        payload = malloc((size_t)plen + 1);
        if (!payload) {
            return -1;
        }
        if (read_full(fd, payload, plen) != 0) {
            free(payload);
            return -1;
        }
        payload[plen] = '\0';
    }
    *payload_out = payload;
    if (plen_out) {
        *plen_out = plen;
    }
    return hdr[5];
}

int zstp_recv_frame(int fd, char **payload_out)
{
    return zstp_recv_frame_raw(fd, payload_out, NULL);
}

int zstp_dial(const char *addr, int port)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_port = htons((uint16_t)port);
    if (inet_pton(AF_INET, addr, &sa.sin_addr) != 1) {
        close(fd);
        return -1;
    }

    int flags = fcntl(fd, F_GETFL, 0);
    if (flags < 0 || fcntl(fd, F_SETFL, flags | O_NONBLOCK) != 0) {
        close(fd);
        return -1;
    }
    int rc = connect(fd, (struct sockaddr *)&sa, sizeof(sa));
    if (rc != 0 && errno != EINPROGRESS) {
        close(fd);
        return -1;
    }
    if (rc != 0) {
        struct pollfd pollfd = { .fd = fd, .events = POLLOUT };
        do {
            rc = poll(&pollfd, 1, PEER_CONNECT_TIMEOUT_MS);
        } while (rc < 0 && errno == EINTR);
        int error = 0;
        socklen_t error_len = sizeof(error);
        if (rc <= 0 || getsockopt(fd, SOL_SOCKET, SO_ERROR, &error,
                                  &error_len) != 0 || error != 0) {
            close(fd);
            return -1;
        }
    }
    if (fcntl(fd, F_SETFL, flags) != 0) {
        close(fd);
        return -1;
    }
    set_tcp_nodelay(fd);
    set_socket_timeouts(fd, PEER_IO_TIMEOUT_MS);
    return fd;
}

static int zstp_send(int fd, zstp_type type, const char *json,
                     pthread_mutex_t *send_lock)
{
    return zstp_send_frame(fd, type, json, send_lock);
}

static int zstp_recv(int fd, char **payload_out)
{
    return zstp_recv_frame(fd, payload_out);
}

/* ------------------------------------------------------------------ */
/* state serialisation                                                 */

static cJSON *state_to_json(const zdb_cluster *cl)
{
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return NULL;
    }
    cJSON_AddStringToObject(obj, "sender", cl->self_id);

    cJSON *nodes = cJSON_AddArrayToObject(obj, "nodes");
    for (size_t i = 0; i < cl->npeers && nodes; i++) {
        cJSON *n = cJSON_CreateObject();
        cJSON_AddStringToObject(n, "id", cl->peers[i].id);
        cJSON_AddStringToObject(n, "addr", cl->peers[i].addr);
        cJSON_AddNumberToObject(n, "port", cl->peers[i].port);
        cJSON_AddNumberToObject(n, "last_seen",
                                (double)cl->peers[i].last_seen);
        if (cl->peers[i].compliant_gen > 0) {
            cJSON_AddNumberToObject(n, "compliant",
                                    (double)cl->peers[i].compliant_gen);
        }
        cJSON_AddItemToArray(nodes, n);
    }

    cJSON *r = cJSON_AddObjectToObject(obj, "ranges");
    if (r) {
        cJSON_AddNumberToObject(r, "generation", (double)cl->generation);
        cJSON *arr = cJSON_AddArrayToObject(r, "assignments");
        for (size_t i = 0; i < cl->nranges && arr; i++) {
            cJSON *a = cJSON_CreateObject();
            cJSON_AddStringToObject(a, "node_id", cl->ranges[i].node_id);
            cJSON_AddStringToObject(a, "start", cl->ranges[i].start);
            cJSON_AddStringToObject(a, "end", cl->ranges[i].end);
            cJSON_AddItemToArray(arr, a);
        }
    }

    if (cl->target_generation > 0) {
        cJSON *t = cJSON_AddObjectToObject(obj, "target");
        if (t) {
            cJSON_AddNumberToObject(t, "generation",
                                    (double)cl->target_generation);
            cJSON *tarr = cJSON_AddArrayToObject(t, "assignments");
            for (size_t i = 0; i < cl->ntarget_ranges && tarr; i++) {
                cJSON *a = cJSON_CreateObject();
                cJSON_AddStringToObject(a, "node_id",
                                        cl->target_ranges[i].node_id);
                cJSON_AddStringToObject(a, "start",
                                        cl->target_ranges[i].start);
                cJSON_AddStringToObject(a, "end", cl->target_ranges[i].end);
                cJSON_AddItemToArray(tarr, a);
            }
        }
    }
    return obj;
}

/* Merges gossiped state into our view. Returns true when anything
 * changed. Caller holds cl->lock. */
static bool merge_state(zdb_cluster *cl, const cJSON *doc)
{
    bool changed = false;

    const cJSON *nodes = cJSON_GetObjectItemCaseSensitive(doc, "nodes");
    if (cJSON_IsArray(nodes)) {
        const cJSON *item = NULL;
        cJSON_ArrayForEach(item, nodes) {
            const cJSON *jid =
                cJSON_GetObjectItemCaseSensitive(item, "id");
            const cJSON *jaddr =
                cJSON_GetObjectItemCaseSensitive(item, "addr");
            const cJSON *jport =
                cJSON_GetObjectItemCaseSensitive(item, "port");
            const cJSON *jseen =
                cJSON_GetObjectItemCaseSensitive(item, "last_seen");
            const cJSON *jcompliant =
                cJSON_GetObjectItemCaseSensitive(item, "compliant");
            if (!cJSON_IsString(jid) || !jid->valuestring ||
                !*jid->valuestring ||
                strlen(jid->valuestring) >= ZDB_NODE_ID_MAX) {
                continue;
            }
            long long seen = cJSON_IsNumber(jseen)
                                 ? (long long)jseen->valuedouble
                                 : 0;
            long long compliant = cJSON_IsNumber(jcompliant)
                                      ? (long long)jcompliant->valuedouble
                                      : 0;

            zdb_peer_info *mine = NULL;
            for (size_t i = 0; i < cl->npeers; i++) {
                if (strcmp(cl->peers[i].id, jid->valuestring) == 0) {
                    mine = &cl->peers[i];
                    break;
                }
            }
            bool is_new = mine == NULL;
            if (is_new) {
                if (cl->npeers >= MAX_PEERS) {
                    continue;
                }
                if (cl->npeers == cl->peers_cap) {
                    size_t cap = cl->peers_cap ? cl->peers_cap * 2 : 8;
                    zdb_peer_info *grown =
                        realloc(cl->peers, cap * sizeof(*grown));
                    if (!grown) {
                        continue;
                    }
                    cl->peers = grown;
                    cl->peers_cap = cap;
                }
                mine = &cl->peers[cl->npeers++];
                memset(mine, 0, sizeof(*mine));
                snprintf(mine->id, sizeof(mine->id), "%s",
                         jid->valuestring);
                mine->online = false;
                changed = true;
            }
            /* never let gossip overwrite our own address/port */
            bool selfish = strcmp(mine->id, cl->self_id) == 0;
            if (!selfish && cJSON_IsString(jaddr) && jaddr->valuestring &&
                strlen(jaddr->valuestring) < ZDB_ADDR_MAX &&
                strcmp(mine->addr, jaddr->valuestring) != 0) {
                snprintf(mine->addr, sizeof(mine->addr), "%s",
                         jaddr->valuestring);
                changed = true;
            }
            if (!selfish && cJSON_IsNumber(jport) &&
                jport->valueint > 0 && jport->valueint < 65536 &&
                mine->port != jport->valueint) {
                mine->port = jport->valueint;
                changed = true;
            }
            if (seen > mine->last_seen) {
                mine->last_seen = seen;
                changed = true;
            }
            if (compliant > mine->compliant_gen) {
                mine->compliant_gen = compliant;
                changed = true;
            }
        }
    }

    const cJSON *jr = cJSON_GetObjectItemCaseSensitive(doc, "ranges");
    const cJSON *jgen =
        cJSON_GetObjectItemCaseSensitive(jr, "generation");
    if (cJSON_IsNumber(jgen)) {
        long long gen = (long long)jgen->valuedouble;
        if (gen > cl->generation) {
            const cJSON *assigns =
                cJSON_GetObjectItemCaseSensitive(jr, "assignments");
            if (cJSON_IsArray(assigns)) {
                size_t n = 0;
                const cJSON *a = NULL;
                cJSON_ArrayForEach(a, assigns) {
                    n++;
                }
                if (n > cl->ranges_cap) {
                    zdb_range_info *grown =
                        realloc(cl->ranges, n * sizeof(*grown));
                    if (grown) {
                        cl->ranges = grown;
                        cl->ranges_cap = n;
                    }
                }
                if (n <= cl->ranges_cap) {
                    size_t i = 0;
                    cJSON_ArrayForEach(a, assigns) {
                        const cJSON *jn = cJSON_GetObjectItemCaseSensitive(
                            a, "node_id");
                        const cJSON *js =
                            cJSON_GetObjectItemCaseSensitive(a, "start");
                        const cJSON *je =
                            cJSON_GetObjectItemCaseSensitive(a, "end");
                        if (!cJSON_IsString(jn) || !cJSON_IsString(js) ||
                            !cJSON_IsString(je) || !jn->valuestring ||
                            !js->valuestring || !je->valuestring) {
                            continue;
                        }
                        snprintf(cl->ranges[i].node_id,
                                 sizeof(cl->ranges[i].node_id), "%.63s",
                                 jn->valuestring);
                        snprintf(cl->ranges[i].start,
                                 sizeof(cl->ranges[i].start), "%.32s",
                                 js->valuestring);
                        snprintf(cl->ranges[i].end,
                                 sizeof(cl->ranges[i].end), "%.32s",
                                 je->valuestring);
                        i++;
                    }
                    cl->nranges = i;
                    cl->generation = gen;
                    cl->gc_after = epoch_now() + OFFLINE_AFTER_SECONDS;
                    changed = true;
                }
            }
        }
        /* a live table promoted to this generation supersedes any
         * pending target of the same or older generation: the wave is
         * over, so drop our stale copy (otherwise it would be gossiped
         * back to the leader forever) */
        if (cl->target_generation > 0 && gen >= cl->target_generation) {
            cl->target_generation = 0;
            cl->ntarget_ranges = 0;
            changed = true;
        }
    }

    /* stage 6: pending target table merges independently */
    const cJSON *jt = cJSON_GetObjectItemCaseSensitive(doc, "target");
    const cJSON *jtgen =
        cJSON_GetObjectItemCaseSensitive(jt, "generation");
    if (cJSON_IsNumber(jtgen)) {
        long long tgen = (long long)jtgen->valuedouble;
        /* never adopt a target at or below our live generation: that is
         * a stale copy from before a promotion */
        if (tgen > cl->target_generation && tgen > cl->generation) {
            const cJSON *assigns =
                cJSON_GetObjectItemCaseSensitive(jt, "assignments");
            if (cJSON_IsArray(assigns)) {
                size_t n = 0;
                const cJSON *a = NULL;
                cJSON_ArrayForEach(a, assigns) {
                    n++;
                }
                if (n > cl->target_ranges_cap) {
                    zdb_range_info *grown =
                        realloc(cl->target_ranges, n * sizeof(*grown));
                    if (grown) {
                        cl->target_ranges = grown;
                        cl->target_ranges_cap = n;
                    }
                }
                if (n <= cl->target_ranges_cap && n > 0) {
                    size_t i = 0;
                    cJSON_ArrayForEach(a, assigns) {
                        const cJSON *jn = cJSON_GetObjectItemCaseSensitive(
                            a, "node_id");
                        const cJSON *js =
                            cJSON_GetObjectItemCaseSensitive(a, "start");
                        const cJSON *je =
                            cJSON_GetObjectItemCaseSensitive(a, "end");
                        if (!cJSON_IsString(jn) || !cJSON_IsString(js) ||
                            !cJSON_IsString(je) || !jn->valuestring ||
                            !js->valuestring || !je->valuestring) {
                            continue;
                        }
                        snprintf(cl->target_ranges[i].node_id,
                                 sizeof(cl->target_ranges[i].node_id),
                                 "%.63s", jn->valuestring);
                        snprintf(cl->target_ranges[i].start,
                                 sizeof(cl->target_ranges[i].start),
                                 "%.32s", js->valuestring);
                        snprintf(cl->target_ranges[i].end,
                                 sizeof(cl->target_ranges[i].end), "%.32s",
                                 je->valuestring);
                        i++;
                    }
                    if (i == n) {
                        cl->ntarget_ranges = n;
                        cl->target_generation = tgen;
                        changed = true;
                    }
                }
            }
        }
    } else if (cl->target_generation > 0) {
        /* stage 6e: the leader gossips state with no pending target
         * while we still hold one: the wave was voided (failed join).
         * Only the current leader may clear our copy so a lagging
         * node's stale gossip cannot kill a fresh wave. */
        const cJSON *jsender =
            cJSON_GetObjectItemCaseSensitive(doc, "sender");
        if (cJSON_IsString(jsender) && jsender->valuestring &&
            cl->leader[0] &&
            strcmp(jsender->valuestring, cl->leader) == 0) {
            cl->target_generation = 0;
            cl->ntarget_ranges = 0;
            changed = true;
        }
    }

    return changed;
}

/* ------------------------------------------------------------------ */
/* persistence                                                         */

#define SETTING_MEMBERS "cluster.members"
#define SETTING_RANGES  "cluster.ranges"
#define SETTING_TARGET  "cluster.target_ranges"
#define SETTING_LOCK    "cluster.rebalance_lock"
#define SETTING_DONE_PREFIX "rebalance.done."

static void persist_state(zdb_cluster *cl)
{
    /* caller holds no locks beyond cl->lock */
    cJSON *doc = state_to_json(cl);
    if (doc) {
        char *json = cJSON_PrintUnformatted(doc);
        if (json) {
            zdb_setting_set(cl->cfg, SETTING_MEMBERS, json);
            free(json);
        }
        cJSON_Delete(doc);
    }
    char rjson[128];
    snprintf(rjson, sizeof(rjson), "{\"generation\":%lld}",
             cl->generation);
    zdb_setting_set(cl->cfg, SETTING_RANGES, rjson);

    if (cl->target_generation > 0) {
        cJSON *t = cJSON_CreateObject();
        if (t) {
            cJSON_AddNumberToObject(t, "generation",
                                    (double)cl->target_generation);
            cJSON *arr = cJSON_AddArrayToObject(t, "assignments");
            for (size_t i = 0; arr && i < cl->ntarget_ranges; i++) {
                cJSON *a = cJSON_CreateObject();
                cJSON_AddStringToObject(a, "node_id",
                                        cl->target_ranges[i].node_id);
                cJSON_AddStringToObject(a, "start",
                                        cl->target_ranges[i].start);
                cJSON_AddStringToObject(a, "end", cl->target_ranges[i].end);
                cJSON_AddItemToArray(arr, a);
            }
            char *tjson = cJSON_PrintUnformatted(t);
            cJSON_Delete(t);
            if (tjson) {
                zdb_setting_set(cl->cfg, SETTING_TARGET, tjson);
                free(tjson);
            }
        }
    } else {
        zdb_setting_delete(cl->cfg, SETTING_TARGET);
    }
}

static void restore_state(zdb_cluster *cl)
{
    char *members = zdb_setting_get(cl->cfg, SETTING_MEMBERS);
    if (members) {
        cJSON *doc = cJSON_Parse(members);
        if (doc) {
            pthread_mutex_lock(&cl->lock);
            merge_state(cl, doc);
            pthread_mutex_unlock(&cl->lock);
            cJSON_Delete(doc);
        }
        free(members);
    }
    char *rjson = zdb_setting_get(cl->cfg, SETTING_RANGES);
    if (rjson) {
        cJSON *doc = cJSON_Parse(rjson);
        if (doc) {
            const cJSON *g =
                cJSON_GetObjectItemCaseSensitive(doc, "generation");
            if (cJSON_IsNumber(g)) {
                pthread_mutex_lock(&cl->lock);
                cl->generation = (long long)g->valuedouble;
                pthread_mutex_unlock(&cl->lock);
            }
            cJSON_Delete(doc);
        }
        free(rjson);
    }
    /* restore a pending target so a restarted node resumes the wave */
    char *tjson = zdb_setting_get(cl->cfg, SETTING_TARGET);
    if (tjson) {
        cJSON *doc = cJSON_Parse(tjson);
        if (doc) {
            pthread_mutex_lock(&cl->lock);
            merge_state(cl, doc);
            pthread_mutex_unlock(&cl->lock);
            cJSON_Delete(doc);
        }
        free(tjson);
    }
}

/* ------------------------------------------------------------------ */
/* election + placement                                                */

/* Caller holds cl->lock. */
static bool is_online_view(const zdb_cluster *cl, const char *id);

/* Recompute leader = lexicographically smallest online node id.
 * Caller holds cl->lock. */
static void recompute_leader(zdb_cluster *cl)
{
    const char *best = NULL;
    for (size_t i = 0; i < cl->npeers; i++) {
        if (!is_online_view(cl, cl->peers[i].id)) {
            continue;
        }
        if (!best || strcmp(cl->peers[i].id, best) < 0) {
            best = cl->peers[i].id;
        }
    }
    const char *cur = cl->leader[0] ? cl->leader : NULL;
    if ((best || cur) &&
        (!best != !cur || (best && cur && strcmp(best, cur) != 0))) {
        if (best) {
            snprintf(cl->leader, sizeof(cl->leader), "%s", best);
        } else {
            cl->leader[0] = '\0';
        }
    }
}

/* Caller holds cl->lock. */
static bool is_online_view(const zdb_cluster *cl, const char *id)
{
    if (strcmp(id, cl->self_id) == 0) {
        return true;
    }
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, id) == 0) {
            return cl->peers[i].online;
        }
    }
    return false;
}

/* Builds one contiguous slice per node (sorted id order) into out.
 * Returns the number of slices written; *gen_out receives generation+1.
 * Caller holds cl->lock. out/out_cap must be large enough or NULL to
 * just count. */
static size_t build_slices(const char **ids, size_t n,
                           zdb_range_info *out, size_t out_cap)
{
    uint64_t step = n ? 0x100000000ULL / n : 0;
    for (size_t i = 0; i < n; i++) {
        if (out && i < out_cap) {
            snprintf(out[i].node_id, sizeof(out[i].node_id), "%s",
                     ids[i]);
            if (i == 0) {
                memset(out[i].start, '0', 32);
                out[i].start[32] = '\0';
            } else {
                snprintf(out[i].start, sizeof(out[i].start),
                         "%08llx%024llx",
                         (unsigned long long)(i * step), 0ULL);
            }
            if (i + 1 == n) {
                memset(out[i].end, 'f', 32);
                out[i].end[32] = '\0';
            } else {
                snprintf(out[i].end, sizeof(out[i].end),
                         "%08llx%024llx",
                         (unsigned long long)((i + 1) * step), 0ULL);
            }
        }
    }
    return n;
}

/* Leader-only: computes a TARGET table over the current online members
 * when membership grew (or otherwise diverged from live owners). The
 * live table keeps serving until every node reports compliance.
 * Caller holds cl->lock. Returns true when a new target was published. */
static bool publish_target_locked(zdb_cluster *cl)
{
    const char *ids[MAX_PEERS];
    size_t n = 0;
    for (size_t i = 0; i < cl->npeers; i++) {
        if (is_online_view(cl, cl->peers[i].id)) {
            ids[n++] = cl->peers[i].id;
        }
    }
    for (size_t i = 1; i < n; i++) {
        const char *key = ids[i];
        size_t j = i;
        while (j > 0 && strcmp(ids[j - 1], key) > 0) {
            ids[j] = ids[j - 1];
            j--;
        }
        ids[j] = key;
    }
    if (n == 0) {
        return false;
    }

    /* already compliant: online set matches the live assignment */
    if (n == cl->nranges && n <= cl->ranges_cap &&
        cl->target_generation == 0) {
        bool same = true;
        for (size_t i = 0; i < n; i++) {
            if (strcmp(cl->ranges[i].node_id, ids[i]) != 0) {
                same = false;
                break;
            }
        }
        if (same) {
            return false;
        }
    }

    /* already pending with this exact assignment: skip so a target
     * generation never re-emits (and never bumps) for the same table */
    if (cl->target_generation > 0 && n == cl->ntarget_ranges &&
        n <= cl->target_ranges_cap) {
        bool same = true;
        for (size_t i = 0; i < n; i++) {
            if (strcmp(cl->target_ranges[i].node_id, ids[i]) != 0) {
                same = false;
                break;
            }
        }
        if (same) {
            return false;
        }
    }

    if (n > cl->target_ranges_cap) {
        zdb_range_info *grown =
            realloc(cl->target_ranges, n * sizeof(*grown));
        if (!grown) {
            return false;
        }
        cl->target_ranges = grown;
        cl->target_ranges_cap = n;
    }
    cl->ntarget_ranges = build_slices(ids, n, cl->target_ranges, n);
    /* monotonic: a pending target that changes content must supersede
     * the previous one so receivers (which dedupe on generation) pick
     * the corrected table up */
    cl->target_generation = cl->target_generation > 0
                                ? cl->target_generation + 1
                                : cl->generation + 1;
    return true;
}

/* Leader-only: promote pending target to live. Caller holds cl->lock. */
static void promote_target_locked(zdb_cluster *cl)
{
    if (cl->target_generation == 0 || cl->ntarget_ranges == 0) {
        return;
    }
    if (cl->ntarget_ranges > cl->ranges_cap) {
        zdb_range_info *grown = realloc(
            cl->ranges, cl->ntarget_ranges * sizeof(*grown));
        if (!grown) {
            return;
        }
        cl->ranges = grown;
        cl->ranges_cap = cl->ntarget_ranges;
    }
    memcpy(cl->ranges, cl->target_ranges,
           cl->ntarget_ranges * sizeof(*cl->ranges));
    cl->nranges = cl->ntarget_ranges;
    cl->generation = cl->target_generation;
    cl->gc_after = epoch_now() + OFFLINE_AFTER_SECONDS;
    cl->target_generation = 0;
    cl->ntarget_ranges = 0;
}

bool zdb_cluster_publish_target(zdb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    pthread_mutex_lock(&cl->lock);
    bool published =
        leader_is_self_locked(cl) && publish_target_locked(cl);
    pthread_mutex_unlock(&cl->lock);
    if (published) {
        persist_state(cl);
    }
    return published;
}

/* ------------------------------------------------------------------ */
/* gossip                                                              */

/* Drops one reference taken by a sender. Caller holds cl->lock. When the
 * connection was torn down in the meantime and this is the last
 * reference, close the fd and free it here. */
static void conn_unref_locked(zdb_cluster *cl, peer_conn *c)
{
    if (--c->refs == 0 && c->dead) {
        shutdown(c->fd, SHUT_RDWR);
        close(c->fd);
        free(c);
        pthread_cond_broadcast(&cl->conn_cv);
    }
}

/* Builds and sends HELLO (+STATE when requested) on a connection.
 * Returns 0 on success. */
static int send_hello_state(zdb_cluster *cl, int fd, bool with_state)
{
    char *hello = NULL;
    char *state = NULL;

    pthread_mutex_lock(&cl->lock);
    cJSON *h = cJSON_CreateObject();
    if (h) {
        cJSON_AddStringToObject(h, "node_id", cl->self_id);
        cJSON_AddStringToObject(h, "addr", cl->self_addr);
        cJSON_AddNumberToObject(h, "port", cl->self_port);
        hello = cJSON_PrintUnformatted(h);
        cJSON_Delete(h);
    }
    if (with_state) {
        cJSON *s = state_to_json(cl);
        state = s ? cJSON_PrintUnformatted(s) : NULL;
        cJSON_Delete(s);
    }
    pthread_mutex_unlock(&cl->lock);

    int rc = zstp_send(fd, ZSTP_HELLO, hello ? hello : "{}", NULL);
    if (rc == 0 && with_state) {
        rc = zstp_send(fd, ZSTP_STATE, state ? state : "{}", NULL);
    }
    free(hello);
    free(state);
    return rc;
}

/* Immediately push our current STATE to every live connection, so
 * topology or compliance changes propagate without waiting for the next
 * heartbeat.
 *
 * Lock ordering: cl->lock is taken alone to snapshot/build the payload,
 * then released before acquiring send_lock for each write. Never hold
 * both at once here - the reverse order would deadlock against the
 * connection-teardown path. */
static void gossip_state(zdb_cluster *cl)
{
    pthread_mutex_lock(&cl->lock);
    cJSON *doc = state_to_json(cl);
    char *state = doc ? cJSON_PrintUnformatted(doc) : NULL;
    cJSON_Delete(doc);

    size_t count = 0;
    for (peer_conn *conn = cl->conns; conn; conn = conn->next) {
        if (!conn->dead) {
            count++;
        }
    }
    peer_conn **connections = count ? calloc(count, sizeof(*connections)) : NULL;
    size_t used = 0;
    if (count == 0 || connections) {
        for (peer_conn *conn = cl->conns; conn && used < count;
             conn = conn->next) {
            if (!conn->dead) {
                conn->refs++;
                connections[used++] = conn;
            }
        }
    }
    pthread_mutex_unlock(&cl->lock);

    for (size_t i = 0; i < used; i++) {
        zstp_send(connections[i]->fd, ZSTP_STATE, state ? state : "{}",
                  &cl->send_lock);
    }

    free(state);
    pthread_mutex_lock(&cl->lock);
    for (size_t i = 0; i < used; i++) {
        conn_unref_locked(cl, connections[i]);
    }
    pthread_mutex_unlock(&cl->lock);
    free(connections);
}

/* Leader reaction after topology changes: recompute ranges and persist.
 * Called without holding the lock. */
static void leader_react(zdb_cluster *cl, bool changed)
{
    if (!changed) {
        return;
    }
    pthread_mutex_lock(&cl->lock);
    bool am_leader = leader_is_self_locked(cl);
    long long before_target = cl->target_generation;
    if (am_leader) {
        publish_target_locked(cl);
    }
    bool target_published =
        am_leader && cl->target_generation != before_target;
    persist_state(cl);
    pthread_mutex_unlock(&cl->lock);

    /* the leader holds the global rebalance lock for the whole wave so
     * only one structure change runs at a time (released on promotion
     * or when the wave is voided by a departure) */
    if (target_published) {
        zdb_cluster_acquire_rebalance_lock(cl);
    }

    /* stage 6d: once every node reports compliance the leader promotes
     * automatically (the data migration steps happen via 6c catch-up) */
    if (am_leader && cl->target_generation > 0) {
        zdb_cluster_maybe_promote(cl);
    }

    /* a fresh target means the structure changed: gossip immediately so
     * the wave starts everywhere without waiting for the next heartbeat
     * (also unblocks tests that join and wait right away) */
    if (target_published) {
        gossip_state(cl);
    }
}

/* Per-connection loop. Takes ownership of `c` and frees it.
 * Frame ordering on a fresh connection: whichever side spawned this
 * thread already sent its own HELLO (acceptor/connector/join). The
 * remote side does the same, so the first frame we read here is their
 * HELLO. After recording their identity we push a full STATE so they
 * learn the mesh immediately, then keep reading STATE heartbeats.
 * The connection is registered only after the HELLO exchange so peers
 * never flap online with unknown identities. */
/* Decrements the live reader-thread counter. Caller holds cl->lock. */
static void reader_exiting_locked(zdb_cluster *cl)
{
    cl->nconn_threads--;
    pthread_cond_broadcast(&cl->conn_cv);
}

static void *conn_thread(void *arg)
{
    peer_conn *c = arg;
    zdb_cluster *cl = c->owner;

    char *payload = NULL;
    int t = zstp_recv(c->fd, &payload);
    if (t == ZSTP_HELLO && payload) {
        cJSON *h = cJSON_Parse(payload);
        if (h) {
            const cJSON *jid =
                cJSON_GetObjectItemCaseSensitive(h, "node_id");
            const cJSON *jaddr =
                cJSON_GetObjectItemCaseSensitive(h, "addr");
            const cJSON *jport =
                cJSON_GetObjectItemCaseSensitive(h, "port");
            bool is_self = false;
            if (cJSON_IsString(jid) && jid->valuestring &&
                strlen(jid->valuestring) < ZDB_NODE_ID_MAX) {
                snprintf(c->node_id, sizeof(c->node_id), "%s",
                         jid->valuestring);
                is_self = strcmp(c->node_id, cl->self_id) == 0;
            }
            /* stage 6a: one node may join at a time. A brand-new node
             * dialling in while a rebalance wave is pending is refused:
             * reply with a rejecting HELLO and hang up without
             * registering it. Known peers (redials, restarts), our own
             * wake-up connection, and one-shot data-plane RPCs (which
             * identify as "ephemeral" and never join the mesh) are
             * always let through. */
            if (!is_self && c->node_id[0] &&
                strcmp(c->node_id, "ephemeral") != 0) {
                bool known = false;
                pthread_mutex_lock(&cl->lock);
                for (size_t i = 0; i < cl->npeers; i++) {
                    if (strcmp(cl->peers[i].id, c->node_id) == 0) {
                        known = true;
                        break;
                    }
                }
                bool busy = cl->target_generation > 0;
                pthread_mutex_unlock(&cl->lock);
                if (!known && busy) {
                    cJSON *r = cJSON_CreateObject();
                    cJSON_AddStringToObject(r, "node_id", cl->self_id);
                    cJSON_AddStringToObject(r, "reject", "rebalance");
                    char *rs = r ? cJSON_PrintUnformatted(r) : NULL;
                    cJSON_Delete(r);
                    zstp_send(c->fd, ZSTP_HELLO, rs ? rs : "{}", NULL);
                    free(rs);
                    close(c->fd);
                    free(c);
                    pthread_mutex_lock(&cl->lock);
                    reader_exiting_locked(cl);
                    pthread_mutex_unlock(&cl->lock);
                    return NULL;
                }
            }
            /* adopt the caller's self-declared address/port for
             * membership so we know where to dial them back */
            if (c->node_id[0] && !is_self && cJSON_IsString(jaddr) &&
                jaddr->valuestring &&
                strlen(jaddr->valuestring) < ZDB_ADDR_MAX) {
                pthread_mutex_lock(&cl->lock);
                zdb_peer_info *p = NULL;
                for (size_t i = 0; i < cl->npeers; i++) {
                    if (strcmp(cl->peers[i].id, c->node_id) == 0) {
                        p = &cl->peers[i];
                        break;
                    }
                }
                if (!p && cl->npeers < MAX_PEERS) {
                    if (cl->npeers == cl->peers_cap) {
                        size_t cap =
                            cl->peers_cap ? cl->peers_cap * 2 : 8;
                        zdb_peer_info *grown =
                            realloc(cl->peers, cap * sizeof(*grown));
                        if (grown) {
                            cl->peers = grown;
                            cl->peers_cap = cap;
                        }
                    }
                    if (cl->npeers < cl->peers_cap) {
                        p = &cl->peers[cl->npeers++];
                        memset(p, 0, sizeof(*p));
                        snprintf(p->id, sizeof(p->id), "%s",
                                 c->node_id);
                    }
                }
                if (p) {
                    snprintf(p->addr, sizeof(p->addr), "%s",
                             jaddr->valuestring);
                    p->port = cJSON_IsNumber(jport) ? jport->valueint : 0;
                    p->last_seen = epoch_now();
                }
                pthread_mutex_unlock(&cl->lock);
            }
            cJSON_Delete(h);
        }
    }
    free(payload);

    if (!c->node_id[0]) {
        close(c->fd);
        free(c);
        pthread_mutex_lock(&cl->lock);
        reader_exiting_locked(cl);
        pthread_mutex_unlock(&cl->lock);
        return NULL;
    }

    send_hello_state(cl, c->fd, true);

    pthread_mutex_lock(&cl->lock);
    c->last_recv = epoch_now();
    c->refs = 1;   /* the reader's own reference */
    c->next = cl->conns;
    cl->conns = c;
    recompute_leader(cl);
    bool became_online = false;
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, c->node_id) == 0) {
            /* a peer we already knew about coming back is not a
             * membership change; only a brand-new member starts a wave */
            became_online = !cl->peers[i].online &&
                            cl->peers[i].last_seen == 0;
            cl->peers[i].online = true;
            break;
        }
    }
    pthread_mutex_unlock(&cl->lock);

    /* a new member changes the topology: let the leader publish a
     * target structure right away instead of waiting for the next
     * heartbeat round */
    leader_react(cl, became_online);
    gossip_state(cl);

    bool changed = false;
    for (;;) {
        t = zstp_recv(c->fd, &payload);
        if (t < 0) {
            break;
        }
        changed = false;
        if (t == ZSTP_STATE && payload) {
            pthread_mutex_lock(&cl->lock);
            c->last_recv = epoch_now();
            cJSON *doc = cJSON_Parse(payload);
            if (doc) {
                changed = merge_state(cl, doc);
                /* a peer we believed offline just told us it is alive:
                 * trust its word and mark it online immediately so the
                 * leader's range table converges on the next tick */
                if (c->node_id[0]) {
                    for (size_t i = 0; i < cl->npeers; i++) {
                        if (strcmp(cl->peers[i].id, c->node_id) == 0 &&
                            !cl->peers[i].online) {
                            cl->peers[i].online = true;
                            changed = true;
                            break;
                        }
                    }
                }
                cJSON_Delete(doc);
            }
            recompute_leader(cl);
            pthread_mutex_unlock(&cl->lock);
        } else if (t == ZSTP_SNAP_REQ) {
            /* stage 6b/6c: shard snapshot request. Served directly
             * (not via the dispatcher) so the engine behind the config
             * store streams a consistent copy; runs without cl->lock
             * held because it blocks on SQLite + socket I/O. */
            pthread_mutex_lock(&cl->lock);
            c->last_recv = epoch_now();
            pthread_mutex_unlock(&cl->lock);
            zdb_snap_serve(c->fd,
                           payload ? (uint32_t)strlen(payload) : 0,
                           payload, zdb_config_engine(cl->cfg));
        } else if (t == ZSTP_VOID) {
            /* stage 6e: rollback request. Only the leader acts; the
             * reply tells the requester whether a wave was voided. */
            pthread_mutex_lock(&cl->lock);
            c->last_recv = epoch_now();
            pthread_mutex_unlock(&cl->lock);
            bool ok = zdb_cluster_void_target(cl);
            char reply[32];
            snprintf(reply, sizeof(reply), "{\"ok\":%s}",
                     ok ? "true" : "false");
            zstp_send(c->fd, ZSTP_ACK, reply, &cl->send_lock);
        } else if (t == ZSTP_REPL || t == ZSTP_QUERY ||
                   t == ZSTP_FLUSH) {
            zstp_dispatch_fn fn = NULL;
            void *fn_ctx = NULL;
            bool global = false;
            pthread_mutex_lock(&cl->lock);
            c->last_recv = epoch_now();
            if (cl->dispatch) {
                fn = cl->dispatch;
                fn_ctx = cl->dispatch_ctx;
                cl->dispatch_inflight++;
            }
            pthread_mutex_unlock(&cl->lock);
            if (!fn) {
                pthread_mutex_lock(&g_dispatch_lock);
                if (g_dispatcher) {
                    fn = g_dispatcher;
                    fn_ctx = g_dispatcher_ctx;
                    g_dispatch_inflight++;
                    global = true;
                }
                pthread_mutex_unlock(&g_dispatch_lock);
            }

            int reply_type = 0;
            char *reply = NULL;
            if (fn && fn(fn_ctx, t, payload ? payload : "{}", &reply_type,
                         &reply) && reply) {
                zstp_send(c->fd, (zstp_type)reply_type, reply,
                          &cl->send_lock);
            }
            free(reply);
            if (fn) {
                if (global) {
                    pthread_mutex_lock(&g_dispatch_lock);
                    g_dispatch_inflight--;
                    pthread_cond_broadcast(&g_dispatch_done);
                    pthread_mutex_unlock(&g_dispatch_lock);
                } else {
                    pthread_mutex_lock(&cl->lock);
                    cl->dispatch_inflight--;
                    pthread_cond_broadcast(&cl->conn_cv);
                    pthread_mutex_unlock(&cl->lock);
                }
            }
        } else if (t >= 0) {
            pthread_mutex_lock(&cl->lock);
            c->last_recv = epoch_now();
            pthread_mutex_unlock(&cl->lock);
        }
        free(payload);
        payload = NULL;
        if (changed) {
            leader_react(cl, true);
        }
    }

    /* unregister before freeing: stop() frees the whole connection list
     * under cl->lock, so a conn_thread must not touch its `c` after
     * unlinking it here (stop may have already freed it). Any sender
     * holding a reference finishes its write first; the last unref does
     * the actual close+free. */
    pthread_mutex_lock(&cl->lock);
    peer_conn **pp = &cl->conns;
    while (*pp && *pp != c) {
        pp = &(*pp)->next;
    }
    bool linked = (*pp == c);
    if (linked) {
        *pp = c->next;
    }
    if (!linked) {
        /* stop() already reaped this connection and its fd */
        reader_exiting_locked(cl);
        pthread_mutex_unlock(&cl->lock);
        return NULL;
    }
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, c->node_id) == 0) {
            cl->peers[i].online = false;
            break;
        }
    }
    recompute_leader(cl);
    bool was_leader = leader_is_self_locked(cl);
    int fd = c->fd;
    c->dead = true;
    c->refs--;   /* drop the reader's own reference */
    if (c->refs > 0) {
        fd = -1;   /* last sender will close+free via conn_unref_locked */
    } else {
        free(c);
    }
    pthread_mutex_unlock(&cl->lock);

    if (fd >= 0) {
        shutdown(fd, SHUT_RDWR);
        close(fd);
    }

    if (was_leader) {
        pthread_mutex_lock(&cl->lock);
        /* a node left: shrink live directly (no transfer possible to a
         * departed node); any pending target is void */
        cl->target_generation = 0;
        cl->ntarget_ranges = 0;
        zdb_setting_delete(cl->cfg, SETTING_LOCK);
        if (cl->nranges > 0) {
            const char *ids[MAX_PEERS];
            size_t n = 0;
            for (size_t i = 0; i < cl->npeers; i++) {
                if (is_online_view(cl, cl->peers[i].id)) {
                    ids[n++] = cl->peers[i].id;
                }
            }
            for (size_t i = 1; i < n; i++) {
                const char *key = ids[i];
                size_t j = i;
                while (j > 0 && strcmp(ids[j - 1], key) > 0) {
                    ids[j] = ids[j - 1];
                    j--;
                }
                ids[j] = key;
            }
            if (n > 0 && n != cl->nranges) {
                if (n <= cl->ranges_cap ||
                    (cl->ranges = realloc(cl->ranges,
                                          n * sizeof(*cl->ranges))) != NULL) {
                    if (n > cl->ranges_cap) {
                        cl->ranges_cap = n;
                    }
                    cl->nranges = build_slices(ids, n, cl->ranges, n);
                    cl->generation++;
                }
            }
        }
        persist_state(cl);
        pthread_mutex_unlock(&cl->lock);
    }
    pthread_mutex_lock(&cl->lock);
    reader_exiting_locked(cl);
    pthread_mutex_unlock(&cl->lock);
    return NULL;
}

static void spawn_conn_thread(peer_conn *c)
{
    pthread_t tid;
    pthread_attr_t attr;
    pthread_attr_init(&attr);
    pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED);
    zdb_cluster *cl = c->owner;
    pthread_mutex_lock(&cl->lock);
    cl->nconn_threads++;
    pthread_mutex_unlock(&cl->lock);
    if (pthread_create(&tid, &attr, conn_thread, c) != 0) {
        close(c->fd);
        free(c);
        pthread_mutex_lock(&cl->lock);
        cl->nconn_threads--;
        pthread_cond_broadcast(&cl->conn_cv);
        pthread_mutex_unlock(&cl->lock);
    }
    pthread_attr_destroy(&attr);
}

/* ------------------------------------------------------------------ */
/* acceptor thread                                                     */

static void *acceptor_main(void *arg)
{
    zdb_cluster *cl = arg;
    pthread_mutex_lock(&cl->lock);
    bool running = cl->running;
    int listen_fd = cl->listen_fd;
    pthread_mutex_unlock(&cl->lock);
    while (running) {
        int fd = accept(listen_fd, NULL, NULL);
        if (fd < 0) {
            if (errno == EINTR || errno == EAGAIN || errno == EWOULDBLOCK) {
                sleep_ms(50);
                continue;
            }
            break;
        }
        set_tcp_nodelay(fd);
        set_socket_timeouts(fd, PEER_IO_TIMEOUT_MS);
        pthread_mutex_lock(&cl->lock);
        bool at_limit = cl->nconn_threads >= MAX_CONN_THREADS;
        pthread_mutex_unlock(&cl->lock);
        if (at_limit) {
            close(fd);
            continue;
        }

        /* announce ourselves before entering the read loop */
        send_hello_state(cl, fd, false);

        peer_conn *c = calloc(1, sizeof(*c));
        if (!c) {
            close(fd);
            continue;
        }
        c->fd = fd;
        c->outbound = false;
        c->owner = cl;
        spawn_conn_thread(c);

        pthread_mutex_lock(&cl->lock);
        running = cl->running;
        listen_fd = cl->listen_fd;   /* unchanged, but re-read under lock */
        pthread_mutex_unlock(&cl->lock);
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* maintainer thread: dial peers, heartbeat, failure detection         */

static int dial_peer(const char *addr, int port)
{
    return zstp_dial(addr, port);
}

static bool have_conn_locked(const zdb_cluster *cl, const char *id)
{
    for (const peer_conn *c = cl->conns; c; c = c->next) {
        if (strcmp(c->node_id, id) == 0) {
            return true;
        }
    }
    return false;
}

static void maintainer_tick(zdb_cluster *cl)
{
    /* heartbeat established connections. Build the payload once under
     * cl->lock, reference every connection, then release cl->lock so a
     * slow/stalled peer cannot stall the whole cluster state while we
     * block in send(). */
    peer_conn *hb[256];
    size_t nhb = 0;
    pthread_mutex_lock(&cl->lock);
    char *state = NULL;
    cJSON *s = state_to_json(cl);
    state = s ? cJSON_PrintUnformatted(s) : NULL;
    cJSON_Delete(s);
    for (peer_conn *c = cl->conns; c && nhb < 256; c = c->next) {
        if (!c->dead) {
            c->refs++;
            hb[nhb++] = c;
        }
    }
    pthread_mutex_unlock(&cl->lock);

    for (size_t i = 0; i < nhb; i++) {
        if (zstp_send(hb[i]->fd, ZSTP_STATE, state ? state : "{}",
                      &cl->send_lock) != 0) {
            shutdown(hb[i]->fd, SHUT_RDWR);   /* reader thread tears it down */
        }
    }
    free(state);

    pthread_mutex_lock(&cl->lock);
    for (size_t i = 0; i < nhb; i++) {
        conn_unref_locked(cl, hb[i]);
    }
    pthread_mutex_unlock(&cl->lock);

    /* connections whose reader threads already exited are unlinked from
     * cl->conns there; nothing else to reap here */

    /* dial known peers that are marked online but have no connection.
     * The online flag is only cleared on connection loss, so a peer that
     * went offline while we were disconnected still gets dialled here
     * and comes back once the link is re-established. */
    pthread_mutex_lock(&cl->lock);
    for (size_t i = 0; i < cl->npeers; i++) {
        zdb_peer_info p = cl->peers[i];   /* copy; lock released below */
        if (strcmp(p.id, cl->self_id) == 0 ||
            p.addr[0] == '\0' || p.port <= 0 ||
            (p.port == cl->self_port &&
             strcmp(p.addr, "127.0.0.1") == 0) ||
            have_conn_locked(cl, p.id)) {
            continue;
        }
        pthread_mutex_unlock(&cl->lock);

        int fd = dial_peer(p.addr, p.port);
        if (fd >= 0) {
            send_hello_state(cl, fd, false);
            peer_conn *c = calloc(1, sizeof(*c));
            if (c) {
                c->fd = fd;
                c->outbound = true;
                c->owner = cl;
                snprintf(c->node_id, sizeof(c->node_id), "%s", p.id);
                spawn_conn_thread(c);
            } else {
                close(fd);
            }
        }
        pthread_mutex_lock(&cl->lock);
    }
    pthread_mutex_unlock(&cl->lock);

    zdb_cluster_maybe_promote(cl);

    /* stage 6e: auto-compliance. A node that already holds everything
     * the pending target assigns to it reports compliance without
     * waiting for an explicit API call; the joiner's catch-up flow
     * disables this until its snapshots have landed. */
    pthread_mutex_lock(&cl->lock);
    bool auto_ok = cl->auto_compliant;
    long long tgen = cl->target_generation;
    long long self_gen = 0;
    if (tgen > 0) {
        for (size_t i = 0; i < cl->npeers; i++) {
            if (strcmp(cl->peers[i].id, cl->self_id) == 0) {
                self_gen = cl->peers[i].compliant_gen;
                break;
            }
        }
    }
    pthread_mutex_unlock(&cl->lock);
    if (auto_ok && tgen > 0 && self_gen < tgen &&
        !zdb_cluster_needs_sync(cl)) {
        zdb_cluster_mark_compliant(cl);
    }
    pthread_mutex_lock(&cl->lock);
    bool gc_ready = cl->target_generation == 0 && cl->gc_after > 0 &&
                    epoch_now() >= cl->gc_after;
    pthread_mutex_unlock(&cl->lock);
    if (gc_ready) {
        zdb_cluster_gc_redundant(cl);
    }
}

static void *maintainer_main(void *arg)
{
    zdb_cluster *cl = arg;
    int elapsed_ms = 0;
    for (;;) {
        sleep_ms(MAINTAINER_TICK_MS);
        pthread_mutex_lock(&cl->lock);
        bool running = cl->running;
        pthread_mutex_unlock(&cl->lock);
        if (!running) {
            break;
        }
        elapsed_ms += MAINTAINER_TICK_MS;
        if (elapsed_ms < HEARTBEAT_SECONDS * 1000) {
            continue;
        }
        elapsed_ms = 0;
        maintainer_tick(cl);
    }
    return NULL;
}

/* ------------------------------------------------------------------ */
/* public API                                                          */

static void add_self_locked(zdb_cluster *cl)
{
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, cl->self_id) == 0) {
            /* restored from persistence: refresh identity fields that
             * gossip never overwrites for our own record */
            snprintf(cl->peers[i].addr, sizeof(cl->peers[i].addr), "%s",
                     cl->self_addr);
            cl->peers[i].port = cl->self_port;
            cl->peers[i].online = true;
            return;
        }
    }
    if (cl->npeers == cl->peers_cap) {
        size_t cap = cl->peers_cap ? cl->peers_cap * 2 : 8;
        zdb_peer_info *grown = realloc(cl->peers, cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        cl->peers = grown;
        cl->peers_cap = cap;
    }
    zdb_peer_info *p = &cl->peers[cl->npeers++];
    memset(p, 0, sizeof(*p));
    snprintf(p->id, sizeof(p->id), "%s", cl->self_id);
    snprintf(p->addr, sizeof(p->addr), "%s", cl->self_addr);
    p->port = cl->self_port;
    p->last_seen = epoch_now();
    p->online = true;
}

zdb_cluster *zdb_cluster_start(zdb_config *cfg, const char *advertise_addr,
                               int peer_port,
                               char node_id_out[ZDB_NODE_ID_MAX])
{
    if (!cfg || !advertise_addr || !*advertise_addr ||
        peer_port <= 0 || peer_port > 65535) {
        return NULL;
    }

    zdb_cluster *cl = calloc(1, sizeof(*cl));
    if (!cl) {
        return NULL;
    }
    cl->cfg = cfg;
    cl->auto_compliant = true;
    snprintf(cl->self_addr, sizeof(cl->self_addr), "%s", advertise_addr);
    cl->self_port = peer_port;

    /* stable node id: md5 of the advertise address + port */
    char seed[256];
    snprintf(seed, sizeof(seed), "zdb-node:%s:%d", advertise_addr,
             peer_port);
    char hex[33];
    zdb_md5_hex(seed, strlen(seed), hex);
    snprintf(cl->self_id, sizeof(cl->self_id), "node-%.12s", hex);

    pthread_mutex_init(&cl->lock, NULL);
    pthread_mutex_init(&cl->send_lock, NULL);
    pthread_cond_init(&cl->conn_cv, NULL);
    cl->listen_fd = -1;

    restore_state(cl);

    pthread_mutex_lock(&cl->lock);
    add_self_locked(cl);
    recompute_leader(cl);
    bool am_leader = leader_is_self_locked(cl) && cl->nranges == 0;
    if (am_leader) {
        /* first node: claim the whole space directly in live */
        const char *self[] = { cl->self_id };
        if (cl->nranges == 0 && cl->ranges_cap < 1) {
            cl->ranges = realloc(cl->ranges, sizeof(*cl->ranges));
            if (cl->ranges) {
                cl->ranges_cap = 1;
            }
        }
        if (cl->ranges_cap >= 1) {
            cl->nranges = build_slices(self, 1, cl->ranges, 1);
            cl->generation++;
        }
    }
    persist_state(cl);
    pthread_mutex_unlock(&cl->lock);

    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        goto fail;
    }
    int one = 1;
    setsockopt(fd, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    struct sockaddr_in sa = {0};
    sa.sin_family = AF_INET;
    sa.sin_addr.s_addr = htonl(INADDR_ANY);
    sa.sin_port = htons((uint16_t)peer_port);
    if (bind(fd, (struct sockaddr *)&sa, sizeof(sa)) != 0 ||
        listen(fd, 16) != 0) {
        close(fd);
        goto fail;
    }
    cl->listen_fd = fd;

    cl->running = true;
    if (pthread_create(&cl->acceptor_thread, NULL, acceptor_main, cl) !=
        0) {
        cl->running = false;
        goto fail;
    }
    if (pthread_create(&cl->maintainer_thread, NULL, maintainer_main,
                       cl) != 0) {
        cl->running = false;
        goto fail;
    }

    if (node_id_out) {
        snprintf(node_id_out, ZDB_NODE_ID_MAX, "%s", cl->self_id);
    }
    return cl;

fail:
    if (cl->listen_fd >= 0) {
        close(cl->listen_fd);
    }
    free(cl->peers);
    free(cl->ranges);
    pthread_mutex_destroy(&cl->send_lock);
    pthread_mutex_destroy(&cl->lock);
    free(cl);
    return NULL;
}

void zdb_cluster_stop(zdb_cluster *cl)
{
    if (!cl) {
        return;
    }
    pthread_mutex_lock(&cl->lock);
    cl->running = false;
    pthread_mutex_unlock(&cl->lock);

    /* wake the acceptor by connecting to our own listener */
    if (cl->self_addr[0]) {
        int fd = dial_peer("127.0.0.1", cl->self_port);
        if (fd >= 0) {
            close(fd);
        }
    }
    pthread_join(cl->acceptor_thread, NULL);
    pthread_join(cl->maintainer_thread, NULL);

    /* tear down live peer connections: shutdown() wakes their reader
     * threads, which unlink themselves and drop their reference; the
     * last unref closes the fd and frees the connection */
    pthread_mutex_lock(&cl->lock);
    for (peer_conn *c = cl->conns; c; c = c->next) {
        c->dead = true;
        shutdown(c->fd, SHUT_RDWR);
    }
    while (cl->nconn_threads > 0) {
        pthread_cond_wait(&cl->conn_cv, &cl->lock);
    }
    cl->conns = NULL;
    persist_state(cl);
    pthread_mutex_unlock(&cl->lock);

    close(cl->listen_fd);
    free(cl->peers);
    free(cl->ranges);
    free(cl->target_ranges);
    pthread_mutex_destroy(&cl->send_lock);
    pthread_cond_destroy(&cl->conn_cv);
    pthread_mutex_destroy(&cl->lock);
    free(cl);
}

size_t zdb_cluster_peers(zdb_cluster *cl, zdb_peer_info *out, size_t cap)
{
    if (!cl || !out) {
        return 0;
    }
    pthread_mutex_lock(&cl->lock);
    size_t n = 0;
    for (size_t i = 0; i < cl->npeers && n < cap; i++) {
        out[n++] = cl->peers[i];
    }
    pthread_mutex_unlock(&cl->lock);
    return n;
}

const char *zdb_cluster_leader(zdb_cluster *cl)
{
    static _Thread_local char buf[ZDB_NODE_ID_MAX];
    if (!cl) {
        return NULL;
    }
    pthread_mutex_lock(&cl->lock);
    if (cl->leader[0]) {
        snprintf(buf, sizeof(buf), "%s", cl->leader);
    } else {
        buf[0] = '\0';
    }
    pthread_mutex_unlock(&cl->lock);
    return buf[0] ? buf : NULL;
}

const char *zdb_cluster_self_id(zdb_cluster *cl)
{
    return cl ? cl->self_id : "";
}

bool zdb_cluster_is_leader(zdb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    pthread_mutex_lock(&cl->lock);
    bool me = leader_is_self_locked(cl);
    pthread_mutex_unlock(&cl->lock);
    return me;
}

long long zdb_cluster_generation(zdb_cluster *cl)
{
    if (!cl) {
        return 0;
    }
    pthread_mutex_lock(&cl->lock);
    long long g = cl->generation;
    pthread_mutex_unlock(&cl->lock);
    return g;
}

size_t zdb_cluster_ranges(zdb_cluster *cl, zdb_range_info *out, size_t cap)
{
    if (!cl || !out) {
        return 0;
    }
    pthread_mutex_lock(&cl->lock);
    size_t n = 0;
    for (size_t i = 0; i < cl->nranges && n < cap; i++) {
        out[n++] = cl->ranges[i];
    }
    pthread_mutex_unlock(&cl->lock);
    return n;
}

const char *zdb_cluster_owner(zdb_cluster *cl, const char *md5hex)
{
    static _Thread_local char owner_buf[ZDB_NODE_ID_MAX];
    if (!cl || !md5hex || strlen(md5hex) != 32) {
        return NULL;
    }
    bool found = false;
    pthread_mutex_lock(&cl->lock);
    for (size_t i = 0; i < cl->nranges; i++) {
        if (strncmp(md5hex, cl->ranges[i].start, 32) >= 0 &&
            (strncmp(md5hex, cl->ranges[i].end, 32) < 0 ||
             memcmp(cl->ranges[i].end,
                    "ffffffffffffffffffffffffffffffff", 32) == 0)) {
            /* copy out: the ranges array can be reallocated or promoted
             * concurrently, so a bare pointer would dangle */
            snprintf(owner_buf, sizeof(owner_buf), "%s",
                     cl->ranges[i].node_id);
            found = true;
            break;
        }
    }
    pthread_mutex_unlock(&cl->lock);
    return found ? owner_buf : NULL;
}

size_t zdb_cluster_holders(zdb_cluster *cl, const char *md5hex,
                           char (*node_ids)[ZDB_NODE_ID_MAX], size_t cap)
{
    if (!cl || !md5hex || strlen(md5hex) != 32 || !node_ids || cap == 0) {
        return 0;
    }
    pthread_mutex_lock(&cl->lock);
    const char *owner = NULL;
    for (size_t i = 0; i < cl->nranges; i++) {
        if (strncmp(md5hex, cl->ranges[i].start, 32) >= 0 &&
            (strncmp(md5hex, cl->ranges[i].end, 32) < 0 ||
             memcmp(cl->ranges[i].end,
                    "ffffffffffffffffffffffffffffffff", 32) == 0)) {
            owner = cl->ranges[i].node_id;
            break;
        }
    }

    const char *candidates[MAX_PEERS];
    size_t ncandidates = 0;
    for (size_t i = 0; i < cl->npeers && ncandidates < MAX_PEERS; i++) {
        candidates[ncandidates++] = cl->peers[i].id;
    }
    for (size_t i = 1; i < ncandidates; i++) {
        const char *candidate = candidates[i];
        size_t j = i;
        while (j > 0 && strcmp(candidates[j - 1], candidate) > 0) {
            candidates[j] = candidates[j - 1];
            j--;
        }
        candidates[j] = candidate;
    }

    size_t start = 0;
    if (owner) {
        for (size_t i = 0; i < ncandidates; i++) {
            if (strcmp(candidates[i], owner) == 0) {
                start = i;
                break;
            }
        }
    }
    size_t count = 0;
    for (size_t offset = 0; offset < ncandidates && count < cap; offset++) {
        size_t index = (start + offset) % ncandidates;
        snprintf(node_ids[count], ZDB_NODE_ID_MAX, "%s", candidates[index]);
        count++;
    }
    pthread_mutex_unlock(&cl->lock);
    return count;
}


/* One-shot join: dial the seed, exchange HELLO+STATE both ways so both
 * membership views merge, then hang up (the maintainer re-dials).
 * Returns 0 on success, -2 when the seed refused because a rebalance
 * wave is pending. */
static int join_exchange(zdb_cluster *cl, int fd)
{
    send_hello_state(cl, fd, true);

    /* read frames for a short while to absorb their state */
    time_t deadline = time(NULL) + 2;
    while (time(NULL) < deadline) {
        char *payload = NULL;
        int t = zstp_recv(fd, &payload);
        if (t < 0) {
            break;
        }
        if (t == ZSTP_HELLO && payload) {
            cJSON *h = cJSON_Parse(payload);
            const cJSON *jr =
                h ? cJSON_GetObjectItemCaseSensitive(h, "reject") : NULL;
            if (cJSON_IsString(jr) && jr->valuestring) {
                cJSON_Delete(h);
                free(payload);
                return -2;
            }
            cJSON_Delete(h);
        } else if (t == ZSTP_STATE && payload) {
            cJSON *doc = cJSON_Parse(payload);
            if (doc) {
                pthread_mutex_lock(&cl->lock);
                bool changed = merge_state(cl, doc);
                recompute_leader(cl);
                pthread_mutex_unlock(&cl->lock);
                cJSON_Delete(doc);
                leader_react(cl, changed);
            }
        }
        free(payload);
    }
    return 0;
}

int zdb_cluster_join(zdb_cluster *cl, const char *seed_addr, int seed_port)
{
    if (!cl || !seed_addr || !*seed_addr || seed_port <= 0 ||
        seed_port > 65535) {
        return -1;
    }
    int fd = dial_peer(seed_addr, seed_port);
    if (fd < 0) {
        return -1;
    }
    set_socket_timeouts(fd, 2000);
    int rc = join_exchange(cl, fd);
    close(fd);
    return rc;
}

void zdb_cluster_set_dispatcher(zdb_cluster *cl,
                                zstp_dispatch_fn fn, void *ctx)
{
    if (!cl) {
        return;
    }
    pthread_mutex_lock(&cl->lock);
    cl->dispatch = NULL;
    cl->dispatch_ctx = NULL;
    while (cl->dispatch_inflight > 0) {
        pthread_cond_wait(&cl->conn_cv, &cl->lock);
    }
    cl->dispatch = fn;
    cl->dispatch_ctx = ctx;
    pthread_mutex_unlock(&cl->lock);
}

size_t zdb_cluster_target_ranges(zdb_cluster *cl, zdb_range_info *out,
                                 size_t cap)
{
    if (!cl || !out) {
        return 0;
    }
    pthread_mutex_lock(&cl->lock);
    size_t n = 0;
    for (size_t i = 0; i < cl->ntarget_ranges && n < cap; i++) {
        out[n++] = cl->target_ranges[i];
    }
    pthread_mutex_unlock(&cl->lock);
    return n;
}

long long zdb_cluster_target_generation(zdb_cluster *cl)
{
    if (!cl) {
        return 0;
    }
    pthread_mutex_lock(&cl->lock);
    long long g = cl->target_generation;
    pthread_mutex_unlock(&cl->lock);
    return g;
}

const char *zdb_cluster_target_owner(zdb_cluster *cl, const char *md5hex)
{
    static _Thread_local char owner_buf[ZDB_NODE_ID_MAX];
    if (!cl || !md5hex || strlen(md5hex) != 32) {
        return NULL;
    }
    bool found = false;
    pthread_mutex_lock(&cl->lock);
    for (size_t i = 0; i < cl->ntarget_ranges; i++) {
        if (strncmp(md5hex, cl->target_ranges[i].start, 32) >= 0 &&
            (strncmp(md5hex, cl->target_ranges[i].end, 32) < 0 ||
             memcmp(cl->target_ranges[i].end,
                    "ffffffffffffffffffffffffffffffff", 32) == 0)) {
            /* copy out: see zdb_cluster_owner for why a bare pointer
             * into the (mutable) range table would dangle */
            snprintf(owner_buf, sizeof(owner_buf), "%s",
                     cl->target_ranges[i].node_id);
            found = true;
            break;
        }
    }
    pthread_mutex_unlock(&cl->lock);
    return found ? owner_buf : NULL;
}

bool zdb_cluster_promote_target(zdb_cluster *cl)
{
    if (!cl || zdb_cluster_target_generation(cl) == 0 ||
        !zdb_cluster_target_compliant(cl)) {
        return false;
    }
    bool promoted = false;
    pthread_mutex_lock(&cl->lock);
    if (leader_is_self_locked(cl) && cl->target_generation > 0) {
        promote_target_locked(cl);
        for (size_t i = 0; i < cl->npeers; i++) {
            char name[96];
            snprintf(name, sizeof(name), "%s%.63s", SETTING_DONE_PREFIX,
                     cl->peers[i].id);
            zdb_setting_delete(cl->cfg, name);
        }
        zdb_setting_delete(cl->cfg, SETTING_LOCK);
        persist_state(cl);
        promoted = true;
    }
    pthread_mutex_unlock(&cl->lock);
    if (promoted) {
        gossip_state(cl);
    }
    return promoted;
}

bool zdb_cluster_target_compliant(zdb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    pthread_mutex_lock(&cl->lock);
    long long target_generation = cl->target_generation;
    char required[MAX_PEERS][ZDB_NODE_ID_MAX];
    size_t nrequired = 0;
    for (size_t i = 0; target_generation > 0 && i < cl->ntarget_ranges &&
                       nrequired < MAX_PEERS;
         i++) {
        bool duplicate = false;
        for (size_t j = 0; j < nrequired; j++) {
            if (strcmp(required[j], cl->target_ranges[i].node_id) == 0) {
                duplicate = true;
                break;
            }
        }
        if (!duplicate) {
            snprintf(required[nrequired], ZDB_NODE_ID_MAX, "%s",
                     cl->target_ranges[i].node_id);
            nrequired++;
        }
    }
    pthread_mutex_unlock(&cl->lock);
    if (target_generation == 0 || nrequired == 0) {
        return false;
    }

    zdb_peer_info peers[MAX_PEERS];
    size_t npeers = zdb_cluster_peers(cl, peers, MAX_PEERS);
    for (size_t i = 0; i < nrequired; i++) {
        bool compliant = false;
        for (size_t p = 0; p < npeers; p++) {
            if (strcmp(peers[p].id, required[i]) == 0 &&
                peers[p].compliant_gen >= target_generation) {
                compliant = true;
                break;
            }
        }
        if (compliant) {
            continue;
        }
        char name[96];
        snprintf(name, sizeof(name), "%s%.63s", SETTING_DONE_PREFIX,
                 required[i]);
        char *value = zdb_setting_get(cl->cfg, name);
        if (value) {
            char *end = NULL;
            long long stored_generation = strtoll(value, &end, 10);
            compliant = end && *end == '\0' &&
                        stored_generation >= target_generation;
        }
        free(value);
        if (!compliant) {
            return false;
        }
    }
    return true;
}

void zdb_cluster_mark_compliant(zdb_cluster *cl)
{
    if (!cl) {
        return;
    }
    long long tgen = zdb_cluster_target_generation(cl);
    if (tgen == 0) {
        tgen = zdb_cluster_generation(cl);
    }
    if (tgen == 0) {
        return;
    }
    /* gossip-based compliance: record self as current for this target */
    pthread_mutex_lock(&cl->lock);
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, cl->self_id) == 0) {
            if (cl->peers[i].compliant_gen < tgen) {
                cl->peers[i].compliant_gen = tgen;
            }
            break;
        }
    }
    pthread_mutex_unlock(&cl->lock);

    /* keep the persisted flag for crash recovery / stage 6a compat */
    char name[96];
    char val[32];
    snprintf(name, sizeof(name), "%s%.63s", SETTING_DONE_PREFIX,
             cl->self_id);
    snprintf(val, sizeof(val), "%lld", tgen);
    zdb_setting_set(cl->cfg, name, val);

    /* propagate compliance immediately so the leader can promote without
     * waiting for the next heartbeat */
    gossip_state(cl);
}

/* --- stage 6d: promotion trigger + shard GC -------------------------- */

/* Leader-only: promote the pending target when every online node has
 * reported compliance. Safe to call from any thread (takes the lock).
 * Returns true when a promotion happened. */
bool zdb_cluster_maybe_promote(zdb_cluster *cl)
{
    if (!cl || !zdb_cluster_is_leader(cl)) {
        return false;
    }
    if (zdb_cluster_target_generation(cl) == 0 ||
        !zdb_cluster_target_compliant(cl)) {
        return false;
    }
    return zdb_cluster_promote_target(cl);
}

/* Removes one redundant local shard file (a shard whose key is now owned
 * by a different node under the live table) after confirming the new
 * owner is assigned. Reserved __system__ config shards are never removed.
 * Returns the number of shards GC'd this call (0 or 1), so callers can
 * drain GC one at a time. */
size_t zdb_cluster_gc_redundant(zdb_cluster *cl)
{
    if (!cl || zdb_cluster_target_generation(cl) != 0) {
        return 0;
    }
    zdb_engine *engine = zdb_config_engine(cl->cfg);
    if (!engine) {
        return 0;
    }
    char keys[256][33];
    size_t key_count = zdb_engine_shard_keys(engine, keys, 256);
    size_t keyspace_count = 0;
    zdb_keyspace_info *keyspaces = zdb_keyspace_list(cl->cfg,
                                                     &keyspace_count);
    zdb_peer_info peers[MAX_PEERS];
    size_t peer_count = zdb_cluster_peers(cl, peers, MAX_PEERS);

    for (size_t i = 0; i < key_count; i++) {
        if (zdb_config_is_system_key(cl->cfg, keys[i])) {
            continue;
        }
        int rf = 1;
        bool registered = false;
        for (size_t k = 0; k < keyspace_count; k++) {
            char path[1024];
            char candidate[33];
            if (zdb_shard_path(engine, keyspaces[k].partition,
                               keyspaces[k].name, path, sizeof(path),
                               candidate) && strcmp(candidate, keys[i]) == 0) {
                zdb_database_info database;
                if (zdb_database_get(cl->cfg, keyspaces[k].database,
                                     &database) &&
                    database.replication_factor > 0) {
                    rf = database.replication_factor;
                }
                registered = true;
                break;
            }
        }
        if (!registered) {
            rf = 1;
        }

        char holders[MAX_PEERS][ZDB_NODE_ID_MAX];
        size_t holder_count = zdb_cluster_holders(cl, keys[i], holders,
                                                  MAX_PEERS);
        if ((size_t)rf > holder_count) {
            rf = (int)holder_count;
        }
        bool keep_local = false;
        bool replicas_ready = rf > 0;
        for (int h = 0; h < rf; h++) {
            if (strcmp(holders[h], cl->self_id) == 0) {
                keep_local = true;
                break;
            }
            bool ready = false;
            for (size_t p = 0; p < peer_count; p++) {
                if (strcmp(peers[p].id, holders[h]) == 0 && peers[p].online) {
                    ready = true;
                    break;
                }
            }
            if (!ready) {
                replicas_ready = false;
                break;
            }
        }
        if (!keep_local && replicas_ready && zdb_shard_gc(engine, keys[i])) {
            free(keyspaces);
            return 1;
        }
    }
    free(keyspaces);
    return 0;
}

bool zdb_cluster_acquire_rebalance_lock(zdb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    char *cur = zdb_setting_get(cl->cfg, SETTING_LOCK);
    if (cur) {
        cJSON *doc = cJSON_Parse(cur);
        free(cur);
        const cJSON *jid =
            doc ? cJSON_GetObjectItemCaseSensitive(doc, "node") : NULL;
        const cJSON *jts =
            doc ? cJSON_GetObjectItemCaseSensitive(doc, "ts") : NULL;
        long long ts =
            cJSON_IsNumber(jts) ? (long long)jts->valuedouble : 0;
        bool mine = cJSON_IsString(jid) && jid->valuestring &&
                    strcmp(jid->valuestring, cl->self_id) == 0;
        bool stale = ts > 0 && epoch_now() - ts > 30;
        cJSON_Delete(doc);
        if (!mine && !stale) {
            return false;
        }
        if (!mine && stale) {
            /* fall through and take over */
        } else if (mine) {
            return true;   /* already ours */
        }
    }
    char json[128];
    snprintf(json, sizeof(json),
             "{\"node\":\"%.63s\",\"ts\":%lld}", cl->self_id,
             epoch_now());
    return zdb_setting_set(cl->cfg, SETTING_LOCK, json);
}

void zdb_cluster_release_rebalance_lock(zdb_cluster *cl)
{
    if (!cl) {
        return;
    }
    zdb_setting_delete(cl->cfg, SETTING_LOCK);
}

/* --- stage 6e: end-to-end rebalance wiring ---------------------------- */

bool zdb_cluster_needs_sync(zdb_cluster *cl)
{
    if (!cl || zdb_cluster_target_generation(cl) == 0) {
        return false;
    }
    zdb_engine *engine = zdb_config_engine(cl->cfg);
    if (!engine) {
        return false;
    }
    size_t nks = 0;
    zdb_keyspace_info *kss = zdb_keyspace_list(cl->cfg, &nks);
    bool needed = false;
    for (size_t i = 0; kss && i < nks && !needed; i++) {
        char key[33];
        char path[1024];
        if (!zdb_shard_path(engine, kss[i].partition, kss[i].name,
                            path, sizeof(path), key)) {
            continue;
        }
        if (zdb_config_is_system_key(cl->cfg, key)) {
            continue;   /* config shards replicate separately */
        }
        const char *towner = zdb_cluster_target_owner(cl, key);
        if (!towner || strcmp(towner, cl->self_id) != 0) {
            continue;   /* the wave does not give us this shard */
        }
        if (zdb_shard_validate(engine, kss[i].partition, kss[i].name)) {
            continue;
        }
        needed = true;
    }
    free(kss);
    return needed;
}

void zdb_cluster_set_auto_compliant(zdb_cluster *cl, bool enabled)
{
    if (!cl) {
        return;
    }
    pthread_mutex_lock(&cl->lock);
    cl->auto_compliant = enabled;
    pthread_mutex_unlock(&cl->lock);
}

bool zdb_cluster_void_target(zdb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    bool voided = false;
    pthread_mutex_lock(&cl->lock);
    if (leader_is_self_locked(cl) && cl->target_generation > 0) {
        cl->target_generation = 0;
        cl->ntarget_ranges = 0;
        for (size_t i = 0; i < cl->npeers; i++) {
            cl->peers[i].compliant_gen = 0;
        }
        voided = true;
    }
    pthread_mutex_unlock(&cl->lock);
    if (voided) {
        /* back to the live structure: release the lock and let every
         * peer drop its stale pending copy via gossip */
        zdb_cluster_release_rebalance_lock(cl);
        pthread_mutex_lock(&cl->lock);
        persist_state(cl);
        pthread_mutex_unlock(&cl->lock);
        gossip_state(cl);
    }
    return voided;
}

bool zdb_cluster_request_void(zdb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    char leader[ZDB_NODE_ID_MAX] = "";
    char addr[ZDB_ADDR_MAX] = "";
    int port = 0;
    pthread_mutex_lock(&cl->lock);
    snprintf(leader, sizeof(leader), "%s", cl->leader);
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, leader) == 0) {
            snprintf(addr, sizeof(addr), "%s", cl->peers[i].addr);
            port = cl->peers[i].port;
            break;
        }
    }
    pthread_mutex_unlock(&cl->lock);
    if (!leader[0] || !addr[0] || port <= 0 ||
        strcmp(leader, cl->self_id) == 0) {
        /* no reachable leader (or we are it): void locally so a leader
         * joiner still rolls back instead of hanging on its own RPC */
        return zdb_cluster_void_target(cl);
    }
    int fd = dial_peer(addr, port);
    if (fd < 0) {
        return false;
    }
    char hello[192];
    snprintf(hello, sizeof(hello),
             "{\"node_id\":\"ephemeral\",\"addr\":\"%s\",\"port\":0}",
             cl->self_addr);
    zstp_send(fd, ZSTP_HELLO, hello, NULL);

    /* absorb their HELLO/STATE before asking */
    for (;;) {
        char *payload = NULL;
        int t = zstp_recv(fd, &payload);
        free(payload);
        if (t < 0 || t == ZSTP_STATE) {
            break;
        }
    }
    char req[64];
    long long tgen = zdb_cluster_target_generation(cl);
    snprintf(req, sizeof(req), "{\"generation\":%lld}", tgen);
    zstp_send(fd, ZSTP_VOID, req, NULL);
    bool ok = false;
    char *payload = NULL;
    int t = zstp_recv(fd, &payload);
    if (t == ZSTP_ACK && payload) {
        cJSON *doc = cJSON_Parse(payload);
        const cJSON *jok =
            doc ? cJSON_GetObjectItemCaseSensitive(doc, "ok") : NULL;
        ok = cJSON_IsTrue(jok);
        cJSON_Delete(doc);
    }
    free(payload);
    close(fd);
    return ok;
}
