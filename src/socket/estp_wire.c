/* estp_wire.c - ESTP frame codec shared between the cluster mesh and the
 * replication/snapshot layers. Extracted from the cluster module; see
 * epsilon_cluster_internal.h.
 */

#include <errno.h>
#include <arpa/inet.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <fcntl.h>
#include <poll.h>
#include <pthread.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "../../vendor/cjson/cJSON.h"
#include "../engine/epsilon_engine.h"
#include "../engine/epsilon_crypto.h"
#include "../engine/random.h"
#include "epsilon_cluster_internal.h"
#include "estp_wire.h"

#define PEER_IO_TIMEOUT_MS    15000
#define PEER_CONNECT_TIMEOUT_MS 1500
long long epoch_now(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_REALTIME, &ts);
    return (long long)ts.tv_sec;
}

void sleep_ms(int ms)
{
    struct timespec ts = { .tv_sec = ms / 1000,
                           .tv_nsec = (long)(ms % 1000) * 1000000L };
    nanosleep(&ts, NULL);
}

void set_tcp_nodelay(int fd)
{
    int one = 1;
    setsockopt(fd, IPPROTO_TCP, TCP_NODELAY, &one, sizeof(one));
}

void set_socket_timeouts(int fd, int ms)
{
    struct timeval timeout = { .tv_sec = ms / 1000,
                               .tv_usec = (ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &timeout, sizeof(timeout));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &timeout, sizeof(timeout));
}

/* ------------------------------------------------------------------ */
/* cluster state                                                       */

/* ------------------------------------------------------------------ */
/* wire codec                                                          */

estp_dispatch_fn g_dispatcher;
void *g_dispatcher_ctx;
pthread_mutex_t g_dispatch_lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t g_dispatch_done = PTHREAD_COND_INITIALIZER;
size_t g_dispatch_inflight;

int write_full(int fd, const void *buf, size_t len)
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

int read_full(int fd, void *buf, size_t len)
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

bool estp_type_valid(int type)
{
    return type >= ESTP_HELLO && type <= ESTP_VOID;
}

void estp_set_dispatcher(estp_dispatch_fn fn, void *ctx)
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

/* ------------------------------------------------------------------ */
/* mesh encryption key (process-wide)                                  */

static struct {
    bool active;
    uint8_t enc_key[32];
    uint8_t mac_key[32];
} g_mesh_key;
static pthread_mutex_t g_mesh_key_lock = PTHREAD_MUTEX_INITIALIZER;

void estp_set_mesh_key(const uint8_t enc_key[32], const uint8_t mac_key[32])
{
    pthread_mutex_lock(&g_mesh_key_lock);
    if (enc_key && mac_key) {
        memcpy(g_mesh_key.enc_key, enc_key, 32);
        memcpy(g_mesh_key.mac_key, mac_key, 32);
        g_mesh_key.active = true;
    } else {
        memset(g_mesh_key.enc_key, 0, 32);
        memset(g_mesh_key.mac_key, 0, 32);
        g_mesh_key.active = false;
    }
    pthread_mutex_unlock(&g_mesh_key_lock);
}

static bool mesh_key_snapshot(uint8_t enc_key[32], uint8_t mac_key[32])
{
    bool active;
    pthread_mutex_lock(&g_mesh_key_lock);
    active = g_mesh_key.active;
    if (active) {
        memcpy(enc_key, g_mesh_key.enc_key, 32);
        memcpy(mac_key, g_mesh_key.mac_key, 32);
    }
    pthread_mutex_unlock(&g_mesh_key_lock);
    return active;
}

int estp_send_frame_raw(int fd, estp_type type, const void *data, size_t len,
                        void *send_lock)
{
    if (!estp_type_valid((int)type) || len > ESTP_MAX_PAYLOAD) {
        return -1;
    }
    unsigned char hdr[ESTP_HEADER_SIZE];
    hdr[0] = 'Z';
    hdr[1] = 'S';
    hdr[2] = 'T';
    hdr[3] = 'P';
    hdr[4] = ESTP_VERSION;
    hdr[5] = (unsigned char)type;

    uint8_t enc_key[32];
    uint8_t mac_key[32];
    bool encrypted = mesh_key_snapshot(enc_key, mac_key);

    /* Body is either the raw payload (plaintext) or
     * nonce || ciphertext || tag (encrypted). */
    uint8_t *body = NULL;
    size_t body_len = len;
    if (encrypted) {
        uint8_t nonce[EDB_NONCE_LEN];
        edb_random_bytes(nonce, sizeof(nonce));
        body_len = EDB_NONCE_LEN + len + EDB_TAG_LEN;
        body = malloc(body_len ? body_len : 1);
        if (!body) {
            return -1;
        }
        if (edb_aead_seal(enc_key, mac_key, nonce, hdr + 4, 2,
                          (const uint8_t *)data, len, body, body_len) != 0) {
            free(body);
            return -1;
        }
    } else {
        body = (uint8_t *)data;
    }

    uint32_t be = htonl((uint32_t)body_len);
    memcpy(hdr + 6, &be, 4);

    int rc = -1;
    if (send_lock) {
        pthread_mutex_lock(send_lock);
    }
    if (write_full(fd, hdr, sizeof(hdr)) == 0 &&
        (body_len == 0 || write_full(fd, body, body_len) == 0)) {
        rc = 0;
    }
    if (send_lock) {
        pthread_mutex_unlock(send_lock);
    }
    if (encrypted) {
        free(body);
    }
    return rc;
}

int estp_send_frame(int fd, estp_type type, const char *json,
                    void *send_lock)
{
    return estp_send_frame_raw(fd, type, json, json ? strlen(json) : 0,
                               send_lock);
}

int estp_recv_frame_raw(int fd, char **payload_out, uint32_t *plen_out)
{
    *payload_out = NULL;
    if (plen_out) {
        *plen_out = 0;
    }
    unsigned char hdr[ESTP_HEADER_SIZE];
    if (read_full(fd, hdr, sizeof(hdr)) != 0) {
        return -1;
    }
    if (hdr[0] != 'Z' || hdr[1] != 'S' || hdr[2] != 'T' ||
        hdr[3] != 'P' || hdr[4] != ESTP_VERSION ||
        !estp_type_valid(hdr[5])) {
        return -1;
    }
    uint32_t be;
    memcpy(&be, hdr + 6, 4);
    uint32_t body_len = ntohl(be);
    if (body_len > ESTP_MAX_PAYLOAD + EDB_NONCE_LEN + EDB_TAG_LEN) {
        return -1;
    }

    uint8_t *body = NULL;
    if (body_len) {
        body = malloc(body_len);
        if (!body) {
            return -1;
        }
        if (read_full(fd, body, body_len) != 0) {
            free(body);
            return -1;
        }
    }

    uint8_t enc_key[32];
    uint8_t mac_key[32];
    bool encrypted = mesh_key_snapshot(enc_key, mac_key);

    char *payload = NULL;
    uint32_t plen = body_len;
    if (encrypted) {
        if (body_len < EDB_NONCE_LEN + EDB_TAG_LEN) {
            free(body);
            return -1;
        }
        plen = body_len - EDB_NONCE_LEN - EDB_TAG_LEN;
        payload = malloc((size_t)plen + 1);
        if (!payload) {
            free(body);
            return -1;
        }
        if (edb_aead_open(enc_key, mac_key, hdr + 4, 2, body, body_len,
                          (uint8_t *)payload, plen) < 0) {
            free(payload);
            free(body);
            return -1;
        }
        payload[plen] = '\0';
        free(body);
    } else {
        payload = (char *)body;
        if (payload && plen) {
            char *nul = realloc(payload, (size_t)plen + 1);
            if (!nul) {
                free(payload);
                return -1;
            }
            payload = nul;
            payload[plen] = '\0';
        }
    }

    *payload_out = payload;
    if (plen_out) {
        *plen_out = plen;
    }
    return hdr[5];
}

int estp_recv_frame(int fd, char **payload_out)
{
    return estp_recv_frame_raw(fd, payload_out, NULL);
}

int estp_dial(const char *addr, int port)
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

int estp_send(int fd, estp_type type, const char *json,
                     pthread_mutex_t *send_lock)
{
    return estp_send_frame(fd, type, json, send_lock);
}

int estp_recv(int fd, char **payload_out)
{
    return estp_recv_frame(fd, payload_out);
}
