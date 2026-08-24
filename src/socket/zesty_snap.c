/* zesty_snap.c - stage 6b shard snapshot transfer. See zesty_snap.h.
 *
 * Transport: a dedicated short-lived peer connection per snapshot,
 * mirroring the replication layer's one-shot RPC style:
 *   dial -> HELLO -> SNAP_REQ {key} -> SNAP_DATA* (raw bytes, empty
 *   frame = EOF) -> SNAP_ACK {ok:bool} -> close
 *
 * Sender side: the shard is copied with the sqlite3_backup_* online
 * backup API into a temp file in the data directory before any byte
 * leaves the node. This gives a transactionally consistent copy even
 * while writes continue (shard files use journal_mode=DELETE +
 * synchronous=FULL precisely so this works), and it never holds the
 * shard mutex during network I/O.
 *
 * Receiver side: bytes land in "<key>.sqlite.incoming" and are renamed
 * over the live path only after the full transfer arrived. The engine's
 * cached handle is invalidated afterwards so subsequent reads reopen
 * from disk; zdb_shard_invalidate runs PRAGMA integrity_check on the
 * result and reports failure through ok:false in the final ack.
 */

#include "zesty_snap.h"

#include <errno.h>
#include <inttypes.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <sys/stat.h>
#include <sys/statvfs.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#include "../../vendor/cjson/cJSON.h"
#include "../engine/zesty_engine.h"
#include "../sqlite/sqlite3.h"
#include "zstp_wire.h"

#define SNAP_CONNECT_TIMEOUT_MS 1500
#define SNAP_IO_DEADLINE_MS    60000
#define SNAP_MAX_BYTES         (4ULL * 1024 * 1024 * 1024)

/* ------------------------------------------------------------------ */
/* helpers                                                             */

static long long mono_ms(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000 + ts.tv_nsec / 1000000;
}

static void set_socket_timeouts(int fd, int ms)
{
    struct timeval tv = { .tv_sec = ms / 1000,
                          .tv_usec = (ms % 1000) * 1000 };
    setsockopt(fd, SOL_SOCKET, SO_RCVTIMEO, &tv, sizeof(tv));
    setsockopt(fd, SOL_SOCKET, SO_SNDTIMEO, &tv, sizeof(tv));
}

static char *json_print(const cJSON *doc)
{
    return doc ? cJSON_PrintUnformatted(doc) : NULL;
}

static bool valid_key(const char *key)
{
    if (!key || strlen(key) != 32) {
        return false;
    }
    for (size_t i = 0; i < 32; i++) {
        if (!((key[i] >= '0' && key[i] <= '9') ||
              (key[i] >= 'a' && key[i] <= 'f'))) {
            return false;
        }
    }
    return true;
}

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

/* Builds the 10-byte ZSTP header into a thread-local buffer. */
static unsigned char *make_header(zstp_type type, uint32_t len)
{
    static _Thread_local unsigned char hdr[ZSTP_HEADER_SIZE];
    hdr[0] = 'Z';
    hdr[1] = 'S';
    hdr[2] = 'T';
    hdr[3] = 'P';
    hdr[4] = ZSTP_VERSION;
    hdr[5] = (unsigned char)type;
    hdr[6] = (unsigned char)(len >> 24);
    hdr[7] = (unsigned char)(len >> 16);
    hdr[8] = (unsigned char)(len >> 8);
    hdr[9] = (unsigned char)len;
    return hdr;
}

/* True when the shard file does not exist (so an empty snapshot is the
 * right answer rather than an error). */
static bool shard_missing(const char *dir, const char *key)
{
    char path[1024];
    snprintf(path, sizeof(path), "%s/%s.sqlite", dir, key);
    struct stat st;
    return stat(path, &st) != 0 && errno == ENOENT;
}

/* ------------------------------------------------------------------ */
/* sender: serve SNAP_REQ frames                                       */

/* Copies the sqlite database at src_path to dst_path with the online
 * backup API. Returns SQLITE_OK on success. */
static int backup_shard_file(const char *src_path, const char *dst_path)
{
    sqlite3 *src = NULL;
    sqlite3 *dst = NULL;
    sqlite3_backup *bak = NULL;
    int rc = sqlite3_open_v2(src_path, &src,
                             SQLITE_OPEN_READONLY | SQLITE_OPEN_FULLMUTEX,
                             NULL);
    if (rc != SQLITE_OK) {
        /* CANTOPEN means there is no shard file to copy: the caller
         * treats that as an empty (but valid) snapshot */
        if (src) {
            sqlite3_close(src);
            src = NULL;
        }
        rc = SQLITE_CANTOPEN;
        goto done;
    }
    rc = sqlite3_open_v2(dst_path, &dst,
                         SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE, NULL);
    if (rc != SQLITE_OK) {
        goto done;
    }
    sqlite3_busy_timeout(src, 5000);
    sqlite3_busy_timeout(dst, 5000);
    bak = sqlite3_backup_init(dst, "main", src, "main");
    if (!bak) {
        rc = sqlite3_errcode(dst);
        goto done;
    }
    /* -1 pages = whole database in one step; the busy timeouts above
     * let concurrent writers win briefly without failing the copy */
    for (;;) {
        int src_rc = sqlite3_backup_step(bak, -1);
        if (src_rc == SQLITE_DONE) {
            rc = SQLITE_OK;
            break;
        }
        if (src_rc != SQLITE_OK && src_rc != SQLITE_BUSY &&
            src_rc != SQLITE_LOCKED) {
            rc = src_rc;
            break;
        }
    }
    sqlite3_backup_finish(bak);
    bak = NULL;

done:
    if (bak) {
        sqlite3_backup_finish(bak);
    }
    if (src) {
        sqlite3_close(src);
    }
    if (dst) {
        /* finish any open transaction cleanly before handing out bytes */
        sqlite3_exec(dst, "COMMIT;", NULL, NULL, NULL);
        int crc = sqlite3_close(dst);
        if (crc != SQLITE_OK && rc == SQLITE_OK) {
            rc = crc;
        }
    }
    return rc;
}

void zdb_snap_serve(int fd, uint32_t payload_len, const char *payload,
                    zdb_engine *engine)
{
    cJSON *req = payload ? cJSON_ParseWithLength(payload, payload_len)
                         : NULL;
    const cJSON *jkey =
        req ? cJSON_GetObjectItemCaseSensitive(req, "key") : NULL;
    const cJSON *jallow_missing =
        req ? cJSON_GetObjectItemCaseSensitive(req, "allow_missing") : NULL;
    const char *key = cJSON_IsString(jkey) && valid_key(jkey->valuestring)
                          ? jkey->valuestring
                          : NULL;
    bool allow_missing = cJSON_IsTrue(jallow_missing);
    bool ok = false;
    bool sent_eof = false;
    char tmp[1100] = "";

    do {
        const char *dir = zdb_engine_path(engine);
        if (!engine || !key || !dir) {
            break;
        }

        /* resolve the shard file path and make a consistent copy of it;
         * a missing shard is an empty but valid snapshot */
        char src_path[1100];
        snprintf(src_path, sizeof(src_path), "%s/%s.sqlite", dir, key);
        if (snprintf(tmp, sizeof(tmp), "%s/.%s.snapshot.XXXXXX", dir,
                     key) >= (int)sizeof(tmp)) {
            break;
        }
        int tmp_fd = mkstemp(tmp);
        if (tmp_fd < 0) {
            break;
        }
        close(tmp_fd);
        int brc = backup_shard_file(src_path, tmp);
        if (brc != SQLITE_OK) {
            if (allow_missing && shard_missing(dir, key)) {
                ok = true;
            }
            break;
        }

        struct stat snapshot_stat;
        if (stat(tmp, &snapshot_stat) != 0 || snapshot_stat.st_size < 0 ||
            (uint64_t)snapshot_stat.st_size > SNAP_MAX_BYTES) {
            break;
        }
        FILE *f = fopen(tmp, "rb");
        if (!f) {
            break;
        }
        cJSON *meta = cJSON_CreateObject();
        if (!meta) {
            fclose(f);
            break;
        }
        cJSON_AddBoolToObject(meta, "ok", true);
        cJSON_AddNumberToObject(meta, "size", (double)snapshot_stat.st_size);
        char *meta_str = json_print(meta);
        cJSON_Delete(meta);
        if (!meta_str ||
            write_full(fd, make_header(ZSTP_SNAP_ACK, strlen(meta_str)),
                       ZSTP_HEADER_SIZE) != 0 ||
            write_full(fd, meta_str, strlen(meta_str)) != 0) {
            free(meta_str);
            fclose(f);
            break;
        }
        free(meta_str);
        ok = true;

        static _Thread_local unsigned char chunk[ZSTP_MAX_PAYLOAD];
        for (;;) {
            size_t n = fread(chunk, 1, sizeof(chunk), f);
            if (n == 0) {
                break;
            }
            if (write_full(fd, make_header(ZSTP_SNAP_DATA, n),
                           ZSTP_HEADER_SIZE) != 0 ||
                write_full(fd, chunk, n) != 0) {
                ok = false;
                break;
            }
            if (n < sizeof(chunk)) {
                break;
            }
        }
        fclose(f);

        /* EOF marker terminates the data stream */
        sent_eof = write_full(fd, make_header(ZSTP_SNAP_DATA, 0),
                              ZSTP_HEADER_SIZE) == 0;
        ok = ok && sent_eof;
        unlink(tmp);
        tmp[0] = '\0';
    } while (0);
    if (tmp[0]) {
        unlink(tmp);
    }

    /* paths that skipped the data loop still owe the receiver metadata
     * and an EOF marker before the final acknowledgement */
    if (ok && !sent_eof) {
        const char *meta = "{\"ok\":true,\"size\":0}";
        ok = write_full(fd, make_header(ZSTP_SNAP_ACK, strlen(meta)),
                        ZSTP_HEADER_SIZE) == 0 &&
             write_full(fd, meta, strlen(meta)) == 0 &&
             write_full(fd, make_header(ZSTP_SNAP_DATA, 0),
                        ZSTP_HEADER_SIZE) == 0;
    }

    cJSON *ack = cJSON_CreateObject();
    if (ack) {
        cJSON_AddBoolToObject(ack, "ok", ok);
        char *ack_str = json_print(ack);
        cJSON_Delete(ack);
        if (ack_str) {
            write_full(fd, make_header(ZSTP_SNAP_ACK, strlen(ack_str)),
                       ZSTP_HEADER_SIZE);
            write_full(fd, ack_str, strlen(ack_str));
            free(ack_str);
        }
    }
    cJSON_Delete(req);
}

/* ------------------------------------------------------------------ */
/* receiver: pull a snapshot                                           */

static int snap_fetch(const char *addr, int port, const char key[33],
                      const char *dest_dir, bool allow_missing)
{
    if (!addr || !valid_key(key) || !dest_dir || port <= 0) {
        return -1;
    }

    int fd = zstp_dial(addr, port);
    if (fd < 0) {
        return -1;
    }
    set_socket_timeouts(fd, SNAP_IO_DEADLINE_MS);

    int rc = -1;
    char incoming[1100] = "";
    char final_path[1100];
    FILE *out = NULL;
    if (snprintf(incoming, sizeof(incoming), "%s/.%s.incoming.XXXXXX",
                 dest_dir, key) >= (int)sizeof(incoming) ||
        snprintf(final_path, sizeof(final_path), "%s/%s.sqlite", dest_dir,
                 key) >= (int)sizeof(final_path)) {
        goto done;
    }

    cJSON *hello = cJSON_CreateObject();
    if (!hello) {
        goto done;
    }
    cJSON_AddStringToObject(hello, "node_id", "ephemeral");
    char *hello_str = json_print(hello);
    cJSON_Delete(hello);
    if (!hello_str ||
        zstp_send_frame(fd, ZSTP_HELLO, hello_str, NULL) != 0) {
        free(hello_str);
        goto done;
    }
    free(hello_str);

    cJSON *req = cJSON_CreateObject();
    if (!req) {
        goto done;
    }
    cJSON_AddStringToObject(req, "key", key);
    cJSON_AddBoolToObject(req, "allow_missing", allow_missing);
    char *req_str = json_print(req);
    cJSON_Delete(req);
    if (!req_str || zstp_send_frame(fd, ZSTP_SNAP_REQ, req_str, NULL) != 0) {
        free(req_str);
        goto done;
    }
    free(req_str);

    int out_fd = mkstemp(incoming);
    if (out_fd < 0) {
        goto done;
    }
    out = fdopen(out_fd, "wb");
    if (!out) {
        close(out_fd);
        goto done;
    }
    setvbuf(out, NULL, _IONBF, 0);

    long long deadline = mono_ms() + SNAP_IO_DEADLINE_MS;
    bool metadata_seen = false;
    bool eof_seen = false;
    bool ack_ok = false;
    uint64_t expected_size = 0;
    uint64_t received_size = 0;
    for (;;) {
        if (mono_ms() >= deadline) {
            goto done;
        }
        char *frame = NULL;
        uint32_t frame_len = 0;
        int type = zstp_recv_frame_raw(fd, &frame, &frame_len);
        if (type < 0) {
            free(frame);
            goto done;
        }
        if (!metadata_seen && (type == ZSTP_HELLO || type == ZSTP_STATE)) {
            free(frame);
            continue;
        }
        if (type == ZSTP_SNAP_ACK) {
            cJSON *ack = frame ? cJSON_Parse(frame) : NULL;
            free(frame);
            const cJSON *ok = ack
                                  ? cJSON_GetObjectItemCaseSensitive(ack, "ok")
                                  : NULL;
            const cJSON *size = ack
                                    ? cJSON_GetObjectItemCaseSensitive(ack,
                                                                       "size")
                                    : NULL;
            if (!metadata_seen && cJSON_IsTrue(ok) && cJSON_IsNumber(size) &&
                size->valuedouble >= 0 &&
                size->valuedouble <= (double)SNAP_MAX_BYTES) {
                expected_size = (uint64_t)size->valuedouble;
                metadata_seen = true;
                struct statvfs space;
                if (statvfs(dest_dir, &space) != 0 ||
                    expected_size >
                        (uint64_t)space.f_bavail * (uint64_t)space.f_frsize) {
                    cJSON_Delete(ack);
                    goto done;
                }
                cJSON_Delete(ack);
                continue;
            }
            ack_ok = metadata_seen && eof_seen && cJSON_IsTrue(ok) &&
                     received_size == expected_size;
            cJSON_Delete(ack);
            break;
        }
        if (type != ZSTP_SNAP_DATA || !metadata_seen || eof_seen ||
            (frame_len > 0 && !frame) ||
            received_size > expected_size ||
            (uint64_t)frame_len > expected_size - received_size) {
            free(frame);
            goto done;
        }
        if (frame_len > 0 &&
            (size_t)frame_len != fwrite(frame, 1, frame_len, out)) {
            free(frame);
            goto done;
        }
        free(frame);
        received_size += frame_len;
        if (frame_len == 0) {
            eof_seen = true;
        }
    }

    fclose(out);
    out = NULL;
    if (ack_ok && rename(incoming, final_path) == 0) {
        incoming[0] = '\0';
        rc = 0;
    }

done:
    if (out) {
        fclose(out);
    }
    if (incoming[0]) {
        unlink(incoming);
    }
    close(fd);
    return rc;
}

int zdb_snap_fetch(const char *addr, int port, const char key[33],
                   const char *dest_dir)
{
    return snap_fetch(addr, port, key, dest_dir, true);
}

int zdb_snap_fetch_required(const char *addr, int port, const char key[33],
                            const char *dest_dir)
{
    return snap_fetch(addr, port, key, dest_dir, false);
}
