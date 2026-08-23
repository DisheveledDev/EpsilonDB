/* zstp_wire.h - ZSTP frame codec shared between the cluster mesh
 * (stage 4) and the replication layer (stage 5).
 *
 * Frame layout:
 *   offset 0: magic 'Z''S''T''P'
 *   offset 4: version byte (ZSTP_VERSION)
 *   offset 5: message type byte (zstp_type)
 *   offset 6: payload length, 4-byte big-endian
 *   offset 10: JSON payload
 *
 * Stage 4 types carry membership gossip. Stage 5 adds request/reply
 * pairs for data replication: REPL/ACK for writes, QUERY/RESULT for
 * quorum reads. Stage 6b adds SNAP_REQ/SNAP_DATA for shard snapshot
 * transfer (raw file bytes, not JSON). Every connection still starts
 * with HELLO.
 */

#ifndef ZSTP_WIRE_H
#define ZSTP_WIRE_H

#include <stdbool.h>
#include <stdint.h>

#define ZSTP_VERSION      1
#define ZSTP_HEADER_SIZE  10
/* JSON payloads stay well under this; SNAP_DATA frames carry raw shard
 * file bytes up to the cap, chunked by the sender when a shard is
 * bigger. 4 MB keeps a single frame within sane socket buffer sizes. */
#define ZSTP_MAX_PAYLOAD  (4 * 1024 * 1024)

typedef enum {
    ZSTP_HELLO  = 1,
    ZSTP_STATE  = 2,
    ZSTP_REPL   = 3,   /* replicate a write; answered by ACK */
    ZSTP_ACK    = 4,   /* acknowledgement of a REPL frame */
    ZSTP_QUERY  = 5,   /* quorum read request; answered by RESULT */
    ZSTP_RESULT = 6,   /* answer to a QUERY frame */
    ZSTP_SNAP_REQ  = 7, /* {key} request a shard snapshot */
    ZSTP_SNAP_DATA = 8, /* raw shard bytes; empty payload = EOF */
    ZSTP_SNAP_ACK  = 9  /* {ok:bool} end of transfer status */
} zstp_type;

/* Sends one framed message. send_lock (optional) serialises writes on a
 * shared socket; pass NULL for exclusive sockets. Returns 0 on success. */
int zstp_send_frame(int fd, zstp_type type, const char *json,
                    void *send_lock);

/* Reads one framed message. Fills *payload_out with a malloc'd NUL-
 * terminated buffer the caller frees (NULL when plen == 0). Returns the
 * message type, or -1 on EOF/protocol error. */
int zstp_recv_frame(int fd, char **payload_out);

/* Reads one framed message like zstp_recv_frame but also returns the
 * raw payload byte count in *plen_out (JSON frames are NUL-terminated on
 * top of that; SNAP_DATA frames are binary and not NUL-terminated). */
int zstp_recv_frame_raw(int fd, char **payload_out, uint32_t *plen_out);

/* True when the type byte is a known message type. */
bool zstp_type_valid(int type);

/* Dial addr:port (IPv4, TCP_NODELAY). Returns fd or -1. */
int zstp_dial(const char *addr, int port);

/* Dispatch hook invoked when an inbound REPL or QUERY frame arrives on
 * any cluster connection. ctx is the value registered with the hook
 * (typically the owning cluster/replication service), letting multiple
 * in-process nodes each route frames to their own engine. Implementations
 * build the reply payload and set *reply_type (ZSTP_ACK or ZSTP_RESULT).
 * Reply JSON must be malloc'd (may be NULL to skip replying); ownership
 * passes to the caller of the hook. */
typedef bool (*zstp_dispatch_fn)(void *ctx, int msg_type,
                                 const char *payload, int *reply_type,
                                 char **reply_json);

/* Installs a process-wide fallback dispatcher (used when a connection's
 * owning cluster has none of its own). Pass fn=NULL to clear. */
void zstp_set_dispatcher(zstp_dispatch_fn fn, void *ctx);

#endif
