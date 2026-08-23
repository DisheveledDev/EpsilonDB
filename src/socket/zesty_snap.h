/* zesty_snap.h - stage 6b shard snapshot transfer.
 *
 * Moves a consistent copy of one shard file between nodes:
 *   - zdb_snap_fetch dials a peer, requests the shard identified by its
 *     32-char md5 hex key, streams the bytes into dest_dir and atomically
 *     renames them over "<key>.sqlite" once complete. Returns 0 on
 *     success, -1 on any failure (nothing is renamed then).
 *   - zdb_snap_serve answers an inbound SNAP_REQ exchange on an already
 *     dialled, HELLO-exchanged socket: it copies the local shard with
 *     the sqlite3_backup_* online backup API (so concurrent writes stay
 *     safe) and streams it out, finishing with a status ack.
 *
 * The receiver's engine must have its cached handle invalidated after a
 * successful fetch (zdb_shard_invalidate) so subsequent reads reopen
 * the replaced file; callers own that sequencing (6c wires it up).
 *
 * All functions are thread-safe except they share no state.
 */

#ifndef ZESTY_SNAP_H
#define ZESTY_SNAP_H

#include <stdint.h>

#include "../engine/zesty_engine.h"
#include "zstp_wire.h"

/* Requests and receives a shard snapshot for `key` from addr:port,
 * writing it to dest_dir/<key>.sqlite. Returns 0 on success. */
int zdb_snap_fetch(const char *addr, int port, const char key[33],
                   const char *dest_dir);

/* Serves one inbound SNAP_REQ exchange on fd. payload/plen describe the
 * SNAP_REQ frame already read from the socket; this function reads the
 * remaining frames' replies itself and sends the final SNAP_ACK.
 * engine may be NULL (replies ok:false). Blocks until the transfer is
 * done or the socket fails. */
void zdb_snap_serve(int fd, uint32_t payload_len, const char *payload,
                    zdb_engine *engine);

#endif
