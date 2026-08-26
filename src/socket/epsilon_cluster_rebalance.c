/* epsilon_cluster_rebalance.c - stage 6 rebalancing: live/target
 * structure versions, the leader-held rebalance lock, per-node compliance,
 * target promotion, redundant-shard GC and the join/remove flows.
 * Part of the cluster module; see epsilon_cluster_internal.h.
 */

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <poll.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "../../vendor/cjson/cJSON.h"
#include "../engine/epsilon_engine.h"
#include "../engine/epsilon_crypto.h"
#include "../engine/md5.h"
#include "../engine/random.h"
#include "epsilon_cluster_internal.h"
#include "estp_wire.h"
bool publish_target_locked(edb_cluster *cl)
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
        edb_range_info *grown =
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
static void promote_target_locked(edb_cluster *cl)
{
    if (cl->target_generation == 0 || cl->ntarget_ranges == 0) {
        return;
    }
    if (cl->ntarget_ranges > cl->ranges_cap) {
        edb_range_info *grown = realloc(
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

/* Leader-only: re-shard the live table directly over the current online,
 * non-removed members (used when a node leaves or is removed — there is no
 * transfer possible to a departed node, so the live table shrinks in place
 * rather than via a target wave). Voids any pending target. Caller holds
 * cl->lock. */
void shrink_live_locked(edb_cluster *cl)
{
    cl->target_generation = 0;
    cl->ntarget_ranges = 0;
    edb_setting_delete(cl->cfg, SETTING_LOCK);

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
        return;
    }
    if (n != cl->nranges) {
        if (n > cl->ranges_cap) {
            edb_range_info *grown =
                realloc(cl->ranges, n * sizeof(*grown));
            if (!grown) {
                return;
            }
            cl->ranges = grown;
            cl->ranges_cap = n;
        }
        cl->nranges = build_slices(ids, n, cl->ranges, n);
        cl->generation++;
        cl->gc_after = epoch_now() + OFFLINE_AFTER_SECONDS;
    }
}

bool edb_cluster_publish_target(edb_cluster *cl)
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
size_t edb_cluster_target_ranges(edb_cluster *cl, edb_range_info *out,
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

long long edb_cluster_target_generation(edb_cluster *cl)
{
    if (!cl) {
        return 0;
    }
    pthread_mutex_lock(&cl->lock);
    long long g = cl->target_generation;
    pthread_mutex_unlock(&cl->lock);
    return g;
}

const char *edb_cluster_target_owner(edb_cluster *cl, const char *md5hex)
{
    static _Thread_local char owner_buf[EDB_NODE_ID_MAX];
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
            /* copy out: see edb_cluster_owner for why a bare pointer
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

bool edb_cluster_promote_target(edb_cluster *cl)
{
    if (!cl || edb_cluster_target_generation(cl) == 0 ||
        !edb_cluster_target_compliant(cl)) {
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
            edb_setting_delete(cl->cfg, name);
        }
        edb_setting_delete(cl->cfg, SETTING_LOCK);
        persist_state(cl);
        promoted = true;
    }
    pthread_mutex_unlock(&cl->lock);
    if (promoted) {
        gossip_state(cl);
    }
    return promoted;
}

bool edb_cluster_target_compliant(edb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    pthread_mutex_lock(&cl->lock);
    long long target_generation = cl->target_generation;
    char required[MAX_PEERS][EDB_NODE_ID_MAX];
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
            snprintf(required[nrequired], EDB_NODE_ID_MAX, "%s",
                     cl->target_ranges[i].node_id);
            nrequired++;
        }
    }
    pthread_mutex_unlock(&cl->lock);
    if (target_generation == 0 || nrequired == 0) {
        return false;
    }

    edb_peer_info peers[MAX_PEERS];
    size_t npeers = edb_cluster_peers(cl, peers, MAX_PEERS);
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
        char *value = edb_setting_get(cl->cfg, name);
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

void edb_cluster_mark_compliant(edb_cluster *cl)
{
    if (!cl) {
        return;
    }
    long long tgen = edb_cluster_target_generation(cl);
    if (tgen == 0) {
        tgen = edb_cluster_generation(cl);
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
    edb_setting_set(cl->cfg, name, val);

    /* propagate compliance immediately so the leader can promote without
     * waiting for the next heartbeat */
    gossip_state(cl);
}

/* --- stage 6d: promotion trigger + shard GC -------------------------- */

/* Leader-only: promote the pending target when every online node has
 * reported compliance. Safe to call from any thread (takes the lock).
 * Returns true when a promotion happened. */
bool edb_cluster_maybe_promote(edb_cluster *cl)
{
    if (!cl || !edb_cluster_is_leader(cl)) {
        return false;
    }
    if (edb_cluster_target_generation(cl) == 0 ||
        !edb_cluster_target_compliant(cl)) {
        return false;
    }
    return edb_cluster_promote_target(cl);
}

/* Removes one redundant local shard file (a shard whose key is now owned
 * by a different node under the live table) after confirming the new
 * owner is assigned. Reserved __system__ config shards are never removed.
 * Returns the number of shards GC'd this call (0 or 1), so callers can
 * drain GC one at a time. */
size_t edb_cluster_gc_redundant(edb_cluster *cl)
{
    if (!cl || edb_cluster_target_generation(cl) != 0) {
        return 0;
    }
    /* restore in progress: shard files are being wiped and re-placed;
     * do not GC anything until the restore unlocks */
    char *lock = edb_setting_get(cl->cfg, "server.restore_lock");
    if (lock) {
        cJSON *jl = cJSON_Parse(lock);
        free(lock);
        bool locked = cJSON_IsTrue(jl);
        cJSON_Delete(jl);
        if (locked) {
            return 0;
        }
    }
    edb_engine *engine = edb_config_engine(cl->cfg);
    if (!engine) {
        return 0;
    }
    char keys[256][33];
    size_t key_count = edb_engine_shard_keys(engine, keys, 256);
    size_t keyspace_count = 0;
    edb_keyspace_info *keyspaces = edb_keyspace_list(cl->cfg,
                                                     &keyspace_count);
    edb_peer_info peers[MAX_PEERS];
    size_t peer_count = edb_cluster_peers(cl, peers, MAX_PEERS);

    for (size_t i = 0; i < key_count; i++) {
        if (edb_config_is_system_key(cl->cfg, keys[i])) {
            continue;
        }
        int rf = 1;
        bool registered = false;
        for (size_t k = 0; k < keyspace_count; k++) {
            char path[1024];
            char candidate[33];
            if (edb_shard_path(engine, keyspaces[k].partition,
                               keyspaces[k].name, path, sizeof(path),
                               candidate) && strcmp(candidate, keys[i]) == 0) {
                edb_database_info database;
                if (edb_database_get(cl->cfg, keyspaces[k].database,
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

        char holders[MAX_PEERS][EDB_NODE_ID_MAX];
        size_t holder_count = edb_cluster_holders(cl, keys[i], holders,
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
        if (!keep_local && replicas_ready && edb_shard_gc(engine, keys[i])) {
            free(keyspaces);
            return 1;
        }
    }
    free(keyspaces);
    return 0;
}

bool edb_cluster_acquire_rebalance_lock(edb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    char *cur = edb_setting_get(cl->cfg, SETTING_LOCK);
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
    return edb_setting_set(cl->cfg, SETTING_LOCK, json);
}

void edb_cluster_release_rebalance_lock(edb_cluster *cl)
{
    if (!cl) {
        return;
    }
    edb_setting_delete(cl->cfg, SETTING_LOCK);
}

/* --- stage 6e: end-to-end rebalance wiring ---------------------------- */

bool edb_cluster_needs_sync(edb_cluster *cl)
{
    if (!cl || edb_cluster_target_generation(cl) == 0) {
        return false;
    }
    edb_engine *engine = edb_config_engine(cl->cfg);
    if (!engine) {
        return false;
    }
    size_t nks = 0;
    edb_keyspace_info *kss = edb_keyspace_list(cl->cfg, &nks);
    bool needed = false;
    for (size_t i = 0; kss && i < nks && !needed; i++) {
        char key[33];
        char path[1024];
        if (!edb_shard_path(engine, kss[i].partition, kss[i].name,
                            path, sizeof(path), key)) {
            continue;
        }
        if (edb_config_is_system_key(cl->cfg, key)) {
            continue;   /* config shards replicate separately */
        }
        const char *towner = edb_cluster_target_owner(cl, key);
        if (!towner || strcmp(towner, cl->self_id) != 0) {
            continue;   /* the wave does not give us this shard */
        }
        if (edb_shard_validate(engine, kss[i].partition, kss[i].name)) {
            continue;
        }
        needed = true;
    }
    free(kss);
    return needed;
}

void edb_cluster_set_auto_compliant(edb_cluster *cl, bool enabled)
{
    if (!cl) {
        return;
    }
    pthread_mutex_lock(&cl->lock);
    cl->auto_compliant = enabled;
    pthread_mutex_unlock(&cl->lock);
}

bool edb_cluster_void_target(edb_cluster *cl)
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
        edb_cluster_release_rebalance_lock(cl);
        pthread_mutex_lock(&cl->lock);
        persist_state(cl);
        pthread_mutex_unlock(&cl->lock);
        gossip_state(cl);
    }
    return voided;
}

bool edb_cluster_request_void(edb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    char leader[EDB_NODE_ID_MAX] = "";
    char addr[EDB_ADDR_MAX] = "";
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
        return edb_cluster_void_target(cl);
    }
    int fd = dial_peer(addr, port);
    if (fd < 0) {
        return false;
    }
    char hello[192];
    snprintf(hello, sizeof(hello),
             "{\"node_id\":\"ephemeral\",\"addr\":\"%s\",\"port\":0}",
             cl->self_addr);
    estp_send(fd, ESTP_HELLO, hello, NULL);

    /* absorb their HELLO/STATE before asking */
    for (;;) {
        char *payload = NULL;
        int t = estp_recv(fd, &payload);
        free(payload);
        if (t < 0 || t == ESTP_STATE) {
            break;
        }
    }
    char req[64];
    long long tgen = edb_cluster_target_generation(cl);
    snprintf(req, sizeof(req), "{\"generation\":%lld}", tgen);
    estp_send(fd, ESTP_VOID, req, NULL);
    bool ok = false;
    char *payload = NULL;
    int t = estp_recv(fd, &payload);
    if (t == ESTP_ACK && payload) {
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

/* ------------------------------------------------------------------ */
/* mesh key management                                                 */

int edb_cluster_derive_keys(const char *secret, uint8_t enc_key[32],
                            uint8_t mac_key[32])
{
    static const uint8_t salt[] = "epsilond-mesh";
    static const uint8_t info[] = "epsilond-mesh-v1";
    uint8_t okm[64];
    if (!secret || !*secret) {
        return -1;
    }
    if (edb_hkdf_sha256((const uint8_t *)secret, strlen(secret), salt,
                        sizeof(salt) - 1, info, sizeof(info) - 1, okm,
                        sizeof(okm)) != 0) {
        return -1;
    }
    memcpy(enc_key, okm, 32);
    memcpy(mac_key, okm + 32, 32);
    return 0;
}

bool edb_cluster_persist_keys(const char *data_dir, const uint8_t enc_key[32],
                              const uint8_t mac_key[32])
{
    char path[1024];
    uint8_t buf[64];
    int fd;
    size_t n = 0;
    if (!data_dir || !enc_key || !mac_key) {
        return false;
    }
    snprintf(path, sizeof(path), "%s/mesh.key", data_dir);
    fd = open(path, O_WRONLY | O_CREAT | O_TRUNC, 0600);
    if (fd < 0) {
        return false;
    }
    memcpy(buf, enc_key, 32);
    memcpy(buf + 32, mac_key, 32);
    while (n < sizeof(buf)) {
        ssize_t w = write(fd, buf + n, sizeof(buf) - n);
        if (w <= 0) {
            close(fd);
            return false;
        }
        n += (size_t)w;
    }
    close(fd);
    return true;
}

bool edb_cluster_load_keys(const char *data_dir, uint8_t enc_key[32],
                           uint8_t mac_key[32])
{
    char path[1024];
    uint8_t buf[64];
    int fd;
    size_t n = 0;
    if (!data_dir || !enc_key || !mac_key) {
        return false;
    }
    snprintf(path, sizeof(path), "%s/mesh.key", data_dir);
    fd = open(path, O_RDONLY);
    if (fd < 0) {
        return false;
    }
    while (n < sizeof(buf)) {
        ssize_t r = read(fd, buf + n, sizeof(buf) - n);
        if (r <= 0) {
            break;
        }
        n += (size_t)r;
    }
    close(fd);
    if (n != sizeof(buf)) {
        return false;
    }
    memcpy(enc_key, buf, 32);
    memcpy(mac_key, buf + 32, 32);
    return true;
}