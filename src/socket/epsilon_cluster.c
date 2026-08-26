/* epsilon_cluster.c - membership mesh, gossip, connection handling and
 * the core cluster API. Rebalancing and the ESTP wire codec live in
 * epsilon_cluster_rebalance.c and estp_wire.c; see
 * epsilon_cluster_internal.h.
 */

#include <errno.h>
#include <arpa/inet.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
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
#include "../engine/md5.h"
#include "../engine/random.h"
#include "epsilon_cluster_internal.h"
#include "epsilon_snap.h"
#include "estp_wire.h"
/* ------------------------------------------------------------------ */
/* helpers                                                             */

bool leader_is_self_locked(const edb_cluster *cl)
{
    return cl->leader[0] && strcmp(cl->leader, cl->self_id) == 0;
}



/* ------------------------------------------------------------------ */
/* state serialisation                                                 */

static cJSON *state_to_json(const edb_cluster *cl)
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
        if (cl->peers[i].http_port > 0) {
            cJSON_AddNumberToObject(n, "http_port",
                                    cl->peers[i].http_port);
        }
        cJSON_AddNumberToObject(n, "last_seen",
                                (double)cl->peers[i].last_seen);
        if (cl->peers[i].removed) {
            cJSON_AddBoolToObject(n, "removed", true);
        }
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
static bool merge_state(edb_cluster *cl, const cJSON *doc)
{
    bool changed = false;

    const cJSON *jsender = cJSON_GetObjectItemCaseSensitive(doc, "sender");
    bool from_leader = cJSON_IsString(jsender) && jsender->valuestring &&
                       cl->leader[0] &&
                       strcmp(jsender->valuestring, cl->leader) == 0;

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
            const cJSON *jhttp =
                cJSON_GetObjectItemCaseSensitive(item, "http_port");
            const cJSON *jseen =
                cJSON_GetObjectItemCaseSensitive(item, "last_seen");
            const cJSON *jcompliant =
                cJSON_GetObjectItemCaseSensitive(item, "compliant");
            const cJSON *jremoved =
                cJSON_GetObjectItemCaseSensitive(item, "removed");
            if (!cJSON_IsString(jid) || !jid->valuestring ||
                !*jid->valuestring ||
                strlen(jid->valuestring) >= EDB_NODE_ID_MAX) {
                continue;
            }
            long long seen = cJSON_IsNumber(jseen)
                                 ? (long long)jseen->valuedouble
                                 : 0;
            long long compliant = cJSON_IsNumber(jcompliant)
                                      ? (long long)jcompliant->valuedouble
                                      : 0;

            edb_peer_info *mine = NULL;
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
                    edb_peer_info *grown =
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
                strlen(jaddr->valuestring) < EDB_ADDR_MAX &&
                strcmp(mine->addr, jaddr->valuestring) != 0) {
                snprintf(mine->addr, sizeof(mine->addr), "%s",
                         jaddr->valuestring);
                changed = true;
            }
            if (!selfish && cJSON_IsNumber(jport) &&
                jport->valueint > 0 && jport->valueint < 65536 &&
                mine->port != jport->valueint) {
                mine->port = jport->valueint;
            }
            if (!selfish && cJSON_IsNumber(jhttp) &&
                jhttp->valueint > 0 && jhttp->valueint < 65536 &&
                mine->http_port != jhttp->valueint) {
                mine->http_port = jhttp->valueint;
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
            /* removal is sticky (any sender can propagate it); only the
             * leader may clear a tombstone (explicit re-join) */
            if (cJSON_IsTrue(jremoved) && !mine->removed) {
                mine->removed = true;
                changed = true;
            } else if (!cJSON_IsTrue(jremoved) && mine->removed &&
                       from_leader) {
                mine->removed = false;
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
                    edb_range_info *grown =
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
                    edb_range_info *grown =
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

void persist_state(edb_cluster *cl)
{
    /* caller holds no locks beyond cl->lock */
    cJSON *doc = state_to_json(cl);
    if (doc) {
        char *json = cJSON_PrintUnformatted(doc);
        if (json) {
            edb_setting_set(cl->cfg, SETTING_MEMBERS, json);
            free(json);
        }
        cJSON_Delete(doc);
    }
    char rjson[128];
    snprintf(rjson, sizeof(rjson), "{\"generation\":%lld}",
             cl->generation);
    edb_setting_set(cl->cfg, SETTING_RANGES, rjson);

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
                edb_setting_set(cl->cfg, SETTING_TARGET, tjson);
                free(tjson);
            }
        }
    } else {
        edb_setting_delete(cl->cfg, SETTING_TARGET);
    }
}

void restore_state(edb_cluster *cl)
{
    char *members = edb_setting_get(cl->cfg, SETTING_MEMBERS);
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
    char *rjson = edb_setting_get(cl->cfg, SETTING_RANGES);
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
    char *tjson = edb_setting_get(cl->cfg, SETTING_TARGET);
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
bool is_online_view(const edb_cluster *cl, const char *id);

/* Recompute leader = lexicographically smallest online node id.
 * Caller holds cl->lock. */
static void recompute_leader(edb_cluster *cl)
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
bool is_online_view(const edb_cluster *cl, const char *id)
{
    if (strcmp(id, cl->self_id) == 0) {
        return true;
    }
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, id) == 0) {
            return cl->peers[i].online && !cl->peers[i].removed;
        }
    }
    return false;
}

/* Builds one contiguous slice per node (sorted id order) into out.
 * Returns the number of slices written; *gen_out receives generation+1.
 * Caller holds cl->lock. out/out_cap must be large enough or NULL to
 * just count. */
size_t build_slices(const char **ids, size_t n,
                           edb_range_info *out, size_t out_cap)
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
static void conn_unref_locked(edb_cluster *cl, peer_conn *c)
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
static int send_hello_state(edb_cluster *cl, int fd, bool with_state,
                            bool joining)
{
    char *hello = NULL;
    char *state = NULL;

    pthread_mutex_lock(&cl->lock);
    cJSON *h = cJSON_CreateObject();
    if (h) {
        cJSON_AddStringToObject(h, "node_id", cl->self_id);
        cJSON_AddStringToObject(h, "addr", cl->self_addr);
        cJSON_AddNumberToObject(h, "port", cl->self_port);
        if (cl->self_http_port > 0) {
            cJSON_AddNumberToObject(h, "http_port", cl->self_http_port);
        }
        if (joining) {
            cJSON_AddBoolToObject(h, "join", true);
        }
        hello = cJSON_PrintUnformatted(h);
        cJSON_Delete(h);
    }
    if (with_state) {
        cJSON *s = state_to_json(cl);
        state = s ? cJSON_PrintUnformatted(s) : NULL;
        cJSON_Delete(s);
    }
    pthread_mutex_unlock(&cl->lock);

    int rc = estp_send(fd, ESTP_HELLO, hello ? hello : "{}", NULL);
    if (rc == 0 && with_state) {
        rc = estp_send(fd, ESTP_STATE, state ? state : "{}", NULL);
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
void gossip_state(edb_cluster *cl)
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
        estp_send(connections[i]->fd, ESTP_STATE, state ? state : "{}",
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
static void leader_react(edb_cluster *cl, bool changed)
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
        edb_cluster_acquire_rebalance_lock(cl);
    }

    /* stage 6d: once every node reports compliance the leader promotes
     * automatically (the data migration steps happen via 6c catch-up) */
    if (am_leader && cl->target_generation > 0) {
        edb_cluster_maybe_promote(cl);
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
static void reader_exiting_locked(edb_cluster *cl)
{
    cl->nconn_threads--;
    pthread_cond_broadcast(&cl->conn_cv);
}

static void pending_remove_locked(edb_cluster *cl, peer_conn *c)
{
    peer_conn **link = &cl->pending_conns;
    while (*link && *link != c) {
        link = &(*link)->next;
    }
    if (*link == c) {
        *link = c->next;
        c->next = NULL;
    }
}

static void pending_discard(edb_cluster *cl, peer_conn *c)
{
    pthread_mutex_lock(&cl->lock);
    pending_remove_locked(cl, c);
    reader_exiting_locked(cl);
    pthread_mutex_unlock(&cl->lock);
    shutdown(c->fd, SHUT_RDWR);
    close(c->fd);
    free(c);
}

static void *conn_thread(void *arg)
{
    peer_conn *c = arg;
    edb_cluster *cl = c->owner;

    char *payload = NULL;
    int t = estp_recv(c->fd, &payload);
    if (t == ESTP_HELLO && payload) {
        cJSON *h = cJSON_Parse(payload);
        if (h) {
            const cJSON *jid =
                cJSON_GetObjectItemCaseSensitive(h, "node_id");
            const cJSON *jaddr =
                cJSON_GetObjectItemCaseSensitive(h, "addr");
            const cJSON *jport =
                cJSON_GetObjectItemCaseSensitive(h, "port");
            const cJSON *jhttp =
                cJSON_GetObjectItemCaseSensitive(h, "http_port");
            const cJSON *jjoin =
                cJSON_GetObjectItemCaseSensitive(h, "join");
            bool joining = cJSON_IsTrue(jjoin);
            bool is_self = false;
            if (cJSON_IsString(jid) && jid->valuestring &&
                strlen(jid->valuestring) < EDB_NODE_ID_MAX) {
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
                bool removed = false;
                pthread_mutex_lock(&cl->lock);
                for (size_t i = 0; i < cl->npeers; i++) {
                    if (strcmp(cl->peers[i].id, c->node_id) == 0) {
                        known = true;
                        if (cl->peers[i].removed) {
                            if (joining) {
                                /* explicit re-join clears the tombstone
                                 * and re-admits as a fresh member */
                                cl->peers[i].removed = false;
                                cl->peers[i].last_seen = 0;
                            } else {
                                removed = true;
                            }
                        }
                        break;
                    }
                }
                bool busy = cl->target_generation > 0;
                pthread_mutex_unlock(&cl->lock);
                if (removed) {
                    cJSON *r = cJSON_CreateObject();
                    cJSON_AddStringToObject(r, "node_id", cl->self_id);
                    cJSON_AddStringToObject(r, "reject", "removed");
                    char *rs = r ? cJSON_PrintUnformatted(r) : NULL;
                    cJSON_Delete(r);
                    estp_send(c->fd, ESTP_HELLO, rs ? rs : "{}", NULL);
                    free(rs);
                    pending_discard(cl, c);
                    return NULL;
                }
                if (!known && busy) {
                    cJSON *r = cJSON_CreateObject();
                    cJSON_AddStringToObject(r, "node_id", cl->self_id);
                    cJSON_AddStringToObject(r, "reject", "rebalance");
                    char *rs = r ? cJSON_PrintUnformatted(r) : NULL;
                    cJSON_Delete(r);
                    estp_send(c->fd, ESTP_HELLO, rs ? rs : "{}", NULL);
                    free(rs);
                    pending_discard(cl, c);
                    return NULL;
                }
            }
            /* adopt the caller's self-declared address/port for
             * membership so we know where to dial them back */
            if (c->node_id[0] && !is_self && cJSON_IsString(jaddr) &&
                jaddr->valuestring &&
                strlen(jaddr->valuestring) < EDB_ADDR_MAX) {
                pthread_mutex_lock(&cl->lock);
                edb_peer_info *p = NULL;
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
                        edb_peer_info *grown =
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
                    int http_port = cJSON_IsNumber(jhttp)
                                        ? jhttp->valueint
                                        : 0;
                    if (http_port > 0 && http_port < 65536) {
                        p->http_port = http_port;
                    }
                    p->last_seen = epoch_now();
                }
                pthread_mutex_unlock(&cl->lock);
            }
            cJSON_Delete(h);
        }
    }
    free(payload);

    if (!c->node_id[0]) {
        pending_discard(cl, c);
        return NULL;
    }

    send_hello_state(cl, c->fd, true, false);

    pthread_mutex_lock(&cl->lock);
    pending_remove_locked(cl, c);
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
        t = estp_recv(c->fd, &payload);
        if (t < 0) {
            break;
        }
        changed = false;
        if (t == ESTP_STATE && payload) {
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
        } else if (t == ESTP_SNAP_REQ) {
            /* stage 6b/6c: shard snapshot request. Served directly
             * (not via the dispatcher) so the engine behind the config
             * store streams a consistent copy; runs without cl->lock
             * held because it blocks on SQLite + socket I/O. */
            pthread_mutex_lock(&cl->lock);
            c->last_recv = epoch_now();
            pthread_mutex_unlock(&cl->lock);
            edb_snap_serve(c->fd,
                           payload ? (uint32_t)strlen(payload) : 0,
                           payload, edb_config_engine(cl->cfg));
        } else if (t == ESTP_VOID) {
            /* stage 6e: rollback request. Only the leader acts; the
             * reply tells the requester whether a wave was voided. */
            pthread_mutex_lock(&cl->lock);
            c->last_recv = epoch_now();
            pthread_mutex_unlock(&cl->lock);
            bool ok = edb_cluster_void_target(cl);
            char reply[32];
            snprintf(reply, sizeof(reply), "{\"ok\":%s}",
                     ok ? "true" : "false");
            estp_send(c->fd, ESTP_ACK, reply, &cl->send_lock);
        } else if (t == ESTP_REPL || t == ESTP_QUERY ||
                   t == ESTP_FLUSH) {
            estp_dispatch_fn fn = NULL;
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
                estp_send(c->fd, (estp_type)reply_type, reply,
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
        edb_setting_delete(cl->cfg, SETTING_LOCK);
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
    edb_cluster *cl = c->owner;
    pthread_mutex_lock(&cl->lock);
    cl->nconn_threads++;
    c->next = cl->pending_conns;
    cl->pending_conns = c;
    pthread_mutex_unlock(&cl->lock);
    if (pthread_create(&tid, &attr, conn_thread, c) != 0) {
        pthread_mutex_lock(&cl->lock);
        pending_remove_locked(cl, c);
        pthread_mutex_unlock(&cl->lock);
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
    edb_cluster *cl = arg;
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
        send_hello_state(cl, fd, false, false);

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

int dial_peer(const char *addr, int port)
{
    return estp_dial(addr, port);
}

static bool have_conn_locked(const edb_cluster *cl, const char *id)
{
    for (const peer_conn *c = cl->conns; c; c = c->next) {
        if (strcmp(c->node_id, id) == 0) {
            return true;
        }
    }
    return false;
}

static void maintainer_tick(edb_cluster *cl)
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
        if (estp_send(hb[i]->fd, ESTP_STATE, state ? state : "{}",
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
        edb_peer_info p = cl->peers[i];   /* copy; lock released below */
        if (strcmp(p.id, cl->self_id) == 0 || p.removed ||
            p.addr[0] == '\0' || p.port <= 0 ||
            (p.port == cl->self_port &&
             strcmp(p.addr, "127.0.0.1") == 0) ||
            have_conn_locked(cl, p.id)) {
            continue;
        }
        pthread_mutex_unlock(&cl->lock);

        int fd = dial_peer(p.addr, p.port);
        if (fd >= 0) {
            send_hello_state(cl, fd, false, false);
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

    edb_cluster_maybe_promote(cl);

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
        !edb_cluster_needs_sync(cl)) {
        edb_cluster_mark_compliant(cl);
    }
    pthread_mutex_lock(&cl->lock);
    bool gc_ready = cl->target_generation == 0 && cl->gc_after > 0 &&
                    epoch_now() >= cl->gc_after;
    pthread_mutex_unlock(&cl->lock);
    if (gc_ready) {
        edb_cluster_gc_redundant(cl);
    }

    /* auto-remove peers that have been offline for long enough: the
     * leader tombstones them and re-shards the live table so their
     * ranges are re-absorbed by the remaining members */
    bool removed_any = false;
    pthread_mutex_lock(&cl->lock);
    if (leader_is_self_locked(cl)) {
        long long now = epoch_now();
        for (size_t i = 0; i < cl->npeers; i++) {
            if (strcmp(cl->peers[i].id, cl->self_id) == 0 ||
                cl->peers[i].removed || cl->peers[i].online ||
                cl->peers[i].last_seen <= 0) {
                continue;
            }
            if (now - cl->peers[i].last_seen >= REMOVE_AFTER_SECONDS) {
                cl->peers[i].removed = true;
                removed_any = true;
            }
        }
        if (removed_any) {
            shrink_live_locked(cl);
            persist_state(cl);
        }
    }
    pthread_mutex_unlock(&cl->lock);
    if (removed_any) {
        gossip_state(cl);
    }
}

static void *maintainer_main(void *arg)
{
    edb_cluster *cl = arg;
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

static void add_self_locked(edb_cluster *cl)
{
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, cl->self_id) == 0) {
            /* restored from persistence: refresh identity fields that
             * gossip never overwrites for our own record */
            snprintf(cl->peers[i].addr, sizeof(cl->peers[i].addr), "%s",
                     cl->self_addr);
            cl->peers[i].port = cl->self_port;
            cl->peers[i].http_port = cl->self_http_port;
            cl->peers[i].online = true;
            return;
        }
    }
    if (cl->npeers == cl->peers_cap) {
        size_t cap = cl->peers_cap ? cl->peers_cap * 2 : 8;
        edb_peer_info *grown = realloc(cl->peers, cap * sizeof(*grown));
        if (!grown) {
            return;
        }
        cl->peers = grown;
        cl->peers_cap = cap;
    }
    edb_peer_info *p = &cl->peers[cl->npeers++];
    memset(p, 0, sizeof(*p));
    snprintf(p->id, sizeof(p->id), "%s", cl->self_id);
    snprintf(p->addr, sizeof(p->addr), "%s", cl->self_addr);
    p->port = cl->self_port;
    p->http_port = cl->self_http_port;
    p->last_seen = epoch_now();
    p->online = true;
}

void edb_cluster_set_http_port(edb_cluster *cl, int http_port)
{
    if (!cl || http_port < 0 || http_port > 65535) {
        return;
    }
    pthread_mutex_lock(&cl->lock);
    cl->self_http_port = http_port;
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, cl->self_id) == 0) {
            cl->peers[i].http_port = http_port;
            break;
        }
    }
    pthread_mutex_unlock(&cl->lock);
    if (http_port > 0) {
        gossip_state(cl);
    }
}

edb_cluster *edb_cluster_start(edb_config *cfg, const char *advertise_addr,
                               int peer_port,
                               char node_id_out[EDB_NODE_ID_MAX])
{
    if (!cfg || !advertise_addr || !*advertise_addr ||
        peer_port <= 0 || peer_port > 65535) {
        return NULL;
    }

    edb_cluster *cl = calloc(1, sizeof(*cl));
    if (!cl) {
        return NULL;
    }
    cl->cfg = cfg;
    cl->auto_compliant = true;
    snprintf(cl->self_addr, sizeof(cl->self_addr), "%s", advertise_addr);
    cl->self_port = peer_port;

    /* stable node id: md5 of the advertise address + port */
    char seed[256];
    snprintf(seed, sizeof(seed), "edb-node:%s:%d", advertise_addr,
             peer_port);
    char hex[33];
    edb_md5_hex(seed, strlen(seed), hex);
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
        snprintf(node_id_out, EDB_NODE_ID_MAX, "%s", cl->self_id);
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

void edb_cluster_stop(edb_cluster *cl)
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
    for (peer_conn *c = cl->pending_conns; c; c = c->next) {
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

size_t edb_cluster_peers(edb_cluster *cl, edb_peer_info *out, size_t cap)
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

const char *edb_cluster_leader(edb_cluster *cl)
{
    static _Thread_local char buf[EDB_NODE_ID_MAX];
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

const char *edb_cluster_self_id(edb_cluster *cl)
{
    return cl ? cl->self_id : "";
}

bool edb_cluster_is_leader(edb_cluster *cl)
{
    if (!cl) {
        return false;
    }
    pthread_mutex_lock(&cl->lock);
    bool me = leader_is_self_locked(cl);
    pthread_mutex_unlock(&cl->lock);
    return me;
}

long long edb_cluster_generation(edb_cluster *cl)
{
    if (!cl) {
        return 0;
    }
    pthread_mutex_lock(&cl->lock);
    long long g = cl->generation;
    pthread_mutex_unlock(&cl->lock);
    return g;
}

size_t edb_cluster_ranges(edb_cluster *cl, edb_range_info *out, size_t cap)
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

const char *edb_cluster_owner(edb_cluster *cl, const char *md5hex)
{
    static _Thread_local char owner_buf[EDB_NODE_ID_MAX];
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

size_t edb_cluster_holders(edb_cluster *cl, const char *md5hex,
                           char (*node_ids)[EDB_NODE_ID_MAX], size_t cap)
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
        if (!cl->peers[i].removed) {
            candidates[ncandidates++] = cl->peers[i].id;
        }
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
        snprintf(node_ids[count], EDB_NODE_ID_MAX, "%s", candidates[index]);
        count++;
    }
    pthread_mutex_unlock(&cl->lock);
    return count;
}


/* One-shot join: dial the seed, exchange HELLO+STATE both ways so both
 * membership views merge, then hang up (the maintainer re-dials).
 * Returns 0 on success, -2 when the seed refused because a rebalance
 * wave is pending, -3 when this node has been removed from the cluster. */
static int join_exchange(edb_cluster *cl, int fd)
{
    send_hello_state(cl, fd, true, true);

    /* read frames for a short while to absorb their state. A wrong (or
     * missing) secret makes every inbound frame fail authentication, so
     * no valid reply ever arrives: that is a failed handshake, not a
     * successful join. */
    bool got_reply = false;
    time_t deadline = time(NULL) + 2;
    while (time(NULL) < deadline) {
        char *payload = NULL;
        int t = estp_recv(fd, &payload);
        if (t < 0) {
            break;
        }
        got_reply = true;
        if (t == ESTP_HELLO && payload) {
            cJSON *h = cJSON_Parse(payload);
            const cJSON *jr =
                h ? cJSON_GetObjectItemCaseSensitive(h, "reject") : NULL;
            if (cJSON_IsString(jr) && jr->valuestring) {
                int rc = strcmp(jr->valuestring, "removed") == 0 ? -3 : -2;
                cJSON_Delete(h);
                free(payload);
                return rc;
            }
            cJSON_Delete(h);
        } else if (t == ESTP_STATE && payload) {
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
    return got_reply ? 0 : -1;
}

int edb_cluster_join(edb_cluster *cl, const char *seed_addr, int seed_port)
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

bool edb_cluster_remove_node(edb_cluster *cl, const char *node_id)
{
    if (!cl || !node_id || !*node_id ||
        strcmp(node_id, cl->self_id) == 0) {
        return false;
    }
    bool found = false;
    bool am_leader = false;
    pthread_mutex_lock(&cl->lock);
    for (size_t i = 0; i < cl->npeers; i++) {
        if (strcmp(cl->peers[i].id, node_id) == 0) {
            cl->peers[i].removed = true;
            found = true;
            break;
        }
    }
    if (found) {
        for (peer_conn *conn = cl->conns; conn; conn = conn->next) {
            if (strcmp(conn->node_id, node_id) == 0) {
                conn->dead = true;
                shutdown(conn->fd, SHUT_RDWR);
            }
        }
        am_leader = leader_is_self_locked(cl);
        if (am_leader) {
            shrink_live_locked(cl);
        }
        persist_state(cl);
    }
    pthread_mutex_unlock(&cl->lock);

    if (found) {
        gossip_state(cl);
    }
    return found;
}

void edb_cluster_set_dispatcher(edb_cluster *cl,
                                estp_dispatch_fn fn, void *ctx)
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