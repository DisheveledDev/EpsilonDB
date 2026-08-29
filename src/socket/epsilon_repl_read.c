/* epsilon_repl_read.c - the quorum read path: fan out GET/ALL/QUERY/IDS
 * to replica holders and merge the responses where a quorum agrees
 * (last-write-wins). Part of the replication module; see
 * epsilon_repl_internal.h.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../../vendor/cjson/cJSON.h"
#include "../engine/epsilon_engine.h"
#include "../engine/md5.h"
#include "epsilon_repl_internal.h"
#define MAX_REPLIES 16

typedef struct {
    const edb_peer_info *peer;
    const char *payload;
    cJSON *reply;
} query_job;

static void *query_worker(void *arg)
{
    query_job *job = arg;
    char *reply = NULL;
    if (rpc_once(job->peer->addr, job->peer->port, ESTP_QUERY,
                 job->payload, ESTP_RESULT, &reply) == ESTP_RESULT) {
        job->reply = reply ? cJSON_Parse(reply) : NULL;
    }
    free(reply);
    return NULL;
}

static size_t query_all(edb_repl *rp, const cJSON *request,
                        cJSON **replies, size_t cap)
{
    char *payload = json_print(request);
    if (!payload) {
        return 0;
    }
    const cJSON *database = cJSON_GetObjectItemCaseSensitive(request, "db");
    const cJSON *partition =
        cJSON_GetObjectItemCaseSensitive(request, "partition");
    const cJSON *keyspace =
        cJSON_GetObjectItemCaseSensitive(request, "keyspace");
    if (!cJSON_IsString(database) || !cJSON_IsString(partition) ||
        !cJSON_IsString(keyspace)) {
        free(payload);
        return 0;
    }
    char holders[MAX_PEERS_SNAPSHOT][EDB_NODE_ID_MAX];
    size_t nholders = holder_ids(rp, partition->valuestring,
                                 keyspace->valuestring,
                                 replication_factor(rp, database->valuestring),
                                 holders);
    edb_peer_info peers[MAX_PEERS_SNAPSHOT];
    size_t npeers = edb_cluster_peers(rp->cluster, peers,
                                      MAX_PEERS_SNAPSHOT);
    query_job jobs[MAX_REPLIES];
    pthread_t threads[MAX_REPLIES];
    size_t njobs = 0;
    for (size_t i = 0; i < npeers && njobs < cap && njobs < MAX_REPLIES; i++) {
        if (strcmp(peers[i].id, rp->self_id) == 0 ||
            !id_in_holders(peers[i].id, holders, nholders) ||
            !peers[i].online || peers[i].addr[0] == '\0' ||
            peers[i].port <= 0) {
            continue;
        }
        jobs[njobs].peer = &peers[i];
        jobs[njobs].payload = payload;
        jobs[njobs].reply = NULL;
        if (pthread_create(&threads[njobs], NULL, query_worker,
                           &jobs[njobs]) == 0) {
            njobs++;
        }
    }
    size_t got = 0;
    for (size_t i = 0; i < njobs; i++) {
        pthread_join(threads[i], NULL);
        if (jobs[i].reply) {
            replies[got++] = jobs[i].reply;
        }
    }
    free(payload);
    return got;
}

/* Canonical fingerprint of a JSON value: agreement between replicas is
 * decided on the md5 of the printed form so values of any size compare
 * exactly (a truncated string comparison would silently drop longer
 * documents from quorum results). */
static void value_fingerprint(const cJSON *value, char out[33])
{
    char *vs = json_print(value);
    if (!vs) {
        out[0] = '\0';
        return;
    }
    edb_md5_hex(vs, strlen(vs), out);
    free(vs);
}

/* Merges row arrays [{"id","timestamp","value"},..] from several
 * replicas into a plain value array containing only records where at
 * least `required` copies agree verbatim; conflicts resolve LWW. */
static cJSON *merge_agreed_rows(cJSON **row_sets, size_t nsets,
                                int required)
{
    /* collect distinct ids */
    cJSON *out = cJSON_CreateArray();
    if (!out) {
        return NULL;
    }

    typedef struct {
        char id[512];
        char fp[33];            /* fingerprint of the winning value */
        cJSON *best_value;      /* borrowed from winner_set */
        long long best_ts;
        int agree;
    } entry;
    size_t entries_cap = 256;
    entry *entries = malloc(entries_cap * sizeof(*entries));
    if (!entries) {
        cJSON_Delete(out);
        return NULL;
    }
    size_t nentries = 0;

    for (size_t s = 0; s < nsets; s++) {
        const cJSON *rows = cJSON_GetObjectItem(row_sets[s], "rows");
        if (!cJSON_IsArray(rows)) {
            continue;
        }
        const cJSON *r = NULL;
        cJSON_ArrayForEach(r, rows) {
            const cJSON *jid = cJSON_GetObjectItemCaseSensitive(r, "id");
            const cJSON *jts =
                cJSON_GetObjectItemCaseSensitive(r, "timestamp");
            const cJSON *jval =
                cJSON_GetObjectItemCaseSensitive(r, "value");
            if (!cJSON_IsString(jid) || !jid->valuestring ||
                !cJSON_IsObject(jval)) {
                continue;
            }
            long long ts = cJSON_IsNumber(jts)
                               ? (long long)jts->valuedouble
                               : 0;

            entry *e = NULL;
            for (size_t k = 0; k < nentries; k++) {
                if (strcmp(entries[k].id, jid->valuestring) == 0) {
                    e = &entries[k];
                    break;
                }
            }
            if (!e) {
                if (nentries == entries_cap) {
                    size_t grown_cap = entries_cap * 2;
                    entry *grown =
                        realloc(entries, grown_cap * sizeof(*grown));
                    if (!grown) {
                        continue;
                    }
                    entries = grown;
                    entries_cap = grown_cap;
                }
                e = &entries[nentries++];
                memset(e, 0, sizeof(*e));
                snprintf(e->id, sizeof(e->id), "%.511s",
                         jid->valuestring);
            }

            /* agreement is decided on the canonical value fingerprint */
            char fp[33];
            value_fingerprint(jval, fp);
            bool same = e->best_value && e->fp[0] && fp[0] &&
                        strcmp(fp, e->fp) == 0;
            if (same) {
                e->agree++;
            }
            if (!e->best_value || ts > e->best_ts) {
                e->best_value = (cJSON *)jval;
                snprintf(e->fp, sizeof(e->fp), "%s", fp);
                e->best_ts = ts;
                e->agree = same ? e->agree : 1;
            }
        }
    }

    for (size_t k = 0; k < nentries; k++) {
        if (entries[k].best_value && entries[k].agree >= required) {
            cJSON *copy = cJSON_Duplicate(entries[k].best_value, 1);
            if (copy) {
                cJSON_AddItemToArray(out, copy);
            }
        }
    }
    free(entries);
    return out;
}
/* Like merge_agreed_rows but keeps id/timestamp metadata in the output
 * rows so a cross-keyspace query can reorder them afterwards. */
static cJSON *merge_agreed_rows_meta(cJSON **row_sets, size_t nsets,
                                     int required)
{
    cJSON *out = cJSON_CreateArray();
    if (!out) {
        return NULL;
    }
    typedef struct {
        char id[512];
        char fp[33];
        cJSON *best_value;
        long long best_ts;
        int agree;
    } entry;
    size_t entries_cap = 256;
    entry *entries = malloc(entries_cap * sizeof(*entries));
    if (!entries) {
        cJSON_Delete(out);
        return NULL;
    }
    size_t nentries = 0;

    for (size_t s = 0; s < nsets; s++) {
        const cJSON *rows = cJSON_GetObjectItem(row_sets[s], "rows");
        if (!cJSON_IsArray(rows)) {
            continue;
        }
        const cJSON *r = NULL;
        cJSON_ArrayForEach(r, rows) {
            const cJSON *jid = cJSON_GetObjectItemCaseSensitive(r, "id");
            const cJSON *jts =
                cJSON_GetObjectItemCaseSensitive(r, "timestamp");
            const cJSON *jval =
                cJSON_GetObjectItemCaseSensitive(r, "value");
            if (!cJSON_IsString(jid) || !jid->valuestring ||
                !cJSON_IsObject(jval)) {
                continue;
            }
            long long ts = cJSON_IsNumber(jts)
                               ? (long long)jts->valuedouble
                               : 0;
            entry *e = NULL;
            for (size_t k = 0; k < nentries; k++) {
                if (strcmp(entries[k].id, jid->valuestring) == 0) {
                    e = &entries[k];
                    break;
                }
            }
            if (!e) {
                if (nentries == entries_cap) {
                    size_t grown_cap = entries_cap * 2;
                    entry *grown =
                        realloc(entries, grown_cap * sizeof(*grown));
                    if (!grown) {
                        continue;
                    }
                    entries = grown;
                    entries_cap = grown_cap;
                }
                e = &entries[nentries++];
                memset(e, 0, sizeof(*e));
                snprintf(e->id, sizeof(e->id), "%.511s",
                         jid->valuestring);
            }
            char fp[33];
            value_fingerprint(jval, fp);
            bool same = e->best_value && e->fp[0] && fp[0] &&
                        strcmp(fp, e->fp) == 0;
            if (same) {
                e->agree++;
            }
            if (!e->best_value || ts > e->best_ts) {
                e->best_value = (cJSON *)jval;
                snprintf(e->fp, sizeof(e->fp), "%s", fp);
                e->best_ts = ts;
                e->agree = same ? e->agree : 1;
            }
        }
    }

    for (size_t k = 0; k < nentries; k++) {
        if (entries[k].best_value && entries[k].agree >= required) {
            cJSON *row = cJSON_CreateObject();
            cJSON_AddStringToObject(row, "id", entries[k].id);
            cJSON_AddNumberToObject(row, "timestamp",
                                    (double)entries[k].best_ts);
            cJSON_AddItemToObject(row, "value",
                                  cJSON_Duplicate(entries[k].best_value, 1));
            cJSON_AddItemToArray(out, row);
        }
    }
    free(entries);
    return out;
}


/* Same merge but emits plain id strings (for the ids operation). */
static cJSON *merge_agreed_ids(char ***id_lists, size_t *counts,
                               size_t nsets, int required)
{
    typedef struct {
        char id[512];
        int seen;
    } entry;
    size_t entries_cap = 256;
    entry *entries = malloc(entries_cap * sizeof(*entries));
    if (!entries) {
        return NULL;
    }
    size_t nentries = 0;

    for (size_t s = 0; s < nsets; s++) {
        for (size_t i = 0; i < counts[s]; i++) {
            entry *e = NULL;
            for (size_t k = 0; k < nentries; k++) {
                if (strcmp(entries[k].id, id_lists[s][i]) == 0) {
                    e = &entries[k];
                    break;
                }
            }
            if (!e) {
                if (nentries == entries_cap) {
                    size_t grown_cap = entries_cap * 2;
                    entry *grown =
                        realloc(entries, grown_cap * sizeof(*grown));
                    if (!grown) {
                        continue;
                    }
                    entries = grown;
                    entries_cap = grown_cap;
                }
                e = &entries[nentries++];
                memset(e, 0, sizeof(*e));
                snprintf(e->id, sizeof(e->id), "%.511s",
                         id_lists[s][i]);
            }
            e->seen++;
        }
    }

    cJSON *arr = cJSON_CreateArray();
    if (!arr) {
        return NULL;
    }
    for (size_t k = 0; k < nentries; k++) {
        if (entries[k].seen >= required) {
            cJSON_AddItemToArray(arr, cJSON_CreateString(entries[k].id));
        }
    }
    free(entries);
    return arr;
}

/* Builds {"q":..,"db":..,...} request skeleton shared by all reads. */
static cJSON *make_request(const char *q, const char *db,
                           const char *partition, const char *keyspace)
{
    cJSON *o = cJSON_CreateObject();
    if (!o) {
        return NULL;
    }
    cJSON_AddStringToObject(o, "q", q);
    cJSON_AddStringToObject(o, "db", db);
    cJSON_AddStringToObject(o, "partition", partition);
    cJSON_AddStringToObject(o, "keyspace", keyspace);
    return o;
}

/* True when quorum reads should engage: clustered, rf > 1. */
static bool quorum_applies(edb_repl *rp, const char *db)
{
    if (!rp || !rp->cluster || !rp->read) {
        return false;
    }
    edb_database_info info;
    return edb_database_get(rp->cfg, db, &info) &&
           info.replication_factor > 1;
}

static int read_quorum(edb_repl *rp, const char *db, const char *partition,
                       const char *keyspace, bool *self_holder)
{
    char holders[MAX_PEERS_SNAPSHOT][EDB_NODE_ID_MAX];
    size_t count = holder_ids(rp, partition, keyspace,
                              replication_factor(rp, db), holders);
    if (count == 0) {
        *self_holder = true;
        return 1;
    }
    *self_holder = id_in_holders(rp->self_id, holders, count);
    return (int)count / 2 + 1;
}


/* Collects filter/field arrays from JSON string arrays. */
static char **strings_from_json(const cJSON *array, size_t *count_out)
{
    *count_out = 0;
    if (!cJSON_IsArray(array)) {
        return NULL;
    }
    int count = cJSON_GetArraySize(array);
    if (count <= 0) {
        return NULL;
    }
    char **strings = calloc((size_t)count + 1, sizeof(char *));
    if (!strings) {
        return NULL;
    }
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, array) {
        if (!cJSON_IsString(item) || !item->valuestring) {
            continue;
        }
        strings[*count_out] = strdup(item->valuestring);
        if (!strings[*count_out]) {
            edb_free_strings(strings);
            *count_out = 0;
            return NULL;
        }
        (*count_out)++;
    }
    if (*count_out == 0) {
        free(strings);
        return NULL;
    }
    return strings;
}

cJSON *edb_repl_read_get(edb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const char *id)
{
    bool self_holder = true;
    int required = quorum_applies(rp, db)
                       ? read_quorum(rp, db, partition, keyspace, &self_holder)
                       : 1;
    long long local_ts = 0;
    cJSON *local = (rp && rp->cfg_engine && self_holder)
                       ? edb_get_ts(rp->cfg_engine, partition, keyspace,
                                    id, &local_ts)
                       : NULL;
    if (!quorum_applies(rp, db)) {
        return local;
    }

    cJSON *req = make_request("get", db, partition, keyspace);
    if (!req) {
        cJSON_Delete(local);
        return NULL;
    }
    cJSON_AddStringToObject(req, "id", id);
    cJSON *replies[MAX_REPLIES];
    size_t n = query_all(rp, req, replies, MAX_REPLIES);
    cJSON_Delete(req);

    int responses = (int)n + (self_holder ? 1 : 0);
    if (responses < required) {
        for (size_t i = 0; i < n; i++) {
            cJSON_Delete(replies[i]);
        }
        cJSON_Delete(local);
        return NULL;
    }

    typedef struct {
        char fp[33];
        long long ts;
        cJSON *sample;
        int votes;
    } group;
    group groups[MAX_REPLIES + 1];
    size_t ngroups = 0;
    int absent = self_holder && !local ? 1 : 0;

    for (size_t i = 0; i < n; i++) {
        const cJSON *row = cJSON_GetObjectItem(replies[i], "row");
        if (!cJSON_IsObject(row)) {
            absent++;
            continue;
        }
        const cJSON *timestamp =
            cJSON_GetObjectItemCaseSensitive(row, "timestamp");
        const cJSON *value =
            cJSON_GetObjectItemCaseSensitive(row, "value");
        if (!cJSON_IsObject(value)) {
            absent++;
            continue;
        }
        char fingerprint[33];
        value_fingerprint(value, fingerprint);
        if (!fingerprint[0]) {
            continue;
        }
        group *candidate = NULL;
        for (size_t k = 0; k < ngroups; k++) {
            if (strcmp(groups[k].fp, fingerprint) == 0) {
                candidate = &groups[k];
                break;
            }
        }
        if (!candidate && ngroups < MAX_REPLIES + 1) {
            candidate = &groups[ngroups++];
            memset(candidate, 0, sizeof(*candidate));
            snprintf(candidate->fp, sizeof(candidate->fp), "%s",
                     fingerprint);
            candidate->sample = (cJSON *)value;
        }
        if (candidate) {
            candidate->votes++;
            long long ts = cJSON_IsNumber(timestamp)
                               ? (long long)timestamp->valuedouble
                               : 0;
            if (ts > candidate->ts) {
                candidate->ts = ts;
            }
        }
    }

    if (local) {
        char fingerprint[33];
        value_fingerprint(local, fingerprint);
        group *candidate = NULL;
        for (size_t k = 0; k < ngroups; k++) {
            if (strcmp(groups[k].fp, fingerprint) == 0) {
                candidate = &groups[k];
                break;
            }
        }
        if (!candidate && fingerprint[0] && ngroups < MAX_REPLIES + 1) {
            candidate = &groups[ngroups++];
            memset(candidate, 0, sizeof(*candidate));
            snprintf(candidate->fp, sizeof(candidate->fp), "%s",
                     fingerprint);
            candidate->sample = local;
        }
        if (candidate) {
            candidate->votes++;
            if (local_ts > candidate->ts) {
                candidate->ts = local_ts;
            }
        }
    }

    group *winner = NULL;
    if (absent < required) {
        for (size_t k = 0; k < ngroups; k++) {
            if (groups[k].votes >= required &&
                (!winner || groups[k].ts > winner->ts)) {
                winner = &groups[k];
            }
        }
    }
    cJSON *result = winner ? cJSON_Duplicate(winner->sample, 1) : NULL;
    for (size_t i = 0; i < n; i++) {
        cJSON_Delete(replies[i]);
    }
    cJSON_Delete(local);
    return result;
}

cJSON *edb_repl_read_all(edb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const cJSON *filters)
{
    if (!quorum_applies(rp, db)) {
        return rp ? edb_all(rp->cfg_engine, partition, keyspace, filters)
                  : NULL;
    }

    cJSON *req = make_request("all_ts", db, partition, keyspace);
    if (!req) {
        return NULL;
    }

    cJSON *sets[MAX_REPLIES + 1];
    size_t n = 0;
    bool self_holder = false;
    int required = read_quorum(rp, db, partition, keyspace, &self_holder);

    cJSON *local_rows = self_holder
                            ? edb_all_ts(rp->cfg_engine, partition, keyspace,
                                         filters)
                            : NULL;
    if (local_rows) {
        cJSON *wrap = cJSON_CreateObject();
        if (wrap) {
            cJSON_AddItemToObject(wrap, "rows", local_rows);
            sets[n++] = wrap;
        } else {
            cJSON_Delete(local_rows);
        }
    }

    if (req) {
        if (filters) {
            cJSON_AddItemToObject(req, "filters", cJSON_Duplicate(filters, 1));
        }
        cJSON *replies[MAX_REPLIES];
        size_t got = query_all(rp, req, replies, MAX_REPLIES);
        cJSON_Delete(req);
        for (size_t i = 0; i < got; i++) {
            sets[n++] = replies[i];
        }
    }

    if ((int)n < required) {
        for (size_t i = 0; i < n; i++) {
            cJSON_Delete(sets[i]);
        }
        return cJSON_CreateArray();
    }
    cJSON *merged = merge_agreed_rows(sets, n, required);
    for (size_t i = 0; i < n; i++) {
        cJSON_Delete(sets[i]);
    }
    return merged ? merged : cJSON_CreateArray();
}

char **edb_repl_read_ids(edb_repl *rp, const char *db, const char *partition,
                         const char *keyspace, const cJSON *filters,
                         size_t *count_out)
{
    *count_out = 0;
    if (!quorum_applies(rp, db)) {
        return rp ? edb_ids(rp->cfg_engine, partition, keyspace, filters,
                            count_out)
                  : NULL;
    }

    cJSON *req = make_request("ids", db, partition, keyspace);
    if (!req) {
        return NULL;
    }
    if (filters) {
        cJSON_AddItemToObject(req, "filters", cJSON_Duplicate(filters, 1));
    }

    char **lists[MAX_REPLIES + 1];
    size_t counts[MAX_REPLIES + 1];
    size_t n = 0;
    bool self_holder = false;
    int required = read_quorum(rp, db, partition, keyspace, &self_holder);

    if (self_holder) {
        lists[n] = edb_ids(rp->cfg_engine, partition, keyspace, filters,
                           &counts[n]);
        if (!lists[n]) {
            counts[n] = 0;
        }
        n++;
    }

    cJSON *replies[MAX_REPLIES];
    size_t got = query_all(rp, req, replies, MAX_REPLIES);
    cJSON_Delete(req);
    for (size_t i = 0; i < got && n < MAX_REPLIES + 1; i++) {
        const cJSON *ids = cJSON_GetObjectItem(replies[i], "ids");
        lists[n] = strings_from_json(ids, &counts[n]);
        if (!lists[n]) {
            lists[n] = NULL;
            counts[n] = 0;
        }
        n++;
    }
    for (size_t i = 0; i < got; i++) {
        cJSON_Delete(replies[i]);
    }

    if ((int)n < required) {
        for (size_t i = 0; i < n; i++) {
            edb_free_strings(lists[i]);
        }
        return NULL;
    }
    cJSON *agreed = merge_agreed_ids(lists, counts, n, required);
    for (size_t i = 0; i < n; i++) {
        edb_free_strings(lists[i]);
    }
    if (!agreed) {
        return NULL;
    }

    size_t cnt = (size_t)cJSON_GetArraySize(agreed);
    char **out = malloc((cnt + 1) * sizeof(char *));
    if (!out) {
        cJSON_Delete(agreed);
        return NULL;
    }
    size_t w = 0;
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, agreed) {
        if (cJSON_IsString(item) && item->valuestring) {
            out[w++] = strdup(item->valuestring);
        }
    }
    out[w] = NULL;
    cJSON_Delete(agreed);
    *count_out = w;
    return out;
}

cJSON *edb_repl_read_query(edb_repl *rp, const char *db,
                           const char *partition, const char *keyspace,
                           const cJSON *filters)
{
    if (!quorum_applies(rp, db)) {
        return rp ? edb_query(rp->cfg_engine, partition, keyspace, filters)
                  : NULL;
    }

    cJSON *req = make_request("query_ts", db, partition, keyspace);
    if (!req) {
        return NULL;
    }
    if (filters) {
        cJSON_AddItemToObject(req, "filters", cJSON_Duplicate(filters, 1));
    }

    cJSON *sets[MAX_REPLIES + 1];
    size_t n = 0;
    bool self_holder = false;
    int required = read_quorum(rp, db, partition, keyspace, &self_holder);
    cJSON *local_rows = self_holder
                            ? edb_query_ts(rp->cfg_engine, partition, keyspace,
                                           filters)
                            : NULL;
    if (local_rows) {
        cJSON *wrap = cJSON_CreateObject();
        if (wrap) {
            cJSON_AddItemToObject(wrap, "rows", local_rows);
            sets[n++] = wrap;
        } else {
            cJSON_Delete(local_rows);
        }
    }

    cJSON *replies[MAX_REPLIES];
    size_t got = query_all(rp, req, replies, MAX_REPLIES);
    cJSON_Delete(req);
    for (size_t i = 0; i < got; i++) {
        sets[n++] = replies[i];
    }

    if ((int)n < required) {
        for (size_t i = 0; i < n; i++) {
            cJSON_Delete(sets[i]);
        }
        return cJSON_CreateArray();
    }
    cJSON *merged = merge_agreed_rows(sets, n, required);
    for (size_t i = 0; i < n; i++) {
        cJSON_Delete(sets[i]);
    }
    return merged ? merged : cJSON_CreateArray();
}

/* Like edb_repl_read_query but returns timestamp-tagged rows
 * {"id","timestamp","value"} so a partition-wide query can merge several
 * keyspaces and reorder them afterwards. */
cJSON *edb_repl_read_query_meta(edb_repl *rp, const char *db,
                                const char *partition, const char *keyspace,
                                const cJSON *filters)
{
    if (!quorum_applies(rp, db)) {
        return rp ? edb_query_ts(rp->cfg_engine, partition, keyspace,
                                 filters)
                  : NULL;
    }

    cJSON *req = make_request("query_ts", db, partition, keyspace);
    if (!req) {
        return NULL;
    }
    if (filters) {
        cJSON_AddItemToObject(req, "filters", cJSON_Duplicate(filters, 1));
    }

    cJSON *sets[MAX_REPLIES + 1];
    size_t n = 0;
    bool self_holder = false;
    int required = read_quorum(rp, db, partition, keyspace, &self_holder);
    cJSON *local_rows = self_holder
                            ? edb_query_ts(rp->cfg_engine, partition, keyspace,
                                           filters)
                            : NULL;
    if (local_rows) {
        cJSON *wrap = cJSON_CreateObject();
        if (wrap) {
            cJSON_AddItemToObject(wrap, "rows", local_rows);
            sets[n++] = wrap;
        } else {
            cJSON_Delete(local_rows);
        }
    }

    cJSON *replies[MAX_REPLIES];
    size_t got = query_all(rp, req, replies, MAX_REPLIES);
    cJSON_Delete(req);
    for (size_t i = 0; i < got; i++) {
        sets[n++] = replies[i];
    }

    if ((int)n < required) {
        for (size_t i = 0; i < n; i++) {
            cJSON_Delete(sets[i]);
        }
        return cJSON_CreateArray();
    }
    cJSON *merged = merge_agreed_rows_meta(sets, n, required);
    for (size_t i = 0; i < n; i++) {
        cJSON_Delete(sets[i]);
    }
    return merged ? merged : cJSON_CreateArray();
}

/* ------------------------------------------------------------------ */
/* delta catch-up                                                      */