/* epsilon_api_eql.c - HTTP surface for the EQL engine (stage 8, eql-d).
 *
 * POST /eql          untrusted port: normal Bearer+password auth; partition
 *                    permission masks are enforced by the engine itself.
 * POST /admin/eql    trusted paths (admin socket): full rights, no auth.
 *
 * Request body: {"sql": "..."} - one EQL statement per request.
 * Response body mirrors edb_eql_execute output (result sets or DML
 * status). While a restore lock is held every /eql request answers 503
 * like all other data endpoints.
 *
 * Workload analytics: each statement's impact is attributed to every shard
 * it references so /eql traffic shows up in GET /admin/analytics exactly
 * like the equivalent REST calls (reads for SELECT, writes for
 * INSERT/UPDATE, deletes for DELETE). Snapshots flush into the replicated
 * config_analytics store on the usual interval.
 */

#include "epsilon_api_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../eql/epsilon_eql.h"

static long long eql_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

/* Splits a dotted db.partition.keyspace reference and records one analytics
 * event of the statement's class against it. */
static void record_ref(eql_kind kind, const char *ref, long long elapsed,
                       size_t nrefs)
{
    const char *p1 = strchr(ref, '.');
    if (!p1) {
        return;
    }
    p1++;
    char scratch[512];
    snprintf(scratch, sizeof(scratch), "%s", p1);
    char *dot2 = strrchr(scratch, '.');
    if (!dot2) {
        return;
    }
    *dot2 = '\0';
    const char *ks = dot2 + 1;
    /* whole-statement latency spread over referenced shards so a join
     * across three shards does not triple-count the wall time */
    long long per_shard =
        nrefs > 1 ? (long long)(elapsed / (double)nrefs + 0.5) : elapsed;
    switch (kind) {
    case EQL_KIND_INSERT:
        edb_analytics_record_write(g_analytics, scratch, ks, false,
                                   per_shard);
        break;
    case EQL_KIND_UPDATE:
        edb_analytics_record_write(g_analytics, scratch, ks, true,
                                   per_shard);
        break;
    case EQL_KIND_DELETE:
        edb_analytics_record_delete(g_analytics, scratch, ks, per_shard);
        break;
    default:   /* SELECT / EXPLAIN: treated as collection reads */
        edb_analytics_record_query(g_analytics, scratch, ks, NULL, 0,
                                   elapsed);
        break;
    }
}

static bool handle_eql_common(const edb_http_request *req,
                              edb_http_response *res)
{
    if (restore_blocked(res)) {
        return true;
    }

    /* deliberately parsed AFTER restore_blocked so a locked node does not
     * even parse attacker-controlled bodies */
    cJSON *body = NULL;
    const char *sql = NULL;
    if (!body_json(req, &body) || !cJSON_IsObject(body)) {
        cJSON_Delete(body);
        respond_error(res, 400, "body must be a JSON object");
        return true;
    }
    const cJSON *jsql = cJSON_GetObjectItemCaseSensitive(body, "sql");
    if (!cJSON_IsString(jsql) || !jsql->valuestring || !*jsql->valuestring) {
        cJSON_Delete(body);
        respond_error(res, 400,
                      "expected a non-empty \"sql\" string in the body");
        return true;
    }
    sql = jsql->valuestring;

    bool auth_ok = false;
    uint64_t groups = authenticate(req, req, &auth_ok);
    if (!auth_ok) {
        cJSON_Delete(body);
        respond_error(res, 401, "unauthorized");
        return true;
    }

    eql_kind kind = (eql_kind)edb_eql_classify(sql);
    enum { EQL_REF_CAP = 16 };   /* matches the engine's table cap */
    char refs[EQL_REF_CAP][512];
    size_t nrefs = edb_eql_references(sql, refs, EQL_REF_CAP);

    const edb_eql_ctx ctx = { g_ctx.engine, g_ctx.config, g_repl };
    long long started = eql_now_us();
    char *json_out = NULL;
    int code = edb_eql_execute(&ctx, sql, groups, req->trusted, &json_out);
    long long elapsed = eql_now_us() - started;
    /* sql points into the request body's parsed JSON: free only after the
     * engine finished reading it */
    cJSON_Delete(body);

    /* attribute impact per referenced shard regardless of per-record DML
     * outcomes, mirroring how failed REST writes still count as writes */
    if (g_analytics && nrefs > 0 && code != 500) {
        for (size_t r = 0; r < nrefs; r++) {
            record_ref(kind, refs[r], elapsed, nrefs);
        }
    }

    res->status = code;
    res->content_type = "application/json";
    res->body = json_out;
    res->body_len = json_out ? strlen(json_out) : 0;
    if (!res->body) {
        respond_error(res, 500, "engine returned no response");
    }
    return true;
}

/* Untrusted client-facing endpoint. The body is parsed once and serves
 * both purposes: credential extraction in authenticate() (password body
 * key) and SQL extraction. It therefore stays alive until after the
 * engine has finished with the statement text. */
bool handle_data_eql(const edb_http_request *req, edb_http_response *res)
{
    return handle_eql_common(req, res);
}

/* Admin-socket alias: same behavior, always full rights via req->trusted.
 * Requests arriving on TCP /admin/eql still pass authenticate() with their
 * bearer token; only socket traffic bypasses auth. */
bool handle_admin_eql(const edb_http_request *req, edb_http_response *res)
{
    return handle_eql_common(req, res);
}
