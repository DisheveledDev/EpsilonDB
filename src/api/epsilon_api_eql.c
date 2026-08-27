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
 */

#include "epsilon_api_internal.h"

#include <stdlib.h>
#include <string.h>

#include "../eql/epsilon_eql.h"

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

    const edb_eql_ctx ctx = { g_ctx.engine, g_ctx.config, g_repl };
    char *json_out = NULL;
    int code = edb_eql_execute(&ctx, sql, groups, req->trusted, &json_out);
    /* sql points into the request body's parsed JSON: free only after the
     * engine finished reading it */
    cJSON_Delete(body);

    /* the engine already produces exactly the response shape this surface
     * wants ({"status":...} success and error objects alike); hand its
     * text straight to the client instead of re-serializing through cJSON */
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
