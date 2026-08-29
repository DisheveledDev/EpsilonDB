/* epsilon_api_lua.c - HTTP admin surface for the Lua scripting engine
 * Management endpoints for function records, all gated by
 * admin auth:
 *
 *   GET    /admin/code[?type=function|action]   list records
 *   POST   /admin/code                          create/update a record
 *   POST   /admin/code/validate                 compile-check a snippet
 *   GET    /admin/code/<name>                   fetch one record
 *   DELETE /admin/code/<name>                   remove one record
 *
 * A record is either a named function ({"type":"function","name":..,
 * "code":..}) or a database action ({"type":"action","name":
 * "<database>_<partition>_<keyspace>_<event>", database/partition/
 * keyspace/event fields for the console, "code":..}). Creating a record
 * without "code" pre-fills the function skeleton:
 *
 *   function generateId ()
 *
 *   end
 *
 *   function demo_people_staff_beforeDelete (entity, id)
 *
 *   end
 *
 * Records live in the config_code keyspace of __system__ (see
 * edb_code_save) and replicate to every node automatically.
 */

#include "epsilon_api_internal.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Accepts only printable, path-safe names. */
static bool code_name_ok(const char *name)
{
    if (!name || !*name || strlen(name) >= 128) {
        return false;
    }
    for (const char *p = name; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (c < 0x20 || c == 0x7f || c == '/' || c == '\\') {
            return false;
        }
    }
    return true;
}

/* Accepts only valid Lua identifiers: the name must be definable as a
 * function in Lua source. */
static bool lua_ident_ok(const char *name)
{
    if (!name || !*name || strlen(name) >= 128) {
        return false;
    }
    for (const char *p = name; *p; p++) {
        unsigned char c = (unsigned char)*p;
        if (c == '_' || (c >= 'a' && c <= 'z') ||
            (c >= 'A' && c <= 'Z') ||
            (p != name && c >= '0' && c <= '9')) {
            continue;
        }
        return false;
    }
    return true;
}

/* GET /admin/code - list functions and/or actions. */
static bool code_list(const edb_http_request *req, edb_http_response *res)
{
    char type[32] = "";
    query_param(req, "type", type, sizeof(type));
    bool want_fn = type[0] == '\0' || strcmp(type, "function") == 0;
    bool want_act = type[0] == '\0' || strcmp(type, "action") == 0;
    if (strcmp(type, "function") != 0 && strcmp(type, "action") != 0 &&
        type[0] != '\0') {
        respond_error(res, 400, "type must be 'function' or 'action'");
        return true;
    }
    cJSON *all = edb_code_list(g_ctx.config);
    cJSON *out = cJSON_CreateArray();
    if (!all || !out) {
        cJSON_Delete(all);
        cJSON_Delete(out);
        respond_error(res, 500, "listing failed");
        return true;
    }
    const cJSON *rec = NULL;
    cJSON_ArrayForEach(rec, all) {
        const cJSON *rtype =
            cJSON_GetObjectItemCaseSensitive(rec, "type");
        if (!cJSON_IsString(rtype) || !rtype->valuestring) {
            continue;
        }
        bool is_fn = strcmp(rtype->valuestring, "function") == 0;
        bool is_act = strcmp(rtype->valuestring, "action") == 0;
        if ((is_fn && want_fn) || (is_act && want_act)) {
            /* duplicate: `out` must own its items independently of `all` */
            cJSON_AddItemToArray(out, cJSON_Duplicate(rec, 1));
        }
    }
    cJSON_Delete(all);
    respond_json(res, 200, out);
    return true;
}

/* POST /admin/code - create/update a function or a database action.
 * When "code" is absent the skeleton is generated from the name (or the
 * scope + event) so the entry always defines its own function. */
static bool code_save(const edb_http_request *req, edb_http_response *res)
{
    cJSON *body = NULL;
    if (!body_json(req, &body) || !cJSON_IsObject(body)) {
        cJSON_Delete(body);
        respond_error(res, 400, "body must be a JSON object");
        return true;
    }
    const cJSON *jtype = cJSON_GetObjectItemCaseSensitive(body, "type");
    if (!cJSON_IsString(jtype) || !jtype->valuestring) {
        cJSON_Delete(body);
        respond_error(res, 400,
                      "a \"type\" (function|action) is required");
        return true;
    }

    char name[512] = "";
    char *code = NULL;             /* malloc'd: supplied or generated */
    cJSON *record = cJSON_CreateObject();
    if (!record) {
        cJSON_Delete(body);
        respond_error(res, 500, "allocation failed");
        return true;
    }

    bool ok = false;
    if (strcmp(jtype->valuestring, "function") == 0) {
        const cJSON *jname = cJSON_GetObjectItemCaseSensitive(body, "name");
        if (!cJSON_IsString(jname) || !jname->valuestring ||
            !code_name_ok(jname->valuestring)) {
            cJSON_Delete(body);
            cJSON_Delete(record);
            respond_error(res, 400, "a valid \"name\" is required");
            return true;
        }
        if (!lua_ident_ok(jname->valuestring)) {
            cJSON_Delete(body);
            cJSON_Delete(record);
            respond_error(res, 400,
                          "function names must be valid Lua identifiers "
                          "(letters, digits, underscores, not starting "
                          "with a digit)");
            return true;
        }
        snprintf(name, sizeof(name), "%s", jname->valuestring);
        const cJSON *jcode = cJSON_GetObjectItemCaseSensitive(body, "code");
        if (cJSON_IsString(jcode) && jcode->valuestring &&
            jcode->valuestring[0]) {
            code = strdup(jcode->valuestring);
        } else {
            size_t cap = strlen(name) + 32;
            code = malloc(cap);
            if (code) {
                snprintf(code, cap, "function %s ()\n\nend\n", name);
            }
        }
        cJSON_AddStringToObject(record, "type", "function");
    } else if (strcmp(jtype->valuestring, "action") == 0) {
        char database[128] = "";
        char partition[256] = "";
        char keyspace[128] = "";
        const cJSON *jdb =
            cJSON_GetObjectItemCaseSensitive(body, "database");
        const cJSON *jpart =
            cJSON_GetObjectItemCaseSensitive(body, "partition");
        const cJSON *jks =
            cJSON_GetObjectItemCaseSensitive(body, "keyspace");
        if (cJSON_IsString(jdb) && jdb->valuestring) {
            snprintf(database, sizeof(database), "%s", jdb->valuestring);
        }
        if (cJSON_IsString(jpart) && jpart->valuestring) {
            snprintf(partition, sizeof(partition), "%s", jpart->valuestring);
        }
        if (cJSON_IsString(jks) && jks->valuestring) {
            snprintf(keyspace, sizeof(keyspace), "%s", jks->valuestring);
        }
        if (!database[0] || !partition[0] || !keyspace[0]) {
            cJSON_Delete(body);
            cJSON_Delete(record);
            respond_error(res, 400,
                          "an action requires \"database\", \"partition\" "
                          "and \"keyspace\"");
            return true;
        }
        if (!lua_ident_ok(database) || !lua_ident_ok(partition) ||
            !lua_ident_ok(keyspace)) {
            cJSON_Delete(body);
            cJSON_Delete(record);
            respond_error(res, 400,
                          "database/partition/keyspace names must be valid "
                          "Lua identifiers (letters, digits, underscores, "
                          "not starting with a digit) so the action "
                          "function name is definable");
            return true;
        }
        const cJSON *jevent = cJSON_GetObjectItemCaseSensitive(body, "event");
        edb_lua_event event;
        if (!cJSON_IsString(jevent) || !jevent->valuestring ||
            !edb_lua_event_parse(jevent->valuestring, &event)) {
            cJSON_Delete(body);
            cJSON_Delete(record);
            respond_error(res, 400,
                          "a valid \"event\" is required (beforeInsert, "
                          "afterInsert, beforeUpdate, afterUpdate, "
                          "beforeDelete, afterDelete)");
            return true;
        }
        if (!edb_lua_action_name(database, partition, keyspace, event,
                                 name, sizeof(name)) ||
            !code_name_ok(name)) {
            cJSON_Delete(body);
            cJSON_Delete(record);
            respond_error(res, 400, "the action name is too long");
            return true;
        }
        const cJSON *jcode = cJSON_GetObjectItemCaseSensitive(body, "code");
        if (cJSON_IsString(jcode) && jcode->valuestring &&
            jcode->valuestring[0]) {
            code = strdup(jcode->valuestring);
        } else {
            size_t cap = strlen(name) + 48;
            code = malloc(cap);
            if (code) {
                snprintf(code, cap,
                         "function %s (entity, id)\n\nend\n", name);
            }
        }
        cJSON_AddStringToObject(record, "type", "action");
        cJSON_AddStringToObject(record, "database", database);
        cJSON_AddStringToObject(record, "partition", partition);
        cJSON_AddStringToObject(record, "keyspace", keyspace);
        cJSON_AddStringToObject(record, "event",
                                edb_lua_event_name(event));
    } else {
        cJSON_Delete(body);
        cJSON_Delete(record);
        respond_error(res, 400, "type must be 'function' or 'action'");
        return true;
    }

    if (!code) {
        cJSON_Delete(body);
        cJSON_Delete(record);
        respond_error(res, 500, "allocation failed");
        return true;
    }
    char *err = NULL;
    if (!edb_lua_validate(code, &err)) {
        char msg[640];
        snprintf(msg, sizeof(msg), "code does not compile: %s",
                 err ? err : "syntax error");
        free(err);
        free(code);
        cJSON_Delete(body);
        cJSON_Delete(record);
        respond_error(res, 400, msg);
        return true;
    }
    cJSON_AddStringToObject(record, "code", code);
    ok = edb_code_save(g_ctx.config, name, record);
    free(code);
    cJSON_Delete(body);
    cJSON_Delete(record);
    if (!ok) {
        respond_error(res, 500, "saving code record failed");
        return true;
    }
    res->status = 200;
    res->content_type = "application/json";
    res->body = edb_http_body_printf(&res->body_len,
                                     "{\"status\":\"ok\",\"name\":\"%s\"}",
                                     name);
    return true;
}

/* POST /admin/code/validate - compile-check a snippet without saving. */
static bool code_validate(const edb_http_request *req,
                          edb_http_response *res)
{
    cJSON *body = NULL;
    if (!body_json(req, &body) || !cJSON_IsObject(body)) {
        cJSON_Delete(body);
        respond_error(res, 400, "body must be a JSON object");
        return true;
    }
    const cJSON *jcode = cJSON_GetObjectItemCaseSensitive(body, "code");
    if (!cJSON_IsString(jcode)) {
        cJSON_Delete(body);
        respond_error(res, 400, "a \"code\" string is required");
        return true;
    }
    char *err = NULL;
    bool ok = edb_lua_validate(jcode->valuestring, &err);
    cJSON *out = cJSON_CreateObject();
    if (out) {
        cJSON_AddBoolToObject(out, "valid", ok ? 1 : 0);
        if (!ok) {
            cJSON_AddStringToObject(out, "error", err ? err : "syntax error");
        }
    }
    free(err);
    cJSON_Delete(body);
    respond_json(res, 200, out);
    return true;
}

/* GET/DELETE /admin/code/<name> - fetch/remove one record. */
static bool code_item(const edb_http_request *req, edb_http_response *res)
{
    const char *name = req->path + strlen("/admin/code/");
    if (!code_name_ok(name)) {
        respond_error(res, 400, "invalid code record name");
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        cJSON *record = edb_code_load(g_ctx.config, name);
        if (!record) {
            respond_error(res, 404, "no such code record");
            return true;
        }
        respond_json(res, 200, record);
        return true;
    }
    if (strcmp(req->method, "DELETE") == 0) {
        if (!edb_code_delete(g_ctx.config, name)) {
            respond_error(res, 404, "no such code record");
            return true;
        }
        res->status = 200;
        res->content_type = "application/json";
        res->body = edb_http_body_printf(&res->body_len,
                                         "{\"status\":\"deleted\","
                                         "\"name\":\"%s\"}",
                                         name);
        return true;
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

/* GET/POST /admin/code (list / save). */
bool handle_admin_code(const edb_http_request *req, edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "GET") == 0) {
        return code_list(req, res);
    }
    if (strcmp(req->method, "POST") == 0) {
        return code_save(req, res);
    }
    respond_error(res, 405, "method not allowed");
    return true;
}

/* POST /admin/code/validate (compile-check). */
bool handle_admin_code_validate(const edb_http_request *req,
                                edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    return code_validate(req, res);
}

/* GET/DELETE /admin/code/<name>. */
bool handle_admin_code_item(const edb_http_request *req,
                            edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    return code_item(req, res);
}
