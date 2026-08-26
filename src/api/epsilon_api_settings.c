/* epsilon_api_settings.c - settings
 * Part of the split epsilon_api module; see epsilon_api_internal.h.
 */

#include "epsilon_api_internal.h"
#include "version.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>


bool handle_settings(const edb_http_request *req,
                            edb_http_response *res)
{
    if (!require_admin_auth(req, res)) {
        return true;
    }
    const char *prefix = "/admin/settings";
    size_t plen = strlen(prefix);
    const char *rest = req->path + plen;   /* "" or "/<name>" */

    if (strcmp(req->method, "GET") == 0) {
        if (!*rest) {
            /* list all settings */
            size_t n = 0;
            char **names = edb_setting_list(g_ctx.config, &n);
            cJSON *arr = cJSON_CreateArray();
            for (size_t i = 0; names && i < n; i++) {
                cJSON *o = cJSON_CreateObject();
                cJSON_AddStringToObject(o, "name", names[i]);
                char *value = edb_setting_get(g_ctx.config, names[i]);
                cJSON *parsed = value ? cJSON_Parse(value) : NULL;
                if (parsed) {
                    cJSON_AddItemToObject(o, "value", parsed);
                } else {
                    cJSON_AddStringToObject(o, "value",
                                            value ? value : "null");
                }
                free(value);
                cJSON_AddItemToArray(arr, o);
            }
            edb_free_strings(names);
            respond_json(res, 200, arr);
            return true;
        }
        rest++;   /* skip '/' */
        char *value = edb_setting_get(g_ctx.config, rest);
        if (!value) {
            respond_error(res, 404, "unknown setting");
            return true;
        }
        res->status = 200;
        res->content_type = "application/json";
        res->body = value;
        res->body_len = strlen(value);
        return true;
    }

    if (strcmp(req->method, "POST") == 0 || strcmp(req->method, "PUT") == 0) {
        if (!*rest) {
            respond_error(res, 400,
                          "expected /admin/settings/<name> with JSON body");
            return true;
        }
        rest++;
        /* body must be valid JSON (any type) */
        cJSON *check = NULL;
        if (!body_json(req, &check)) {
            respond_error(res, 400, "body must be valid JSON");
            return true;
        }
        char *printed = cJSON_PrintUnformatted(check);
        cJSON_Delete(check);
        if (!edb_setting_set(g_ctx.config, rest, printed ? printed : "null")) {
            free(printed);
            respond_error(res, 500, "setting store failed");
            return true;
        }
        free(printed);
        res->status = 200;
        res->content_type = "application/json";
        res->body = edb_http_body_printf(&res->body_len,
                                         "{\"set\":\"%s\"}", rest);
        return true;
    }

    if (strcmp(req->method, "DELETE") == 0) {
        if (!*rest) {
            respond_error(res, 400, "expected /admin/settings/<name>");
            return true;
        }
        if (!edb_setting_delete(g_ctx.config, rest + 1)) {
            respond_error(res, 404, "unknown setting");
            return true;
        }
        res->status = 200;
        res->content_type = "application/json";
        res->body = edb_http_body_printf(&res->body_len, "{\"deleted\":true}");
        return true;
    }

    respond_error(res, 405, "method not allowed");
    return true;
}

bool handle_status(const edb_http_request *req,
                          edb_http_response *res)
{
    (void)req;
    cJSON *o = cJSON_CreateObject();
    cJSON_AddStringToObject(o, "service", "epsilondb");
    cJSON_AddStringToObject(o, "version", sw_version);
    cJSON_AddBoolToObject(o, "clustered", g_cluster != NULL);
    respond_json(res, 200, o);
    return true;
}

/* ------------------------------------------------------------------ */
/* admin console authentication                                        */
