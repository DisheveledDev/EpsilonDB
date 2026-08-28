/* epsilon_api_console.c - console
 * Part of the split epsilon_api module; see epsilon_api_internal.h.
 */

#include "epsilon_api_internal.h"
#include "../engine/random.h"

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>


/* admin console session tokens (shared with the login handlers) */
#define EDB_SESSION_CAPACITY 256
#define EDB_SESSION_TTL      43200   /* seconds: 12 hours */

typedef struct {
    char token[65];
    char username[128];
    uint64_t groups;
    long long expires;
    bool used;
} edb_session;

static edb_session g_sessions[EDB_SESSION_CAPACITY];
static pthread_mutex_t g_session_lock = PTHREAD_MUTEX_INITIALIZER;

static long long session_now(void)
{
    return (long long)time(NULL);
}

bool session_lookup(const char *token, char username[128],
                           uint64_t *groups)
{
    if (!token) {
        return false;
    }
    long long now = session_now();
    pthread_mutex_lock(&g_session_lock);
    for (int i = 0; i < EDB_SESSION_CAPACITY; i++) {
        edb_session *s = &g_sessions[i];
        if (s->used && strcmp(s->token, token) == 0) {
            if (s->expires <= now) {
                s->used = false;
                pthread_mutex_unlock(&g_session_lock);
                return false;
            }
            snprintf(username, 128, "%s", s->username);
            *groups = s->groups;
            pthread_mutex_unlock(&g_session_lock);
            return true;
        }
    }
    pthread_mutex_unlock(&g_session_lock);
    return false;
}

void session_create(const char *username, uint64_t groups,
                           char token_out[65])
{
    char token[65];
    edb_random_hex(token, 64);

    pthread_mutex_lock(&g_session_lock);
    edb_session *slot = NULL;
    for (int i = 0; i < EDB_SESSION_CAPACITY; i++) {
        if (!g_sessions[i].used || g_sessions[i].expires <= session_now()) {
            slot = &g_sessions[i];
            break;
        }
    }
    if (!slot) {
        slot = &g_sessions[0];
    }
    memset(slot, 0, sizeof(*slot));
    snprintf(slot->token, sizeof(slot->token), "%s", token);
    snprintf(slot->username, sizeof(slot->username), "%s", username);
    slot->groups = groups;
    slot->expires = session_now() + EDB_SESSION_TTL;
    slot->used = true;
    pthread_mutex_unlock(&g_session_lock);

    snprintf(token_out, 65, "%s", token);
}

void session_destroy(const char *token)
{
    if (!token) {
        return;
    }
    pthread_mutex_lock(&g_session_lock);
    for (int i = 0; i < EDB_SESSION_CAPACITY; i++) {
        if (g_sessions[i].used && strcmp(g_sessions[i].token, token) == 0) {
            g_sessions[i].used = false;
            break;
        }
    }
    pthread_mutex_unlock(&g_session_lock);
}


static const char *bearer_token(const edb_http_request *req, char out[256])
{
    const char *hdr = edb_http_header(req, "Authorization");
    if (!hdr) {
        hdr = edb_http_header(req, "authorization");
    }
    if (hdr) {
        const char *value = strncmp(hdr, "Bearer ", 7) == 0 ? hdr + 7 : hdr;
        if (snprintf(out, 256, "%s", value) < 256) {
            return out;
        }
    }
    return NULL;
}

static void respond_groups_json(edb_http_response *res, int status,
                                const char *token, const char *username,
                                uint64_t groups)
{
    char group_bits[32];
    snprintf(group_bits, sizeof(group_bits), "%llu",
             (unsigned long long)groups);
    cJSON *o = cJSON_CreateObject();
    if (token) {
        cJSON_AddStringToObject(o, "token", token);
    }
    cJSON_AddStringToObject(o, "username", username);
    cJSON_AddRawToObject(o, "groups", group_bits);
    respond_json(res, status, o);
}

bool handle_console_state(const edb_http_request *req,
                                 edb_http_response *res)
{
    bool setup_required = g_ctx.config && !edb_admin_exists(g_ctx.config);
    bool authenticated = false;
    char username[128] = "";
    uint64_t groups = 0;
    char token[256];
    const char *presented = bearer_token(req, token);
    if (presented) {
        authenticated = session_lookup(presented, username, &groups);
    }
    size_t peers = 0;
    if (g_cluster) {
        edb_peer_info info[64];
        peers = edb_cluster_peers(g_cluster, info, 64);
    }
    cJSON *o = cJSON_CreateObject();
    cJSON_AddBoolToObject(o, "setup_required", setup_required);
    cJSON_AddBoolToObject(o, "authenticated", authenticated);
    cJSON_AddStringToObject(o, "username", authenticated ? username : "");
    cJSON_AddBoolToObject(o, "clustered", g_cluster != NULL);
    cJSON_AddNumberToObject(o, "peers", (double)peers);
    respond_json(res, 200, o);
    return true;
}

bool handle_admin_login(const edb_http_request *req,
                               edb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (!req->trusted && edb_auth_throttled(req->peer_ip)) {
        respond_error(res, 401, "too many failed attempts");
        return true;
    }
    cJSON *body = NULL;
    if (!body_json(req, &body) || !cJSON_IsObject(body)) {
        respond_error(res, 400, "JSON object required");
        cJSON_Delete(body);
        return true;
    }
    const cJSON *username = cJSON_GetObjectItemCaseSensitive(body, "username");
    const cJSON *password = cJSON_GetObjectItemCaseSensitive(body, "password");
    if (!cJSON_IsString(username) || !cJSON_IsString(password) ||
        !edb_user_verify_password(g_ctx.config, username->valuestring,
                                  password->valuestring)) {
        cJSON_Delete(body);
        if (!req->trusted) {
            edb_auth_throttle_fail(req->peer_ip);
        }
        respond_error(res, 401, "invalid credentials");
        return true;
    }
    edb_user_info user;
    if (!edb_user_get(g_ctx.config, username->valuestring, &user)) {
        cJSON_Delete(body);
        respond_error(res, 401, "invalid credentials");
        return true;
    }
    if (!req->trusted) {
        edb_auth_throttle_reset(req->peer_ip);
    }
    char token[65];
    session_create(user.name, user.groups, token);
    cJSON_Delete(body);
    respond_groups_json(res, 200, token, user.name, user.groups);
    return true;
}

/* ------------------------------------------------------------------ */
/* first-run demo data                                                 */

typedef struct {
    const char *partition;
    const char *keyspace;
    const char *id;
    const char *json;
} demo_record;

static const demo_record DEMO_RECORDS[] = {
    { "people", "employees", "e1001",
      "{\"name\":\"Alice Johnson\",\"title\":\"Software Engineer\","
      "\"department\":\"Engineering\",\"email\":\"alice@acme.example\","
      "\"salary\":95000,\"manager\":\"e1004\",\"active\":true}" },
    { "people", "employees", "e1002",
      "{\"name\":\"Bob Smith\",\"title\":\"Product Manager\","
      "\"department\":\"Product\",\"email\":\"bob@acme.example\","
      "\"salary\":88000,\"manager\":\"e1004\",\"active\":true}" },
    { "people", "employees", "e1003",
      "{\"name\":\"Carol Williams\",\"title\":\"Data Analyst\","
      "\"department\":\"Data\",\"email\":\"carol@acme.example\","
      "\"salary\":72000,\"manager\":\"e1005\",\"active\":true}" },
    { "people", "employees", "e1004",
      "{\"name\":\"Dave Brown\",\"title\":\"Engineering Director\","
      "\"department\":\"Engineering\",\"email\":\"dave@acme.example\","
      "\"salary\":140000,\"manager\":null,\"active\":true}" },
    { "people", "employees", "e1005",
      "{\"name\":\"Eve Davis\",\"title\":\"Head of Data\","
      "\"department\":\"Data\",\"email\":\"eve@acme.example\","
      "\"salary\":130000,\"manager\":null,\"active\":true}" },
    { "departments", "depts", "eng",
      "{\"name\":\"Engineering\",\"head\":\"Dave Brown\","
      "\"budget\":2500000,\"headcount\":42}" },
    { "departments", "depts", "prod",
      "{\"name\":\"Product\",\"head\":\"Bob Smith\","
      "\"budget\":900000,\"headcount\":12}" },
    { "departments", "depts", "data",
      "{\"name\":\"Data\",\"head\":\"Eve Davis\","
      "\"budget\":1200000,\"headcount\":18}" },
    { "departments", "depts", "sales",
      "{\"name\":\"Sales\",\"head\":\"Frank Lee\","
      "\"budget\":1500000,\"headcount\":30}" },
    { "departments", "depts", "hr",
      "{\"name\":\"Human Resources\",\"head\":\"Grace Kim\","
      "\"budget\":400000,\"headcount\":6}" },
    { "projects", "projects", "p100",
      "{\"name\":\"Website Redesign\",\"owner\":\"Product\","
      "\"status\":\"active\",\"budget\":180000,\"progress\":0.65}" },
    { "projects", "projects", "p200",
      "{\"name\":\"Mobile App\",\"owner\":\"Engineering\","
      "\"status\":\"active\",\"budget\":320000,\"progress\":0.4}" },
    { "projects", "projects", "p300",
      "{\"name\":\"Data Warehouse\",\"owner\":\"Data\","
      "\"status\":\"planned\",\"budget\":150000,\"progress\":0.0}" },
    { "locations", "offices", "l1",
      "{\"city\":\"London\",\"country\":\"United Kingdom\","
      "\"address\":\"1 Acme Way\",\"headcount\":58,\"hq\":true}" },
    { "locations", "offices", "l2",
      "{\"city\":\"New York\",\"country\":\"United States\","
      "\"address\":\"500 Park Ave\",\"headcount\":34,\"hq\":false}" },
    { "locations", "offices", "l3",
      "{\"city\":\"Berlin\",\"country\":\"Germany\","
      "\"address\":\"Mitte 12\",\"headcount\":16,\"hq\":false}" },
};

/* Seeds a local example company database so a fresh node has something
 * to explore before it joins a cluster. Data is written straight to the
 * engine (not replicated) so it stays purely local; the join flow wipes
 * it when the node adopts the shared cluster data. */
static void seed_demo_data(void)
{
    edb_database_create(g_ctx.config, "demo", 1);
    size_t n = sizeof(DEMO_RECORDS) / sizeof(DEMO_RECORDS[0]);
    for (size_t i = 0; i < n; i++) {
        const demo_record *r = &DEMO_RECORDS[i];
        edb_put(g_ctx.engine, r->partition, r->keyspace, r->id, r->json, -1);
        edb_partition_ensure(g_ctx.config, "demo", r->partition,
                             r->keyspace, NULL);
    }
}

bool handle_admin_setup(const edb_http_request *req,
                               edb_http_response *res)
{
    if (strcmp(req->method, "POST") != 0) {
        respond_error(res, 405, "method not allowed");
        return true;
    }
    if (g_ctx.config && edb_admin_exists(g_ctx.config)) {
        respond_error(res, 409, "admin already configured");
        return true;
    }
    cJSON *body = NULL;
    if (!body_json(req, &body) || !cJSON_IsObject(body)) {
        respond_error(res, 400, "JSON object required");
        cJSON_Delete(body);
        return true;
    }
    const cJSON *username = cJSON_GetObjectItemCaseSensitive(body, "username");
    const cJSON *password = cJSON_GetObjectItemCaseSensitive(body, "password");
    const cJSON *secret = cJSON_GetObjectItemCaseSensitive(body, "secret");
    /* copy out of the body: the body is freed below and the name must
     * survive until the response is built */
    char name[128];
    const char *username_str =
        cJSON_IsString(username) && username->valuestring &&
                *username->valuestring
            ? username->valuestring
            : "admin";
    snprintf(name, sizeof(name), "%s", username_str);
    if (!cJSON_IsString(password) || !password->valuestring ||
        strlen(password->valuestring) < 4 ||
        strlen(password->valuestring) > 256) {
        cJSON_Delete(body);
        respond_error(res, 400, "password must be 4-256 characters");
        return true;
    }
    /* Optional cluster secret: derive mesh keys, persist them and enable
     * frame encryption. Nodes must present the same secret to join. */
    const char *secret_str =
        cJSON_IsString(secret) && secret->valuestring &&
                *secret->valuestring
            ? secret->valuestring
            : NULL;
    uint8_t enc_key[32], mac_key[32];
    if (secret_str) {
        if (edb_cluster_derive_keys(secret_str, enc_key, mac_key) != 0) {
            cJSON_Delete(body);
            respond_error(res, 400, "invalid cluster secret");
            return true;
        }
        edb_cluster_persist_keys(edb_engine_path(g_ctx.engine), enc_key,
                                 mac_key);
        estp_set_mesh_key(enc_key, mac_key);
    }
    /* First run: create the default SysAdmins group (bit 1) and add the
     * admin user to it, then set the admin password. */
    edb_group_create(g_ctx.config, "SysAdmins");
    edb_group_info sysadmins;
    uint64_t admin_groups = 1ULL;
    if (edb_group_get(g_ctx.config, "SysAdmins", &sysadmins)) {
        admin_groups = 1ULL << (sysadmins.bit_position - 1);
    }
    if (!edb_user_create(g_ctx.config, name, admin_groups)) {
        cJSON_Delete(body);
        respond_error(res, 409, "could not create admin user");
        return true;
    }
    if (!edb_user_set_password(g_ctx.config, name, password->valuestring)) {
        cJSON_Delete(body);
        respond_error(res, 500, "could not store password");
        return true;
    }
    seed_demo_data();
    char token[65];
    session_create(name, admin_groups, token);
    cJSON_Delete(body);
    respond_groups_json(res, 200, token, name, admin_groups);
    return true;
}

bool handle_admin_logout(const edb_http_request *req,
                                edb_http_response *res)
{
    char token[256];
    const char *presented = bearer_token(req, token);
    if (presented) {
        session_destroy(presented);
    }
    respond_json(res, 200, NULL);
    res->body = edb_http_body_printf(&res->body_len, "{\"ok\":true}");
    return true;
}

/* ------------------------------------------------------------------ */
