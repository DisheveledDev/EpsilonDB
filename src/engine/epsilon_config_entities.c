/* epsilon_config_entities.c - databases, security groups and users.
 * Part of the config module; see epsilon_config_internal.h.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "epsilon_config_internal.h"
#include "random.h"
#include "sha256.h"
bool edb_database_create(edb_config *cfg, const char *name,
                         int replication_factor)
{
    if (!cfg || !name || !*name || !NAME_OK(name, edb_database_info) ||
        replication_factor < 1) {
        return false;
    }
    edb_database_info existing;
    if (edb_database_get(cfg, name, &existing)) {
        return false;   /* duplicate */
    }
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "database");
    cJSON_AddStringToObject(obj, "name", name);
    cJSON_AddNumberToObject(obj, "replication_factor", replication_factor);
    bool ok = store(cfg, CFG_KEYSPACE_DATABASES, name, obj);
    cJSON_Delete(obj);
    return ok;
}

bool remove_keyspaces_for(edb_config *cfg, const char *database,
                                 const char *partition)
{
    cJSON *all = collect(cfg, CFG_KEYSPACE_KEYSPACES);
    if (!all) {
        return true;
    }
    bool ok = true;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, all) {
        const cJSON *db = cJSON_GetObjectItemCaseSensitive(item, "database");
        const cJSON *part = cJSON_GetObjectItemCaseSensitive(item, "partition");
        if (!cJSON_IsString(db) || !cJSON_IsString(part) ||
            strcmp(db->valuestring, database) != 0 ||
            (partition && strcmp(part->valuestring, partition) != 0)) {
            continue;
        }
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(item, "name");
        if (!cJSON_IsString(name)) {
            ok = false;
            continue;
        }
        char id[640];
        if (snprintf(id, sizeof(id), "%s/%s/%s", database,
                     part->valuestring, name->valuestring) >= (int)sizeof(id) ||
            !remove_record(cfg, CFG_KEYSPACE_KEYSPACES, id)) {
            ok = false;
        }
    }
    cJSON_Delete(all);
    return ok;
}


bool edb_database_delete(edb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    /* also drop partitions belonging to this database */
    size_t n = 0;
    edb_partition_info *parts = edb_partition_list(cfg, name, &n);
    for (size_t i = 0; parts && i < n; i++) {
        edb_partition_delete(cfg, name, parts[i].name);
    }
    bool keyspaces_ok = remove_keyspaces_for(cfg, name, NULL);
    free(parts);
    return keyspaces_ok && remove_record(cfg, CFG_KEYSPACE_DATABASES, name);
}

bool edb_database_get(edb_config *cfg, const char *name,
                      edb_database_info *out)
{
    if (!cfg || !name || !out) {
        return false;
    }
    cJSON *obj = fetch(cfg, CFG_KEYSPACE_DATABASES, name);
    if (!obj) {
        return false;
    }
    copy_name(out->name, sizeof(out->name), obj, "name");
    const cJSON *rf = cJSON_GetObjectItemCaseSensitive(obj,
                                                       "replication_factor");
    out->replication_factor = cJSON_IsNumber(rf) ? rf->valueint : 1;
    cJSON_Delete(obj);
    return out->name[0] != '\0';
}

edb_database_info *edb_database_list(edb_config *cfg, size_t *count_out)
{
    *count_out = 0;
    if (!cfg) {
        return NULL;
    }
    cJSON *all = collect(cfg, CFG_KEYSPACE_DATABASES);
    if (!all) {
        return NULL;
    }
    size_t n = (size_t)cJSON_GetArraySize(all);
    edb_database_info *out = calloc(n + 1, sizeof(*out));
    if (!out) {
        cJSON_Delete(all);
        return NULL;
    }
    size_t i = 0;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, all) {
        copy_name(out[i].name, sizeof(out[i].name), item, "name");
        const cJSON *rf = cJSON_GetObjectItemCaseSensitive(item,
                                                       "replication_factor");
        out[i].replication_factor = cJSON_IsNumber(rf) ? rf->valueint : 1;
        i++;
    }
    cJSON_Delete(all);
    *count_out = n;
    return out;
}

/* --- security groups --------------------------------------------------- */

uint64_t next_free_bit(edb_config *cfg)
{
    size_t n = 0;
    edb_group_info *groups = edb_group_list(cfg, &n);
    uint64_t used = 0;
    for (size_t i = 0; groups && i < n; i++) {
        if (groups[i].bit_position >= 1 &&
            groups[i].bit_position <= EDB_MAX_GROUPS) {
            used |= (1ULL << (groups[i].bit_position - 1));
        }
    }
    free(groups);
    for (int bit = 1; bit <= EDB_MAX_GROUPS; bit++) {
        if (!(used & (1ULL << (bit - 1)))) {
            return (uint64_t)bit;
        }
    }
    return 0;   /* exhausted */
}

bool edb_group_create(edb_config *cfg, const char *name)
{
    if (!cfg || !name || !*name || !NAME_OK(name, edb_group_info)) {
        return false;
    }
    pthread_mutex_lock(&cfg->group_lock);
    edb_group_info existing;
    if (edb_group_get(cfg, name, &existing)) {
        pthread_mutex_unlock(&cfg->group_lock);
        return false;
    }
    uint64_t bit = next_free_bit(cfg);
    if (bit == 0) {
        pthread_mutex_unlock(&cfg->group_lock);
        return false;
    }
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        pthread_mutex_unlock(&cfg->group_lock);
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "group");
    cJSON_AddStringToObject(obj, "name", name);
    set_json_u64(obj, "bit_position", bit);
    bool ok = store(cfg, CFG_KEYSPACE_GROUPS, name, obj);
    cJSON_Delete(obj);
    pthread_mutex_unlock(&cfg->group_lock);
    return ok;
}

bool edb_group_delete(edb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    pthread_mutex_lock(&cfg->group_lock);
    bool ok = remove_record(cfg, CFG_KEYSPACE_GROUPS, name);
    pthread_mutex_unlock(&cfg->group_lock);
    return ok;
}

bool edb_group_get(edb_config *cfg, const char *name, edb_group_info *out)
{
    if (!cfg || !name || !out) {
        return false;
    }
    cJSON *obj = fetch(cfg, CFG_KEYSPACE_GROUPS, name);
    if (!obj) {
        return false;
    }
    copy_name(out->name, sizeof(out->name), obj, "name");
    out->bit_position = json_u64(obj, "bit_position");
    cJSON_Delete(obj);
    return out->name[0] != '\0' &&
           out->bit_position >= 1 && out->bit_position <= EDB_MAX_GROUPS;
}

edb_group_info *edb_group_list(edb_config *cfg, size_t *count_out)
{
    *count_out = 0;
    if (!cfg) {
        return NULL;
    }
    cJSON *all = collect(cfg, CFG_KEYSPACE_GROUPS);
    if (!all) {
        return NULL;
    }
    size_t n = (size_t)cJSON_GetArraySize(all);
    edb_group_info *out = calloc(n + 1, sizeof(*out));
    if (!out) {
        cJSON_Delete(all);
        return NULL;
    }
    size_t i = 0;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, all) {
        copy_name(out[i].name, sizeof(out[i].name), item, "name");
        out[i].bit_position = json_u64(item, "bit_position");
        i++;
    }
    cJSON_Delete(all);
    *count_out = n;
    return out;
}

/* --- users ------------------------------------------------------------- */

bool edb_user_create(edb_config *cfg, const char *name, uint64_t groups)
{
    if (!cfg || !name || !*name || !NAME_OK(name, edb_user_info)) {
        return false;
    }
    edb_user_info existing;
    if (edb_user_get(cfg, name, &existing)) {
        return false;
    }
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "user");
    cJSON_AddStringToObject(obj, "name", name);
    set_json_u64(obj, "groups", groups);
    bool ok = store(cfg, CFG_KEYSPACE_USERS, name, obj);
    cJSON_Delete(obj);
    return ok;
}

bool edb_user_delete(edb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    return remove_record(cfg, CFG_KEYSPACE_USERS, name);
}

static bool user_update(edb_config *cfg, const char *name, uint64_t groups)
{
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "user");
    cJSON_AddStringToObject(obj, "name", name);
    set_json_u64(obj, "groups", groups);
    bool ok = store(cfg, CFG_KEYSPACE_USERS, name, obj);
    cJSON_Delete(obj);
    return ok;
}

bool edb_user_set_groups(edb_config *cfg, const char *name, uint64_t groups)
{
    if (!cfg || !name) {
        return false;
    }
    edb_user_info existing;
    if (!edb_user_get(cfg, name, &existing)) {
        return false;
    }
    return user_update(cfg, name, groups);
}

bool edb_user_get(edb_config *cfg, const char *name, edb_user_info *out)
{
    if (!cfg || !name || !out) {
        return false;
    }
    cJSON *obj = fetch(cfg, CFG_KEYSPACE_USERS, name);
    if (!obj) {
        return false;
    }
    copy_name(out->name, sizeof(out->name), obj, "name");
    out->groups = json_u64(obj, "groups");
    cJSON_Delete(obj);
    return out->name[0] != '\0';
}

edb_user_info *edb_user_list(edb_config *cfg, size_t *count_out)
{
    *count_out = 0;
    if (!cfg) {
        return NULL;
    }
    cJSON *all = collect(cfg, CFG_KEYSPACE_USERS);
    if (!all) {
        return NULL;
    }
    size_t n = (size_t)cJSON_GetArraySize(all);
    edb_user_info *out = calloc(n + 1, sizeof(*out));
    if (!out) {
        cJSON_Delete(all);
        return NULL;
    }
    size_t i = 0;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, all) {
        copy_name(out[i].name, sizeof(out[i].name), item, "name");
        out[i].groups = json_u64(item, "groups");
        i++;
    }
    cJSON_Delete(all);
    *count_out = n;
    return out;
}

/* --- passwords ---------------------------------------------------------- */

#define EDB_PASSWORD_ITERATIONS 100000

int hex_value(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

bool hex_decode(const char *hex, uint8_t *out, size_t out_len)
{
    for (size_t i = 0; i < out_len; i++) {
        int hi = hex_value(hex[i * 2]);
        int lo = hex_value(hex[i * 2 + 1]);
        if (hi < 0 || lo < 0) {
            return false;
        }
        out[i] = (uint8_t)((hi << 4) | lo);
    }
    return true;
}

/* Iterated salted SHA-256 (a simple, dependency-free key stretch). */
void hash_password(const char *salt_hex, const char *password,
                          char out[65])
{
    uint8_t salt[16];
    if (!hex_decode(salt_hex, salt, sizeof(salt))) {
        memset(salt, 0, sizeof(salt));
    }
    size_t plen = strlen(password);
    if (plen > 256) {
        plen = 256;
    }
    uint8_t input[16 + 256];
    memcpy(input, salt, sizeof(salt));
    memcpy(input + sizeof(salt), password, plen);

    uint8_t digest[32];
    edb_sha256(input, sizeof(salt) + plen, digest);
    for (int i = 1; i < EDB_PASSWORD_ITERATIONS; i++) {
        edb_sha256(digest, sizeof(digest), digest);
    }
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 0xf];
    }
    out[64] = '\0';
}

bool edb_user_set_password(edb_config *cfg, const char *name,
                           const char *password)
{
    if (!cfg || !name || !password || !*password) {
        return false;
    }
    edb_user_info existing;
    if (!edb_user_get(cfg, name, &existing)) {
        return false;
    }
    char salt_hex[33];
    edb_random_hex(salt_hex, 32);
    char hash[65];
    hash_password(salt_hex, password, hash);

    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "user");
    cJSON_AddStringToObject(obj, "name", name);
    set_json_u64(obj, "groups", existing.groups);
    cJSON_AddStringToObject(obj, "salt", salt_hex);
    cJSON_AddStringToObject(obj, "password_hash", hash);
    bool ok = store(cfg, CFG_KEYSPACE_USERS, name, obj);
    cJSON_Delete(obj);
    return ok;
}

bool edb_user_verify_password(edb_config *cfg, const char *name,
                              const char *password)
{
    if (!cfg || !name || !password) {
        return false;
    }
    cJSON *obj = fetch(cfg, CFG_KEYSPACE_USERS, name);
    if (!obj) {
        return false;
    }
    const cJSON *salt = cJSON_GetObjectItemCaseSensitive(obj, "salt");
    const cJSON *hash = cJSON_GetObjectItemCaseSensitive(obj, "password_hash");
    bool ok = false;
    if (cJSON_IsString(salt) && cJSON_IsString(hash) &&
        strlen(salt->valuestring) == 32 &&
        strlen(hash->valuestring) == 64) {
        char expected[65];
        hash_password(salt->valuestring, password, expected);
        ok = strcmp(expected, hash->valuestring) == 0;
    }
    cJSON_Delete(obj);
    return ok;
}

bool edb_user_has_password(edb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    cJSON *obj = fetch(cfg, CFG_KEYSPACE_USERS, name);
    if (!obj) {
        return false;
    }
    const cJSON *hash = cJSON_GetObjectItemCaseSensitive(obj, "password_hash");
    bool has = cJSON_IsString(hash) && hash->valuestring &&
               strlen(hash->valuestring) == 64;
    cJSON_Delete(obj);
    return has;
}

bool edb_admin_exists(edb_config *cfg)
{
    size_t count = 0;
    edb_user_info *users = edb_user_list(cfg, &count);
    bool exists = false;
    for (size_t i = 0; users && i < count; i++) {
        if (users[i].groups & 1ULL) {
            exists = true;
            break;
        }
    }
    free(users);
    return exists;
}

/* --- partitions --------------------------------------------------------- */