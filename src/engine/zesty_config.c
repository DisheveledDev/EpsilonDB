#include "zesty_config.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "random.h"
#include "sha256.h"

/* Every config record is a JSON document stored in a dedicated __system__
 * keyspace, so list operations need no secondary filter records. */

#define CFG_KEYSPACE_DATABASES   "config_databases"
#define CFG_KEYSPACE_GROUPS      "config_groups"
#define CFG_KEYSPACE_USERS       "config_users"
#define CFG_KEYSPACE_PARTITIONS  "config_partitions"
#define CFG_KEYSPACE_KEYSPACES   "config_keyspaces"
#define CFG_KEYSPACE_SETTINGS    "config_settings"

struct zdb_config {
    zdb_engine *engine;
    bool owns_engine;
    pthread_mutex_t replicate_lock;
    pthread_mutex_t group_lock;
    zdb_config_replicate_fn replicate;
    void *replicate_ctx;
};

#define NAME_OK(name, type) \
    ((name) && *(name) && strlen(name) < sizeof(((type *)0)->name))

static char *record_json(const cJSON *obj)
{
    char *printed = cJSON_PrintUnformatted(obj);
    return printed;
}

static cJSON *fetch(zdb_config *cfg, const char *keyspace, const char *id)
{
    return zdb_get(cfg->engine, ZDB_SYSTEM_DB, keyspace, id);
}

static bool internal_cluster_setting(const char *keyspace, const char *id)
{
    return strcmp(keyspace, CFG_KEYSPACE_SETTINGS) == 0 &&
           (strncmp(id, "cluster.", 8) == 0 ||
            strncmp(id, "rebalance.", 10) == 0);
}

static bool store(zdb_config *cfg, const char *keyspace, const char *id,
                  cJSON *obj)
{
    char *json = record_json(obj);
    if (!json) {
        return false;
    }
    pthread_mutex_lock(&cfg->replicate_lock);
    bool ok;
    if (cfg->replicate && !internal_cluster_setting(keyspace, id)) {
        ok = cfg->replicate(cfg->replicate_ctx, keyspace, id, json);
        pthread_mutex_unlock(&cfg->replicate_lock);
    } else {
        pthread_mutex_unlock(&cfg->replicate_lock);
        ok = zdb_put(cfg->engine, ZDB_SYSTEM_DB, keyspace, id, json, -1);
    }
    free(json);
    return ok;
}

static bool remove_record(zdb_config *cfg, const char *keyspace,
                          const char *id)
{
    pthread_mutex_lock(&cfg->replicate_lock);
    if (cfg->replicate && !internal_cluster_setting(keyspace, id)) {
        bool ok = cfg->replicate(cfg->replicate_ctx, keyspace, id, NULL);
        pthread_mutex_unlock(&cfg->replicate_lock);
        return ok;
    }
    pthread_mutex_unlock(&cfg->replicate_lock);
    return zdb_delete(cfg->engine, ZDB_SYSTEM_DB, keyspace, id);
}

static cJSON *collect(zdb_config *cfg, const char *keyspace)
{
    return zdb_all(cfg->engine, ZDB_SYSTEM_DB, keyspace, NULL);
}

/* Extract helpers with bounds-checked copies. */
static void copy_name(char *dst, size_t cap, const cJSON *obj,
                      const char *field)
{
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, field);
    dst[0] = '\0';
    if (cJSON_IsString(item) && item->valuestring) {
        snprintf(dst, cap, "%s", item->valuestring);
    }
}

static uint64_t json_u64(const cJSON *obj, const char *field)
{
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, field);
    if (cJSON_IsString(item) && item->valuestring) {
        char *end = NULL;
        errno = 0;
        unsigned long long value = strtoull(item->valuestring, &end, 10);
        if (errno == 0 && end && *end == '\0') {
            return (uint64_t)value;
        }
    }
    if (cJSON_IsNumber(item) && item->valuedouble >= 0) {
        return (uint64_t)item->valuedouble;
    }
    return 0;
}

static void set_json_u64(cJSON *obj, const char *field, uint64_t v)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "%llu", (unsigned long long)v);
    cJSON_AddStringToObject(obj, field, buf);
}

zdb_config *zdb_config_open(zdb_engine *engine)
{
    if (!engine) {
        return NULL;
    }
    zdb_config *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        return NULL;
    }
    cfg->engine = engine;
    cfg->owns_engine = false;
    pthread_mutex_init(&cfg->replicate_lock, NULL);
    pthread_mutex_init(&cfg->group_lock, NULL);
    return cfg;
}

zdb_engine *zdb_config_engine(zdb_config *cfg)
{
    return cfg ? cfg->engine : NULL;
}

void zdb_config_set_replicator(zdb_config *cfg,
                               zdb_config_replicate_fn replicate, void *ctx)
{
    if (!cfg) {
        return;
    }
    pthread_mutex_lock(&cfg->replicate_lock);
    cfg->replicate = replicate;
    cfg->replicate_ctx = ctx;
    pthread_mutex_unlock(&cfg->replicate_lock);
}

void zdb_config_close(zdb_config *cfg)
{
    if (!cfg) {
        return;
    }
    pthread_mutex_destroy(&cfg->group_lock);
    pthread_mutex_destroy(&cfg->replicate_lock);
    free(cfg);
}

/* --- databases -------------------------------------------------------- */

bool zdb_database_create(zdb_config *cfg, const char *name,
                         int replication_factor)
{
    if (!cfg || !name || !*name || !NAME_OK(name, zdb_database_info) ||
        replication_factor < 1) {
        return false;
    }
    zdb_database_info existing;
    if (zdb_database_get(cfg, name, &existing)) {
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

bool zdb_database_delete(zdb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    /* also drop partitions belonging to this database */
    size_t n = 0;
    zdb_partition_info *parts = zdb_partition_list(cfg, name, &n);
    for (size_t i = 0; parts && i < n; i++) {
        zdb_partition_delete(cfg, name, parts[i].name);
    }
    free(parts);
    return remove_record(cfg, CFG_KEYSPACE_DATABASES, name);
}

bool zdb_database_get(zdb_config *cfg, const char *name,
                      zdb_database_info *out)
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

zdb_database_info *zdb_database_list(zdb_config *cfg, size_t *count_out)
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
    zdb_database_info *out = calloc(n + 1, sizeof(*out));
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

static uint64_t next_free_bit(zdb_config *cfg)
{
    size_t n = 0;
    zdb_group_info *groups = zdb_group_list(cfg, &n);
    uint64_t used = 0;
    for (size_t i = 0; groups && i < n; i++) {
        used |= (1ULL << (groups[i].bit_position - 1));
    }
    free(groups);
    for (int bit = 1; bit <= ZDB_MAX_GROUPS; bit++) {
        if (!(used & (1ULL << (bit - 1)))) {
            return (uint64_t)bit;
        }
    }
    return 0;   /* exhausted */
}

bool zdb_group_create(zdb_config *cfg, const char *name)
{
    if (!cfg || !name || !*name || !NAME_OK(name, zdb_group_info)) {
        return false;
    }
    pthread_mutex_lock(&cfg->group_lock);
    zdb_group_info existing;
    if (zdb_group_get(cfg, name, &existing)) {
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

bool zdb_group_delete(zdb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    pthread_mutex_lock(&cfg->group_lock);
    bool ok = remove_record(cfg, CFG_KEYSPACE_GROUPS, name);
    pthread_mutex_unlock(&cfg->group_lock);
    return ok;
}

bool zdb_group_get(zdb_config *cfg, const char *name, zdb_group_info *out)
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
           out->bit_position >= 1 && out->bit_position <= ZDB_MAX_GROUPS;
}

zdb_group_info *zdb_group_list(zdb_config *cfg, size_t *count_out)
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
    zdb_group_info *out = calloc(n + 1, sizeof(*out));
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

bool zdb_user_create(zdb_config *cfg, const char *name, uint64_t groups)
{
    if (!cfg || !name || !*name || !NAME_OK(name, zdb_user_info)) {
        return false;
    }
    zdb_user_info existing;
    if (zdb_user_get(cfg, name, &existing)) {
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

bool zdb_user_delete(zdb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    return remove_record(cfg, CFG_KEYSPACE_USERS, name);
}

static bool user_update(zdb_config *cfg, const char *name, uint64_t groups)
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

bool zdb_user_set_groups(zdb_config *cfg, const char *name, uint64_t groups)
{
    if (!cfg || !name) {
        return false;
    }
    zdb_user_info existing;
    if (!zdb_user_get(cfg, name, &existing)) {
        return false;
    }
    return user_update(cfg, name, groups);
}

bool zdb_user_get(zdb_config *cfg, const char *name, zdb_user_info *out)
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

zdb_user_info *zdb_user_list(zdb_config *cfg, size_t *count_out)
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
    zdb_user_info *out = calloc(n + 1, sizeof(*out));
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

#define ZDB_PASSWORD_ITERATIONS 100000

static int hex_value(char c)
{
    if (c >= '0' && c <= '9') return c - '0';
    if (c >= 'a' && c <= 'f') return c - 'a' + 10;
    if (c >= 'A' && c <= 'F') return c - 'A' + 10;
    return -1;
}

static bool hex_decode(const char *hex, uint8_t *out, size_t out_len)
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
static void hash_password(const char *salt_hex, const char *password,
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
    zdb_sha256(input, sizeof(salt) + plen, digest);
    for (int i = 1; i < ZDB_PASSWORD_ITERATIONS; i++) {
        zdb_sha256(digest, sizeof(digest), digest);
    }
    static const char hex[] = "0123456789abcdef";
    for (int i = 0; i < 32; i++) {
        out[i * 2] = hex[digest[i] >> 4];
        out[i * 2 + 1] = hex[digest[i] & 0xf];
    }
    out[64] = '\0';
}

bool zdb_user_set_password(zdb_config *cfg, const char *name,
                           const char *password)
{
    if (!cfg || !name || !password || !*password) {
        return false;
    }
    zdb_user_info existing;
    if (!zdb_user_get(cfg, name, &existing)) {
        return false;
    }
    char salt_hex[33];
    zdb_random_hex(salt_hex, 32);
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

bool zdb_user_verify_password(zdb_config *cfg, const char *name,
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

bool zdb_user_has_password(zdb_config *cfg, const char *name)
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

bool zdb_admin_exists(zdb_config *cfg)
{
    size_t count = 0;
    zdb_user_info *users = zdb_user_list(cfg, &count);
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

bool zdb_partition_create(zdb_config *cfg, const char *database,
                          const char *name, uint64_t create_mask,
                          uint64_t update_mask, uint64_t read_mask,
                          uint64_t delete_mask)
{
    if (!cfg || !database || !name || !*database || !*name ||
        !NAME_OK(database, zdb_partition_info) ||
        !NAME_OK(name, zdb_partition_info)) {
        return false;
    }
    zdb_partition_info existing;
    if (zdb_partition_get(cfg, database, name, &existing)) {
        return false;
    }
    char id[384];
    snprintf(id, sizeof(id), "%s/%s", database, name);
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "partition");
    cJSON_AddStringToObject(obj, "database", database);
    cJSON_AddStringToObject(obj, "name", name);
    set_json_u64(obj, "create_mask", create_mask);
    set_json_u64(obj, "update_mask", update_mask);
    set_json_u64(obj, "read_mask", read_mask);
    set_json_u64(obj, "delete_mask", delete_mask);
    bool ok = store(cfg, CFG_KEYSPACE_PARTITIONS, id, obj);
    cJSON_Delete(obj);
    return ok;
}

bool zdb_partition_delete(zdb_config *cfg, const char *database,
                          const char *name)
{
    if (!cfg || !database || !name) {
        return false;
    }
    char id[384];
    snprintf(id, sizeof(id), "%s/%s", database, name);
    return remove_record(cfg, CFG_KEYSPACE_PARTITIONS, id);
}

bool zdb_partition_set_masks(zdb_config *cfg, const char *database,
                             const char *name, uint64_t create_mask,
                             uint64_t update_mask, uint64_t read_mask,
                             uint64_t delete_mask)
{
    if (!cfg || !database || !name) {
        return false;
    }
    zdb_partition_info existing;
    if (!zdb_partition_get(cfg, database, name, &existing)) {
        return false;
    }
    char id[384];
    snprintf(id, sizeof(id), "%s/%s", database, name);
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "partition");
    cJSON_AddStringToObject(obj, "database", database);
    cJSON_AddStringToObject(obj, "name", name);
    set_json_u64(obj, "create_mask", create_mask);
    set_json_u64(obj, "update_mask", update_mask);
    set_json_u64(obj, "read_mask", read_mask);
    set_json_u64(obj, "delete_mask", delete_mask);
    bool ok = store(cfg, CFG_KEYSPACE_PARTITIONS, id, obj);
    cJSON_Delete(obj);
    return ok;
}

bool zdb_partition_get(zdb_config *cfg, const char *database,
                       const char *name, zdb_partition_info *out)
{
    if (!cfg || !database || !name || !out) {
        return false;
    }
    char id[384];
    snprintf(id, sizeof(id), "%s/%s", database, name);
    cJSON *obj = fetch(cfg, CFG_KEYSPACE_PARTITIONS, id);
    if (!obj) {
        return false;
    }
    copy_name(out->database, sizeof(out->database), obj, "database");
    copy_name(out->name, sizeof(out->name), obj, "name");
    out->create_mask = json_u64(obj, "create_mask");
    out->update_mask = json_u64(obj, "update_mask");
    out->read_mask = json_u64(obj, "read_mask");
    out->delete_mask = json_u64(obj, "delete_mask");
    cJSON_Delete(obj);
    return out->name[0] != '\0';
}

/* Returns true if the partition already existed or was created now.
 * Auto-created partitions carry allow-all masks (0); operators can
 * tighten them afterwards via set_masks. This is what makes writes to
 * unseen partitions transparent: the first put registers the partition
 * and its keyspace usage in the system database. */
bool zdb_partition_ensure(zdb_config *cfg, const char *database,
                          const char *partition, const char *keyspace,
                          bool *created_out)
{
    if (created_out) {
        *created_out = false;
    }
    if (!cfg || !database || !partition || !keyspace ||
        !*database || !*partition || !*keyspace) {
        return false;
    }
    if (!NAME_OK(database, zdb_partition_info) ||
        !NAME_OK(partition, zdb_keyspace_info)) {
        return false;
    }

    zdb_partition_info existing;
    bool exists = zdb_partition_get(cfg, database, partition, &existing);
    if (!exists) {
        /* registry of used partitions per database */
        if (!zdb_partition_create(cfg, database, partition,
                                  ZDB_MASK_ALLOW_ALL, ZDB_MASK_ALLOW_ALL,
                                  ZDB_MASK_ALLOW_ALL,
                                  ZDB_MASK_ALLOW_ALL)) {
            return false;
        }
        if (created_out) {
            *created_out = true;
        }
    }

    /* registry of used keyspaces per database: one record per distinct
     * db/partition/keyspace triple; re-storing is idempotent */
    char ks_id[640];
    snprintf(ks_id, sizeof(ks_id), "%s/%s/%s", database, partition,
             keyspace);
    cJSON *ks = fetch(cfg, CFG_KEYSPACE_KEYSPACES, ks_id);
    if (!ks) {
        cJSON *obj = cJSON_CreateObject();
        if (!obj) {
            return false;
        }
        cJSON_AddStringToObject(obj, "type", "keyspace");
        cJSON_AddStringToObject(obj, "database", database);
        cJSON_AddStringToObject(obj, "partition", partition);
        cJSON_AddStringToObject(obj, "name", keyspace);
        if (!store(cfg, CFG_KEYSPACE_KEYSPACES, ks_id, obj)) {
            cJSON_Delete(obj);
            return false;
        }
        cJSON_Delete(obj);
    } else {
        cJSON_Delete(ks);
    }
    return true;
}

zdb_keyspace_info *zdb_keyspace_list(zdb_config *cfg, size_t *count_out)
{
    *count_out = 0;
    if (!cfg) {
        return NULL;
    }
    cJSON *all = collect(cfg, CFG_KEYSPACE_KEYSPACES);
    if (!all) {
        return NULL;
    }
    size_t total = (size_t)cJSON_GetArraySize(all);
    zdb_keyspace_info *out = calloc(total + 1, sizeof(*out));
    if (!out) {
        cJSON_Delete(all);
        return NULL;
    }
    size_t n = 0;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, all) {
        copy_name(out[n].database, sizeof(out[n].database), item,
                  "database");
        copy_name(out[n].partition, sizeof(out[n].partition), item,
                  "partition");
        copy_name(out[n].name, sizeof(out[n].name), item, "name");
        if (out[n].name[0] != '\0') {
            n++;
        }
    }
    cJSON_Delete(all);
    *count_out = n;
    return out;
}

zdb_partition_info *zdb_partition_list(zdb_config *cfg, const char *database,
                                       size_t *count_out)
{
    *count_out = 0;
    if (!cfg || !database) {
        return NULL;
    }
    cJSON *all = collect(cfg, CFG_KEYSPACE_PARTITIONS);
    if (!all) {
        return NULL;
    }
    size_t total = (size_t)cJSON_GetArraySize(all);
    zdb_partition_info *out = calloc(total + 1, sizeof(*out));
    if (!out) {
        cJSON_Delete(all);
        return NULL;
    }
    size_t n = 0;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, all) {
        char db[128];
        copy_name(db, sizeof(db), item, "database");
        if (strcmp(db, database) != 0) {
            continue;
        }
        copy_name(out[n].database, sizeof(out[n].database), item, "database");
        copy_name(out[n].name, sizeof(out[n].name), item, "name");
        out[n].create_mask = json_u64(item, "create_mask");
        out[n].update_mask = json_u64(item, "update_mask");
        out[n].read_mask = json_u64(item, "read_mask");
        out[n].delete_mask = json_u64(item, "delete_mask");
        n++;
    }
    cJSON_Delete(all);
    *count_out = n;
    return out;
}

/* --- authorization ------------------------------------------------------ */

bool zdb_check_perm(uint64_t mask, uint64_t user_groups,
                    zdb_permission perm)
{
    (void)perm;   /* masks are per-permission; caller selects the mask */
    if (mask == ZDB_MASK_ALLOW_ALL) {
        return true;
    }
    return (mask & user_groups) != 0;
}

/* --- server/cluster settings -------------------------------------------- */

bool zdb_setting_set(zdb_config *cfg, const char *name,
                     const char *json_value)
{
    if (!cfg || !name || !*name || !json_value) {
        return false;
    }
    cJSON *value = cJSON_Parse(json_value);
    if (!value) {
        return false;   /* not valid JSON */
    }
    char *printed = cJSON_PrintUnformatted(value);
    cJSON_Delete(value);
    if (!printed) {
        return false;
    }

    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        free(printed);
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "setting");
    cJSON_AddStringToObject(obj, "name", name);
    cJSON_AddRawToObject(obj, "value", printed);
    bool ok = store(cfg, CFG_KEYSPACE_SETTINGS, name, obj);
    cJSON_Delete(obj);   /* also frees the raw string copy */
    free(printed);
    return ok;
}

char *zdb_setting_get(zdb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return NULL;
    }
    cJSON *obj = fetch(cfg, CFG_KEYSPACE_SETTINGS, name);
    if (!obj) {
        return NULL;
    }
    const cJSON *value = cJSON_GetObjectItemCaseSensitive(obj, "value");
    char *out = NULL;
    if (cJSON_IsRaw(value) && value->valuestring) {
        out = strdup(value->valuestring);
    } else {
        out = cJSON_PrintUnformatted(value);
    }
    cJSON_Delete(obj);
    return out;
}

bool zdb_setting_delete(zdb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    return remove_record(cfg, CFG_KEYSPACE_SETTINGS, name);
}

char **zdb_setting_list(zdb_config *cfg, size_t *count_out)
{
    *count_out = 0;
    if (!cfg) {
        return NULL;
    }
    cJSON *all = collect(cfg, CFG_KEYSPACE_SETTINGS);
    if (!all) {
        return NULL;
    }
    size_t n = (size_t)cJSON_GetArraySize(all);
    char **names = calloc(n + 1, sizeof(char *));
    if (!names) {
        cJSON_Delete(all);
        return NULL;
    }
    size_t i = 0;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, all) {
        const cJSON *name = cJSON_GetObjectItemCaseSensitive(item, "name");
        if (cJSON_IsString(name) && name->valuestring && i < n) {
            names[i++] = strdup(name->valuestring);
        }
    }
    cJSON_Delete(all);
    names[i] = NULL;
    *count_out = i;
    return names;
}

bool zdb_config_is_system_key(zdb_config *cfg, const char key[33])
{
    if (!cfg || !key || strlen(key) != 32) {
        return false;
    }
    static const char *const keyspaces[] = {
        CFG_KEYSPACE_DATABASES, CFG_KEYSPACE_GROUPS,   CFG_KEYSPACE_USERS,
        CFG_KEYSPACE_PARTITIONS, CFG_KEYSPACE_KEYSPACES,
        CFG_KEYSPACE_SETTINGS,
    };
    for (size_t i = 0;
         i < sizeof(keyspaces) / sizeof(keyspaces[0]); i++) {
        char sys_key[33];
        char path[1024];
        if (zdb_shard_path(cfg->engine, ZDB_SYSTEM_DB, keyspaces[i], path,
                           sizeof(path), sys_key) &&
            strcmp(sys_key, key) == 0) {
            return true;
        }
    }
    return false;
}

size_t zdb_config_system_keyspaces(const char **out, size_t cap)
{
    static const char *const keyspaces[] = {
        CFG_KEYSPACE_DATABASES, CFG_KEYSPACE_GROUPS,   CFG_KEYSPACE_USERS,
        CFG_KEYSPACE_PARTITIONS, CFG_KEYSPACE_KEYSPACES,
        CFG_KEYSPACE_SETTINGS,
    };
    size_t n = sizeof(keyspaces) / sizeof(keyspaces[0]);
    for (size_t i = 0; out && i < n && i < cap; i++) {
        out[i] = keyspaces[i];
    }
    return n;
}
