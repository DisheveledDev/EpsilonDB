#include "zesty_config.h"

#include <stdio.h>
#include <stdlib.h>
#include <string.h>

/* Every config record is a JSON document stored in the __system__ database
 * with filters selecting the record type, so list operations can use the
 * engine's filter index. */

#define CFG_KEYSPACE_DATABASES   "config_databases"
#define CFG_KEYSPACE_GROUPS      "config_groups"
#define CFG_KEYSPACE_USERS       "config_users"
#define CFG_KEYSPACE_PARTITIONS  "config_partitions"
#define CFG_KEYSPACE_KEYSPACES   "config_keyspaces"
#define CFG_KEYSPACE_SETTINGS    "config_settings"

#define FILTER_TYPE_DATABASE     "type=database"
#define FILTER_TYPE_GROUP        "type=group"
#define FILTER_TYPE_USER         "type=user"
#define FILTER_TYPE_PARTITION    "type=partition"
#define FILTER_TYPE_KEYSPACE     "type=keyspace"
#define FILTER_TYPE_SETTING      "type=setting"

struct zdb_config {
    zdb_engine *engine;
    bool owns_engine;
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

/* Maps a config keyspace to its record type filter for store(). */
static const char *filter_for_keyspace(const char *keyspace)
{
    if (strcmp(keyspace, CFG_KEYSPACE_DATABASES) == 0) {
        return FILTER_TYPE_DATABASE;
    }
    if (strcmp(keyspace, CFG_KEYSPACE_GROUPS) == 0) {
        return FILTER_TYPE_GROUP;
    }
    if (strcmp(keyspace, CFG_KEYSPACE_USERS) == 0) {
        return FILTER_TYPE_USER;
    }
    if (strcmp(keyspace, CFG_KEYSPACE_KEYSPACES) == 0) {
        return FILTER_TYPE_KEYSPACE;
    }
    if (strcmp(keyspace, CFG_KEYSPACE_SETTINGS) == 0) {
        return FILTER_TYPE_SETTING;
    }
    return FILTER_TYPE_PARTITION;
}

static bool store(zdb_config *cfg, const char *keyspace, const char *id,
                  cJSON *obj)
{
    char *json = record_json(obj);
    if (!json) {
        return false;
    }
    const char *type_filter = filter_for_keyspace(keyspace);
    bool ok = zdb_put(cfg->engine, ZDB_SYSTEM_DB, keyspace, id, json, -1,
                      &type_filter, 1);
    free(json);
    return ok;
}

static cJSON *collect(zdb_config *cfg, const char *keyspace,
                      const char *type_filter)
{
    return zdb_all(cfg->engine, ZDB_SYSTEM_DB, keyspace, &type_filter, 1);
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
    if (cJSON_IsNumber(item)) {
        /* 64-bit masks exceed double precision only above 2^53, and group
         * bits are <= 63, so exact representation is guaranteed. */
        return (uint64_t)item->valuedouble;
    }
    return 0;
}

static void set_json_u64(cJSON *obj, const char *field, uint64_t v)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "%llu", (unsigned long long)v);
    cJSON_AddNumberToObject(obj, field, strtod(buf, NULL));
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
    return cfg;
}

zdb_engine *zdb_config_engine(zdb_config *cfg)
{
    return cfg ? cfg->engine : NULL;
}

void zdb_config_close(zdb_config *cfg)
{
    if (!cfg) {
        return;
    }
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
    return zdb_delete(cfg->engine, ZDB_SYSTEM_DB, CFG_KEYSPACE_DATABASES,
                      name);
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
    cJSON *all = collect(cfg, CFG_KEYSPACE_DATABASES, FILTER_TYPE_DATABASE);
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
    zdb_group_info existing;
    if (zdb_group_get(cfg, name, &existing)) {
        return false;
    }
    uint64_t bit = next_free_bit(cfg);
    if (bit == 0) {
        return false;
    }
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return false;
    }
    cJSON_AddStringToObject(obj, "type", "group");
    cJSON_AddStringToObject(obj, "name", name);
    set_json_u64(obj, "bit_position", bit);
    bool ok = store(cfg, CFG_KEYSPACE_GROUPS, name, obj);
    cJSON_Delete(obj);
    return ok;
}

bool zdb_group_delete(zdb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    return zdb_delete(cfg->engine, ZDB_SYSTEM_DB, CFG_KEYSPACE_GROUPS, name);
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
    cJSON *all = collect(cfg, CFG_KEYSPACE_GROUPS, FILTER_TYPE_GROUP);
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
    return zdb_delete(cfg->engine, ZDB_SYSTEM_DB, CFG_KEYSPACE_USERS, name);
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
    cJSON *all = collect(cfg, CFG_KEYSPACE_USERS, FILTER_TYPE_USER);
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
    return zdb_delete(cfg->engine, ZDB_SYSTEM_DB, CFG_KEYSPACE_PARTITIONS,
                      id);
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

    bool exists = zdb_partition_get(cfg, database, partition, NULL);
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
    cJSON *all = collect(cfg, CFG_KEYSPACE_KEYSPACES,
                         FILTER_TYPE_KEYSPACE);
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
    cJSON *all = collect(cfg, CFG_KEYSPACE_PARTITIONS,
                         FILTER_TYPE_PARTITION);
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
    return zdb_delete(cfg->engine, ZDB_SYSTEM_DB, CFG_KEYSPACE_SETTINGS,
                      name);
}

char **zdb_setting_list(zdb_config *cfg, size_t *count_out)
{
    *count_out = 0;
    if (!cfg) {
        return NULL;
    }
    cJSON *all = collect(cfg, CFG_KEYSPACE_SETTINGS, FILTER_TYPE_SETTING);
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
