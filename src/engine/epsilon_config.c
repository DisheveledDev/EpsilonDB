#include "epsilon_config_internal.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "random.h"
#include "sha256.h"

#define NAME_OK(name, type) \
    ((name) && *(name) && strlen(name) < sizeof(((type *)0)->name))

char *record_json(const cJSON *obj)
{
    char *printed = cJSON_PrintUnformatted(obj);
    return printed;
}

cJSON *fetch(edb_config *cfg, const char *keyspace, const char *id)
{
    return edb_get(cfg->engine, EDB_SYSTEM_DB, keyspace, id);
}

bool internal_cluster_setting(const char *keyspace, const char *id)
{
    return strcmp(keyspace, CFG_KEYSPACE_SETTINGS) == 0 &&
           (strncmp(id, "cluster.", 8) == 0 ||
            strncmp(id, "rebalance.", 10) == 0);
}

bool store(edb_config *cfg, const char *keyspace, const char *id,
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
        ok = edb_put(cfg->engine, EDB_SYSTEM_DB, keyspace, id, json, -1);
    }
    free(json);
    return ok;
}

bool remove_record(edb_config *cfg, const char *keyspace,
                          const char *id)
{
    pthread_mutex_lock(&cfg->replicate_lock);
    if (cfg->replicate && !internal_cluster_setting(keyspace, id)) {
        bool ok = cfg->replicate(cfg->replicate_ctx, keyspace, id, NULL);
        pthread_mutex_unlock(&cfg->replicate_lock);
        return ok;
    }
    pthread_mutex_unlock(&cfg->replicate_lock);
    return edb_delete(cfg->engine, EDB_SYSTEM_DB, keyspace, id);
}

cJSON *collect(edb_config *cfg, const char *keyspace)
{
    return edb_all(cfg->engine, EDB_SYSTEM_DB, keyspace, NULL);
}

/* Extract helpers with bounds-checked copies. */
void copy_name(char *dst, size_t cap, const cJSON *obj,
                      const char *field)
{
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, field);
    dst[0] = '\0';
    if (cJSON_IsString(item) && item->valuestring) {
        snprintf(dst, cap, "%s", item->valuestring);
    }
}

uint64_t json_u64(const cJSON *obj, const char *field)
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

void set_json_u64(cJSON *obj, const char *field, uint64_t v)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "%llu", (unsigned long long)v);
    cJSON_AddStringToObject(obj, field, buf);
}

long long json_i64(const cJSON *obj, const char *field)
{
    const cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, field);
    if (cJSON_IsString(item) && item->valuestring) {
        char *end = NULL;
        errno = 0;
        long long value = strtoll(item->valuestring, &end, 10);
        if (errno == 0 && end && *end == '\0') {
            return value;
        }
    }
    if (cJSON_IsNumber(item)) {
        return (long long)item->valuedouble;
    }
    return 0;
}

void set_json_i64(cJSON *obj, const char *field, long long v)
{
    char buf[32];
    snprintf(buf, sizeof(buf), "%lld", v);
    cJSON_AddStringToObject(obj, field, buf);
}


edb_config *edb_config_open(edb_engine *engine)
{
    if (!engine) {
        return NULL;
    }
    edb_config *cfg = calloc(1, sizeof(*cfg));
    if (!cfg) {
        return NULL;
    }
    cfg->engine = engine;
    cfg->owns_engine = false;
    pthread_mutex_init(&cfg->replicate_lock, NULL);
    pthread_mutex_init(&cfg->group_lock, NULL);
    return cfg;
}

edb_engine *edb_config_engine(edb_config *cfg)
{
    return cfg ? cfg->engine : NULL;
}

void edb_config_set_replicator(edb_config *cfg,
                               edb_config_replicate_fn replicate, void *ctx)
{
    if (!cfg) {
        return;
    }
    pthread_mutex_lock(&cfg->replicate_lock);
    cfg->replicate = replicate;
    cfg->replicate_ctx = ctx;
    pthread_mutex_unlock(&cfg->replicate_lock);
}

void edb_config_close(edb_config *cfg)
{
    if (!cfg) {
        return;
    }
    pthread_mutex_destroy(&cfg->group_lock);
    pthread_mutex_destroy(&cfg->replicate_lock);
    free(cfg);
}

/* --- databases -------------------------------------------------------- */



/* --- authorization ------------------------------------------------------ */

bool edb_check_perm(uint64_t mask, uint64_t user_groups,
                    edb_permission perm)
{
    (void)perm;   /* masks are per-permission; caller selects the mask */
    if (mask == EDB_MASK_ALLOW_ALL) {
        return true;
    }
    return (mask & user_groups) != 0;
}

/* --- server/cluster settings -------------------------------------------- */

bool edb_setting_set(edb_config *cfg, const char *name,
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

char *edb_setting_get(edb_config *cfg, const char *name)
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

bool edb_setting_delete(edb_config *cfg, const char *name)
{
    if (!cfg || !name) {
        return false;
    }
    return remove_record(cfg, CFG_KEYSPACE_SETTINGS, name);
}

char **edb_setting_list(edb_config *cfg, size_t *count_out)
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

bool edb_config_is_system_key(edb_config *cfg, const char key[33])
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
        if (edb_shard_path(cfg->engine, EDB_SYSTEM_DB, keyspaces[i], path,
                           sizeof(path), sys_key) &&
            strcmp(sys_key, key) == 0) {
            return true;
        }
    }
    return false;
}

size_t edb_config_system_keyspaces(const char **out, size_t cap)
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