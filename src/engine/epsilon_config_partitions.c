/* epsilon_config_partitions.c - partitions (permission masks + SQLite
 * tuning) and the keyspace registry. Part of the config module; see
 * epsilon_config_internal.h.
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "epsilon_config_internal.h"
bool edb_partition_create(edb_config *cfg, const char *database,
                          const char *name, uint64_t create_mask,
                          uint64_t update_mask, uint64_t read_mask,
                          uint64_t delete_mask)
{
    if (!cfg || !database || !name || !*database || !*name ||
        !NAME_OK(database, edb_partition_info) ||
        !NAME_OK(name, edb_partition_info)) {
        return false;
    }
    edb_partition_info existing;
    if (edb_partition_get(cfg, database, name, &existing)) {
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
    edb_shard_settings defaults;
    edb_shard_settings_default(&defaults);
    set_json_i64(obj, "cache_size", defaults.cache_size);
    set_json_bool(obj, "auto_cache", defaults.auto_cache);
    cJSON_AddStringToObject(obj, "journal_mode", defaults.journal_mode);
    set_json_i64(obj, "vacuum_seconds", defaults.vacuum_seconds);
    set_json_i64(obj, "reindex_seconds", defaults.reindex_seconds);
    bool ok = store(cfg, CFG_KEYSPACE_PARTITIONS, id, obj);
    cJSON_Delete(obj);
    return ok;
}

bool edb_partition_delete(edb_config *cfg, const char *database,
                          const char *name)
{
    if (!cfg || !database || !name) {
        return false;
    }
    char id[384];
    snprintf(id, sizeof(id), "%s/%s", database, name);
    return remove_keyspaces_for(cfg, database, name) &&
           remove_record(cfg, CFG_KEYSPACE_PARTITIONS, id);
}

bool edb_partition_set_masks(edb_config *cfg, const char *database,
                             const char *name, uint64_t create_mask,
                             uint64_t update_mask, uint64_t read_mask,
                             uint64_t delete_mask)
{
    if (!cfg || !database || !name) {
        return false;
    }
    edb_partition_info existing;
    if (!edb_partition_get(cfg, database, name, &existing)) {
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
    set_json_i64(obj, "cache_size", existing.cache_size);
    set_json_bool(obj, "auto_cache", existing.auto_cache);
    cJSON_AddStringToObject(obj, "journal_mode", existing.journal_mode);
    set_json_i64(obj, "vacuum_seconds", existing.vacuum_seconds);
    set_json_i64(obj, "reindex_seconds", existing.reindex_seconds);
    bool ok = store(cfg, CFG_KEYSPACE_PARTITIONS, id, obj);
    cJSON_Delete(obj);
    return ok;
}

bool edb_partition_get(edb_config *cfg, const char *database,
                       const char *name, edb_partition_info *out)
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
    out->cache_size = json_i64(obj, "cache_size");
    out->auto_cache = json_bool(obj, "auto_cache", true);
    copy_name(out->journal_mode, sizeof(out->journal_mode), obj,
              "journal_mode");
    if (out->journal_mode[0] == '\0') {
        snprintf(out->journal_mode, sizeof(out->journal_mode), "TRUNCATE");
    }
    out->vacuum_seconds = json_i64(obj, "vacuum_seconds");
    out->reindex_seconds = json_i64(obj, "reindex_seconds");
    cJSON_Delete(obj);
    return out->name[0] != '\0';
}

bool edb_partition_set_settings(edb_config *cfg, const char *database,
                                const char *name,
                                const edb_shard_settings *settings)
{
    if (!cfg || !database || !name || !settings) {
        return false;
    }
    edb_partition_info existing;
    if (!edb_partition_get(cfg, database, name, &existing)) {
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
    set_json_u64(obj, "create_mask", existing.create_mask);
    set_json_u64(obj, "update_mask", existing.update_mask);
    set_json_u64(obj, "read_mask", existing.read_mask);
    set_json_u64(obj, "delete_mask", existing.delete_mask);
    set_json_i64(obj, "cache_size", settings->cache_size);
    set_json_bool(obj, "auto_cache", settings->auto_cache);
    cJSON_AddStringToObject(obj, "journal_mode", settings->journal_mode);
    set_json_i64(obj, "vacuum_seconds", settings->vacuum_seconds);
    set_json_i64(obj, "reindex_seconds", settings->reindex_seconds);
    bool ok = store(cfg, CFG_KEYSPACE_PARTITIONS, id, obj);
    cJSON_Delete(obj);
    if (ok) {
        edb_engine_reload_partition(cfg->engine, name);
    }
    return ok;
}

/* Returns true if the partition already existed or was created now.
 * Auto-created partitions carry allow-all masks (0); operators can
 * tighten them afterwards via set_masks. This is what makes writes to
 * unseen partitions transparent: the first put registers the partition
 * and its keyspace usage in the system database. */
bool edb_partition_ensure(edb_config *cfg, const char *database,
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
    if (!NAME_OK(database, edb_partition_info) ||
        !NAME_OK(partition, edb_keyspace_info)) {
        return false;
    }

    edb_partition_info existing;
    bool exists = edb_partition_get(cfg, database, partition, &existing);
    if (!exists) {
        /* registry of used partitions per database */
        if (!edb_partition_create(cfg, database, partition,
                                  EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL,
                                  EDB_MASK_ALLOW_ALL,
                                  EDB_MASK_ALLOW_ALL)) {
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

edb_keyspace_info *edb_keyspace_list(edb_config *cfg, size_t *count_out)
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
    edb_keyspace_info *out = calloc(total + 1, sizeof(*out));
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

edb_partition_info *edb_partition_list(edb_config *cfg, const char *database,
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
    edb_partition_info *out = calloc(total + 1, sizeof(*out));
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
        out[n].cache_size = json_i64(item, "cache_size");
        out[n].auto_cache = json_bool(item, "auto_cache", true);
        copy_name(out[n].journal_mode, sizeof(out[n].journal_mode), item,
                  "journal_mode");
        if (out[n].journal_mode[0] == '\0') {
            snprintf(out[n].journal_mode, sizeof(out[n].journal_mode), "TRUNCATE");
        }
        out[n].vacuum_seconds = json_i64(item, "vacuum_seconds");
        out[n].reindex_seconds = json_i64(item, "reindex_seconds");
        n++;
    }
    cJSON_Delete(all);
    *count_out = n;
    return out;
}

/* Resolves a partition's tuning by its shard identity (partition name only:
 * shards are keyed by partition, not database). First matching record wins.
 * The reserved __system__ partition always uses defaults: its config shards
 * are what this very provider reads, so reading them here would recurse. */