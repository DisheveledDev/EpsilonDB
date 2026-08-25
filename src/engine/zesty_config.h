/* zesty_config.h - ZestyDB system configuration, stored through the same
 * shard engine as user data (the config "dogfoods" the engine).
 *
 * Concepts:
 *   - Databases: named containers with a replication factor (how many nodes
 *     must hold each record).
 *   - Security groups: up to 63 named groups; group N occupies bit N-1 of the
 *     64-bit permission masks. Mask value 0 means "allow all operations for
 *     all security groups".
 *   - Users: named accounts linked to one or more security groups.
 *   - Partitions: per-database partition definitions carrying separate
 *     create/update/read/delete permission masks (int64 bitmasks over
 *     security groups). Partitions are created implicitly by the first
 *     write to them (see zdb_partition_ensure); explicit creation is only
 *     needed to assign non-open permission masks.
 *   - Keyspace usage: the system records every distinct
 *     database/partition/keyspace triple that has received a write, so
 *     used keyspaces can be listed per database.
 *
 * All records live in the reserved "__system__" database so they replicate
 * through the same machinery as everything else once clustering exists.
 */

#ifndef ZESTY_CONFIG_H
#define ZESTY_CONFIG_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "zesty_engine.h"

#define ZDB_SYSTEM_DB       "__system__"
#define ZDB_MAX_GROUPS      63
#define ZDB_MASK_ALLOW_ALL  0ULL

typedef struct zdb_config zdb_config;
typedef bool (*zdb_config_replicate_fn)(void *ctx, const char *keyspace,
                                        const char *id, const char *json);

zdb_config *zdb_config_open(zdb_engine *engine);
void zdb_config_close(zdb_config *cfg);

/* The shard engine backing this config store. */
zdb_engine *zdb_config_engine(zdb_config *cfg);
void zdb_config_set_replicator(zdb_config *cfg,
                               zdb_config_replicate_fn replicate, void *ctx);

/* --- databases ------------------------------------------------------- */

typedef struct {
    char name[128];
    int replication_factor;
} zdb_database_info;

bool zdb_database_create(zdb_config *cfg, const char *name,
                         int replication_factor);
bool zdb_database_delete(zdb_config *cfg, const char *name);
bool zdb_database_get(zdb_config *cfg, const char *name,
                      zdb_database_info *out);
zdb_database_info *zdb_database_list(zdb_config *cfg, size_t *count_out);

/* --- security groups -------------------------------------------------- */

typedef struct {
    char name[128];
    uint64_t bit_position;   /* bit index in permission masks (1..63) */
} zdb_group_info;

bool zdb_group_create(zdb_config *cfg, const char *name);
bool zdb_group_delete(zdb_config *cfg, const char *name);
bool zdb_group_get(zdb_config *cfg, const char *name, zdb_group_info *out);
zdb_group_info *zdb_group_list(zdb_config *cfg, size_t *count_out);

/* --- users ------------------------------------------------------------ */

typedef struct {
    char name[128];
    uint64_t groups;         /* bitmask of group bits this user belongs to */
} zdb_user_info;

bool zdb_user_create(zdb_config *cfg, const char *name, uint64_t groups);
bool zdb_user_delete(zdb_config *cfg, const char *name);
bool zdb_user_set_groups(zdb_config *cfg, const char *name, uint64_t groups);
bool zdb_user_get(zdb_config *cfg, const char *name, zdb_user_info *out);
zdb_user_info *zdb_user_list(zdb_config *cfg, size_t *count_out);

/* Stores a salted, iterated SHA-256 hash of `password` on the user record.
 * Returns false when the user does not exist. */
bool zdb_user_set_password(zdb_config *cfg, const char *name,
                           const char *password);

/* True when the stored password hash matches `password`. */
bool zdb_user_verify_password(zdb_config *cfg, const char *name,
                              const char *password);

/* True when the user has a stored password. */
bool zdb_user_has_password(zdb_config *cfg, const char *name);

/* True when at least one user holds admin rights (group bit 1). */
bool zdb_admin_exists(zdb_config *cfg);

/* --- partitions ------------------------------------------------------- */

typedef struct {
    char database[128];
    char name[256];
    uint64_t create_mask;
    uint64_t update_mask;
    uint64_t read_mask;
    uint64_t delete_mask;
    long long cache_size;       /* SQLite cache_size; 0 = default */
    char journal_mode[16];      /* DELETE | TRUNCATE | WAL */
    long long vacuum_seconds;   /* 0 = never */
    long long reindex_seconds;  /* 0 = never */
} zdb_partition_info;

bool zdb_partition_create(zdb_config *cfg, const char *database,
                          const char *name, uint64_t create_mask,
                          uint64_t update_mask, uint64_t read_mask,
                          uint64_t delete_mask);
bool zdb_partition_delete(zdb_config *cfg, const char *database,
                          const char *name);
bool zdb_partition_set_masks(zdb_config *cfg, const char *database,
                             const char *name, uint64_t create_mask,
                             uint64_t update_mask, uint64_t read_mask,
                             uint64_t delete_mask);
bool zdb_partition_get(zdb_config *cfg, const char *database,
                       const char *name, zdb_partition_info *out);
zdb_partition_info *zdb_partition_list(zdb_config *cfg, const char *database,
                                       size_t *count_out);

/* Updates the SQLite tuning settings (cache size / journal mode / vacuum &
 * reindex intervals) for a partition. Reopens any open shard connection for
 * that partition so the new settings take effect immediately. */
bool zdb_partition_set_settings(zdb_config *cfg, const char *database,
                                const char *name,
                                const zdb_shard_settings *settings);

/* Registers the engine's shard-settings provider so newly opened shards
 * pick up their partition's tuning. Call after zdb_config_open. */
void zdb_config_register_settings(zdb_config *cfg);

/* Transparent partition registration: creates the partition (with
 * allow-all masks) and records keyspace usage if they do not exist yet.
 * Returns true when the write may proceed. *created_out (optional) is
 * set when a new partition record was materialised. */
bool zdb_partition_ensure(zdb_config *cfg, const char *database,
                          const char *partition, const char *keyspace,
                          bool *created_out);

/* --- keyspace usage registry ------------------------------------------ */

typedef struct {
    char database[128];
    char partition[256];
    char name[128];
} zdb_keyspace_info;

/* All known database/partition/keyspace triples that have been written. */
zdb_keyspace_info *zdb_keyspace_list(zdb_config *cfg, size_t *count_out);

/* --- authorization ---------------------------------------------------- */

typedef enum {
    ZDB_PERM_CREATE = 0,
    ZDB_PERM_UPDATE = 1,
    ZDB_PERM_READ   = 2,
    ZDB_PERM_DELETE = 3
} zdb_permission;

/* Returns true if any of the caller's groups grants permission on the given
 * mask. A mask of 0 allows every operation for every group. */
bool zdb_check_perm(uint64_t mask, uint64_t user_groups,
                    zdb_permission perm);

/* --- server/cluster settings ------------------------------------------ */

/* Arbitrary named JSON settings (cluster topology knobs, server options).
 * json_value must be valid JSON (any type). Returns the previous value as a
 * malloc'd string when set, or NULL if it did not exist. */
bool zdb_setting_set(zdb_config *cfg, const char *name,
                     const char *json_value);

/* Returns the stored JSON for a setting as a malloc'd string, NULL if
 * absent. */
char *zdb_setting_get(zdb_config *cfg, const char *name);

bool zdb_setting_delete(zdb_config *cfg, const char *name);

/* Returns a NULL-terminated array of malloc'd setting names. */
char **zdb_setting_list(zdb_config *cfg, size_t *count_out);

/* True when `key` is one of the reserved __system__ config shard keys
 * (databases/groups/users/partitions/keyspaces/settings). GC must never
 * remove these: every node needs its own config view. */
bool zdb_config_is_system_key(zdb_config *cfg, const char key[33]);

/* Fills `out` (up to `cap` entries) with the reserved __system__ config
 * keyspace names (in stable order) and returns the count. Used by the
 * stage 6e join flow to snapshot config shards onto a joining node. */
size_t zdb_config_system_keyspaces(const char **out, size_t cap);

#endif
