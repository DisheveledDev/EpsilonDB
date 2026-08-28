/* epsilon_config.h - EpsilonDB system configuration, stored through the same
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
 *     write to them (see edb_partition_ensure); explicit creation is only
 *     needed to assign non-open permission masks.
 *   - Keyspace usage: the system records every distinct
 *     database/partition/keyspace triple that has received a write, so
 *     used keyspaces can be listed per database.
 *
 * All records live in the reserved "__system__" database so they replicate
 * through the same machinery as everything else once clustering exists.
 */

#ifndef EPSILON_CONFIG_H
#define EPSILON_CONFIG_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

#include "epsilon_engine.h"

#define EDB_SYSTEM_DB       "__system__"
#define EDB_MAX_GROUPS      63
#define EDB_MASK_ALLOW_ALL  0ULL

typedef struct edb_config edb_config;
typedef bool (*edb_config_replicate_fn)(void *ctx, const char *keyspace,
                                        const char *id, const char *json);

edb_config *edb_config_open(edb_engine *engine);
void edb_config_close(edb_config *cfg);

/* The shard engine backing this config store. */
edb_engine *edb_config_engine(edb_config *cfg);
void edb_config_set_replicator(edb_config *cfg,
                               edb_config_replicate_fn replicate, void *ctx);

/* --- databases ------------------------------------------------------- */

typedef struct {
    char name[128];
    int replication_factor;
} edb_database_info;

bool edb_database_create(edb_config *cfg, const char *name,
                         int replication_factor);
bool edb_database_delete(edb_config *cfg, const char *name);
bool edb_database_get(edb_config *cfg, const char *name,
                      edb_database_info *out);
edb_database_info *edb_database_list(edb_config *cfg, size_t *count_out);

/* --- security groups -------------------------------------------------- */

typedef struct {
    char name[128];
    uint64_t bit_position;   /* bit index in permission masks (1..63) */
} edb_group_info;

bool edb_group_create(edb_config *cfg, const char *name);
bool edb_group_delete(edb_config *cfg, const char *name);
bool edb_group_get(edb_config *cfg, const char *name, edb_group_info *out);
edb_group_info *edb_group_list(edb_config *cfg, size_t *count_out);

/* --- users ------------------------------------------------------------ */

typedef struct {
    char name[128];
    uint64_t groups;         /* bitmask of group bits this user belongs to */
} edb_user_info;

bool edb_user_create(edb_config *cfg, const char *name, uint64_t groups);
bool edb_user_delete(edb_config *cfg, const char *name);
bool edb_user_set_groups(edb_config *cfg, const char *name, uint64_t groups);
bool edb_user_get(edb_config *cfg, const char *name, edb_user_info *out);
edb_user_info *edb_user_list(edb_config *cfg, size_t *count_out);

/* Stores a salted, iterated SHA-256 hash of `password` on the user record.
 * Returns false when the user does not exist. */
bool edb_user_set_password(edb_config *cfg, const char *name,
                           const char *password);

/* True when the stored password hash matches `password`. */
bool edb_user_verify_password(edb_config *cfg, const char *name,
                              const char *password);

/* True when the user has a stored password. */
bool edb_user_has_password(edb_config *cfg, const char *name);

/* True when at least one user holds admin rights (group bit 1). */
bool edb_admin_exists(edb_config *cfg);

/* --- partitions ------------------------------------------------------- */

typedef struct {
    char database[128];
    char name[256];
    uint64_t create_mask;
    uint64_t update_mask;
    uint64_t read_mask;
    uint64_t delete_mask;
} edb_partition_info;

bool edb_partition_create(edb_config *cfg, const char *database,
                          const char *name, uint64_t create_mask,
                          uint64_t update_mask, uint64_t read_mask,
                          uint64_t delete_mask);
bool edb_partition_delete(edb_config *cfg, const char *database,
                          const char *name);
bool edb_partition_set_masks(edb_config *cfg, const char *database,
                             const char *name, uint64_t create_mask,
                             uint64_t update_mask, uint64_t read_mask,
                             uint64_t delete_mask);
bool edb_partition_get(edb_config *cfg, const char *database,
                       const char *name, edb_partition_info *out);
edb_partition_info *edb_partition_list(edb_config *cfg, const char *database,
                                       size_t *count_out);

/* Transparent partition registration: creates the partition (with
 * allow-all masks) and records keyspace usage if they do not exist yet.
 * Returns true when the write may proceed. *created_out (optional) is
 * set when a new partition record was materialised. */
bool edb_partition_ensure(edb_config *cfg, const char *database,
                          const char *partition, const char *keyspace,
                          bool *created_out);

/* --- keyspace usage registry ------------------------------------------ */

typedef struct {
    char database[128];
    char partition[256];
    char name[128];
} edb_keyspace_info;

/* All known database/partition/keyspace triples that have been written. */
edb_keyspace_info *edb_keyspace_list(edb_config *cfg, size_t *count_out);

/* --- authorization ---------------------------------------------------- */

typedef enum {
    EDB_PERM_CREATE = 0,
    EDB_PERM_UPDATE = 1,
    EDB_PERM_READ   = 2,
    EDB_PERM_DELETE = 3
} edb_permission;

/* Returns true if any of the caller's groups grants permission on the given
 * mask. A mask of 0 allows every operation for every group. */
bool edb_check_perm(uint64_t mask, uint64_t user_groups,
                    edb_permission perm);

/* --- code store (stage 9: Lua function records) ----------------------- */

/* All code records live in the reserved config_code keyspace of
 * __system__ (see CFG_KEYSPACE_CODE), so they replicate to every node
 * with the same machinery as the rest of the config.
 * Named function: {"type":"function","name":..,"code":..}
 * Database action: {"type":"action","name":"<db>_<partition>_<keyspace>_
 *   <event>","database":..,"partition":..,"keyspace":..,"event":..,
 *   "code":..} */

/* Saves (upserts) one code record under `name`; the record must carry a
 * "type" and a "name" field (the name field is overridden with `name`).
 * Replicates through the normal config path. */
bool edb_code_save(edb_config *cfg, const char *name, cJSON *record);

/* Deletes the record named `name`. Returns true when it existed. */
bool edb_code_delete(edb_config *cfg, const char *name);

/* Loads one record by name (caller frees with cJSON_Delete), or NULL
 * when absent. */
cJSON *edb_code_load(edb_config *cfg, const char *name);

/* Every code record as an array of objects, ordered by name (caller
 * frees with cJSON_Delete). */
cJSON *edb_code_list(edb_config *cfg);

/* --- server/cluster settings ------------------------------------------ */

/* Arbitrary named JSON settings (cluster topology knobs, server options).
 * json_value must be valid JSON (any type). Returns the previous value as a
 * malloc'd string when set, or NULL if it did not exist. */
bool edb_setting_set(edb_config *cfg, const char *name,
                     const char *json_value);

/* Returns the stored JSON for a setting as a malloc'd string, NULL if
 * absent. */
char *edb_setting_get(edb_config *cfg, const char *name);

bool edb_setting_delete(edb_config *cfg, const char *name);

/* Returns a NULL-terminated array of malloc'd setting names. */
char **edb_setting_list(edb_config *cfg, size_t *count_out);

/* True when `key` is one of the reserved __system__ config shard keys
 * (databases/groups/users/partitions/keyspaces/settings). GC must never
 * remove these: every node needs its own config view. */
bool edb_config_is_system_key(edb_config *cfg, const char key[33]);

/* Fills `out` (up to `cap` entries) with the reserved __system__ config
 * keyspace names (in stable order) and returns the count. Used by the
 * stage 6e join flow to snapshot config shards onto a joining node. */
size_t edb_config_system_keyspaces(const char **out, size_t cap);

#endif
