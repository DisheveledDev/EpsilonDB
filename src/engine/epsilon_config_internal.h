/* epsilon_config_internal.h - shared state and helpers across the
 * config module translation units (epsilon_config.c and its entity
 * splits). Not installed.
 */

#ifndef EPSILON_CONFIG_INTERNAL_H
#define EPSILON_CONFIG_INTERNAL_H

#include <pthread.h>

#include "epsilon_config.h"

/* Every config record is a JSON document stored in a dedicated __system__
 * keyspace, so list operations need no secondary filter records. */
#define CFG_KEYSPACE_DATABASES   "config_databases"
#define CFG_KEYSPACE_GROUPS      "config_groups"
#define CFG_KEYSPACE_USERS       "config_users"
#define CFG_KEYSPACE_PARTITIONS  "config_partitions"
#define CFG_KEYSPACE_KEYSPACES   "config_keyspaces"
#define CFG_KEYSPACE_SETTINGS    "config_settings"
#define CFG_KEYSPACE_CODE        "config_code"

struct edb_config {
    edb_engine *engine;
    bool owns_engine;
    pthread_mutex_t replicate_lock;
    pthread_mutex_t group_lock;
    edb_config_replicate_fn replicate;
    void *replicate_ctx;
};

#define NAME_OK(name, type) \
    ((name) && *(name) && strlen(name) < sizeof(((type *)0)->name))

/* --- shared record helpers (epsilon_config.c) ------------------------ */
char *record_json(const cJSON *obj);
cJSON *fetch(edb_config *cfg, const char *keyspace, const char *id);
bool store(edb_config *cfg, const char *keyspace, const char *id,
           cJSON *obj);
bool remove_record(edb_config *cfg, const char *keyspace, const char *id);
cJSON *collect(edb_config *cfg, const char *keyspace);
void copy_name(char *dst, size_t cap, const cJSON *obj, const char *field);
uint64_t json_u64(const cJSON *obj, const char *field);
void set_json_u64(cJSON *obj, const char *field, uint64_t v);
long long json_i64(const cJSON *obj, const char *field);
void set_json_i64(cJSON *obj, const char *field, long long v);

/* --- entity helpers -------------------------------------------------- */
bool internal_cluster_setting(const char *keyspace, const char *id);
uint64_t next_free_bit(edb_config *cfg);
int hex_value(char c);
bool hex_decode(const char *hex, uint8_t *out, size_t out_len);
void hash_password(const char *salt_hex, const char *password,
                   char out[65]);
bool remove_keyspaces_for(edb_config *cfg, const char *database,
                          const char *partition);

#endif
