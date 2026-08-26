/* Tests for the EpsilonDB system configuration layer (stage 2). */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../src/engine/epsilon_config.h"

static int tests_run = 0;
static int tests_failed = 0;

#define CHECK(cond)                                                       \
    do {                                                                  \
        tests_run++;                                                      \
        if (!(cond)) {                                                    \
            tests_failed++;                                               \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__,       \
                    #cond);                                               \
        }                                                                 \
    } while (0)

static void rm_rf(const char *path)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf '%s'", path);
    if (system(cmd) != 0) {
        fprintf(stderr, "warning: rm -rf %s failed\n", path);
    }
}

static void test_database_crud(void)
{
    rm_rf("tests/data/cfgdb");
    edb_engine *eng = edb_engine_open("tests/data/cfgdb");
    CHECK(eng != NULL);
    edb_config *cfg = edb_config_open(eng);
    CHECK(cfg != NULL);

    CHECK(edb_database_create(cfg, "orders", 3));
    CHECK(edb_database_create(cfg, "sessions", 1));

    /* duplicate rejected */
    CHECK(!edb_database_create(cfg, "orders", 2));
    /* invalid replication factor */
    CHECK(!edb_database_create(cfg, "bad", 0));

    edb_database_info info;
    CHECK(edb_database_get(cfg, "orders", &info));
    CHECK(strcmp(info.name, "orders") == 0 && info.replication_factor == 3);

    CHECK(!edb_database_get(cfg, "nope", &info));

    size_t n = 0;
    edb_database_info *list = edb_database_list(cfg, &n);
    CHECK(list != NULL && n == 2);
    free(list);

    /* delete + verify */
    CHECK(edb_database_delete(cfg, "sessions"));
    list = edb_database_list(cfg, &n);
    CHECK(n == 1);
    free(list);

    edb_config_close(cfg);
    edb_engine_close(eng);

    /* reopen: persistence through the shard engine */
    eng = edb_engine_open("tests/data/cfgdb");
    cfg = edb_config_open(eng);
    CHECK(edb_database_get(cfg, "orders", &info));
    CHECK(info.replication_factor == 3);
    edb_config_close(cfg);
    edb_engine_close(eng);
}

static void test_groups(void)
{
    rm_rf("tests/data/cfgrp");
    edb_engine *eng = edb_engine_open("tests/data/cfgrp");
    edb_config *cfg = edb_config_open(eng);
    CHECK(cfg != NULL);

    CHECK(edb_group_create(cfg, "admins"));
    CHECK(edb_group_create(cfg, "readers"));
    CHECK(edb_group_create(cfg, "writers"));

    /* duplicate rejected */
    CHECK(!edb_group_create(cfg, "admins"));

    /* bits allocated sequentially starting at 1 */
    edb_group_info g;
    CHECK(edb_group_get(cfg, "admins", &g) && g.bit_position == 1);
    CHECK(edb_group_get(cfg, "readers", &g) && g.bit_position == 2);
    CHECK(edb_group_get(cfg, "writers", &g) && g.bit_position == 3);

    size_t n = 0;
    edb_group_info *groups = edb_group_list(cfg, &n);
    CHECK(groups != NULL && n == 3);
    free(groups);

    /* deleting frees the bit for reuse */
    CHECK(edb_group_delete(cfg, "readers"));
    CHECK(edb_group_create(cfg, "auditors"));
    CHECK(edb_group_get(cfg, "auditors", &g) && g.bit_position == 2);

    edb_config_close(cfg);
    edb_engine_close(eng);
}

static void test_users(void)
{
    rm_rf("tests/data/cfusr");
    edb_engine *eng = edb_engine_open("tests/data/cfusr");
    edb_config *cfg = edb_config_open(eng);
    CHECK(cfg != NULL);

    CHECK(edb_user_create(cfg, "alice", (1ULL << 0) | (1ULL << 2)));
    CHECK(!edb_user_create(cfg, "alice", 0));   /* duplicate */

    edb_user_info u;
    CHECK(edb_user_get(cfg, "alice", &u));
    CHECK(u.groups == ((1ULL << 0) | (1ULL << 2)));

    CHECK(edb_user_set_groups(cfg, "alice", 1ULL << 1));
    CHECK(edb_user_get(cfg, "alice", &u) && u.groups == (1ULL << 1));

    CHECK(!edb_user_set_groups(cfg, "ghost", 1));   /* unknown user fails */
    CHECK(!edb_user_get(cfg, "ghost", &u));

    size_t n = 0;
    edb_user_info *users = edb_user_list(cfg, &n);
    CHECK(users != NULL && n == 1);
    free(users);

    CHECK(edb_user_delete(cfg, "alice"));
    users = edb_user_list(cfg, &n);
    CHECK(n == 0);
    free(users);

    edb_config_close(cfg);
    edb_engine_close(eng);
}

static void test_partitions_and_perms(void)
{
    rm_rf("tests/data/cfpart");
    edb_engine *eng = edb_engine_open("tests/data/cfpart");
    edb_config *cfg = edb_config_open(eng);
    CHECK(cfg != NULL);

    uint64_t admins = 1ULL << 0;      /* group bit 1 */
    uint64_t readers = 1ULL << 1;    /* group bit 2 */
    uint64_t writers = 1ULL << 2;    /* group bit 3 */

    CHECK(edb_partition_create(cfg, "app", "cache",
                               EDB_MASK_ALLOW_ALL,   /* create */
                               writers | admins,     /* update */
                               readers | writers | admins,
                               admins));             /* delete */

    /* duplicate partition rejected */
    CHECK(!edb_partition_create(cfg, "app", "cache", 0, 0, 0, 0));

    edb_partition_info p;
    CHECK(edb_partition_get(cfg, "app", "cache", &p));
    CHECK(p.create_mask == 0 && p.delete_mask == admins &&
          p.read_mask == (readers | writers | admins));

    /* stored defaults: cache auto (0), Auto Cache on, vacuum weekly,
     * reindex daily */
    CHECK(p.cache_size == 0 && p.auto_cache && p.vacuum_seconds == 604800 &&
          p.reindex_seconds == 86400);

    /* mask semantics: 0 allows everything */
    CHECK(edb_check_perm(p.create_mask, 0, EDB_PERM_CREATE));
    CHECK(edb_check_perm(p.create_mask, ~0ULL, EDB_PERM_CREATE));

    /* bit matching */
    CHECK(edb_check_perm(p.read_mask, readers, EDB_PERM_READ));
    CHECK(edb_check_perm(p.read_mask, admins, EDB_PERM_READ));
    CHECK(!edb_check_perm(p.read_mask, 0, EDB_PERM_READ));
    CHECK(edb_check_perm(p.update_mask, writers, EDB_PERM_UPDATE));
    CHECK(!edb_check_perm(p.update_mask, readers, EDB_PERM_UPDATE));
    CHECK(edb_check_perm(p.delete_mask, admins, EDB_PERM_DELETE));
    CHECK(!edb_check_perm(p.delete_mask, writers, EDB_PERM_DELETE));

    /* set_masks updates in place */
    CHECK(edb_partition_set_masks(cfg, "app", "cache", admins, admins,
                                  admins, admins));
    CHECK(edb_partition_get(cfg, "app", "cache", &p));
    CHECK(p.read_mask == admins && p.create_mask == admins);
    CHECK(!edb_partition_set_masks(cfg, "app", "missing", 0, 0, 0, 0));

    /* --- Auto Cache setting ------------------------------------------- */
    edb_shard_settings s;
    edb_shard_settings_default(&s);
    /* on by default */
    CHECK(s.auto_cache && s.cache_size == 0);

    /* turning it off persists and survives a round trip */
    s.auto_cache = false;
    CHECK(edb_partition_set_settings(cfg, "app", "cache", &s));
    CHECK(edb_partition_get(cfg, "app", "cache", &p));
    CHECK(p.auto_cache == false && p.cache_size == 0);

    /* turning it back on persists too */
    s.auto_cache = true;
    CHECK(edb_partition_set_settings(cfg, "app", "cache", &s));
    CHECK(edb_partition_get(cfg, "app", "cache", &p));
    CHECK(p.auto_cache == true);

    /* an explicit size is kept alongside the flag; precedence (explicit
     * wins over auto) is enforced in the engine, not the config store */
    s.cache_size = 4096;
    CHECK(edb_partition_set_settings(cfg, "app", "cache", &s));
    CHECK(edb_partition_get(cfg, "app", "cache", &p));
    CHECK(p.cache_size == 4096 && p.auto_cache == true);

    /* a partition record written before auto_cache existed must read back
     * as enabled, so upgrades keep today's behaviour */
    {
        const char *legacy =
            "{\"database\":\"app\",\"name\":\"legacy\","
            "\"cache_size\":\"0\",\"journal_mode\":\"TRUNCATE\"}";
        CHECK(edb_put(eng, EDB_SYSTEM_DB, "config_partitions", "app/legacy",
                      legacy, -1));
        edb_partition_info lp;
        CHECK(edb_partition_get(cfg, "app", "legacy", &lp));
        CHECK(lp.auto_cache == true);
        /* remove it again so the listing assertions below still see one */
        CHECK(edb_partition_delete(cfg, "app", "legacy"));
    }

    /* listing filters by database */
    CHECK(edb_partition_create(cfg, "other", "cache", 0, 0, 0, 0));
    size_t n = 0;
    edb_partition_info *parts = edb_partition_list(cfg, "app", &n);
    CHECK(parts != NULL && n == 1);
    free(parts);
    parts = edb_partition_list(cfg, "other", &n);
    CHECK(parts != NULL && n == 1);
    free(parts);

    /* deleting a database drops its partitions */
    CHECK(edb_database_create(cfg, "doomed", 1));
    CHECK(edb_partition_create(cfg, "doomed", "p1", 0, 0, 0, 0));
    CHECK(edb_database_delete(cfg, "doomed"));
    parts = edb_partition_list(cfg, "doomed", &n);
    CHECK(parts != NULL && n == 0);
    free(parts);

    edb_config_close(cfg);
    edb_engine_close(eng);
}

static void test_high_bit_positions(void)
{
    rm_rf("tests/data/cfbits");
    edb_engine *eng = edb_engine_open("tests/data/cfbits");
    edb_config *cfg = edb_config_open(eng);
    CHECK(cfg != NULL);

    char name[32];
    for (int i = 1; i <= EDB_MAX_GROUPS; i++) {
        snprintf(name, sizeof(name), "g%d", i);
        CHECK(edb_group_create(cfg, name));
    }
    /* all 63 groups exhausted */
    CHECK(!edb_group_create(cfg, "overflow"));

    edb_group_info g;
    CHECK(edb_group_get(cfg, "g63", &g) && g.bit_position == 63);

    /* high-bit masks round-trip through JSON exactly */
    uint64_t high = 1ULL << 62;
    CHECK(edb_user_create(cfg, "h", high));
    edb_user_info u;
    CHECK(edb_user_get(cfg, "h", &u));
    CHECK(u.groups == high);

    edb_config_close(cfg);
    edb_engine_close(eng);
}

int main(void)
{
    test_database_crud();
    test_groups();
    test_users();
    test_partitions_and_perms();
    test_high_bit_positions();

    printf("%d tests, %d failures\n", tests_run, tests_failed);
    return tests_failed ? 1 : 0;
}
