/* Tests for the ZestyDB system configuration layer (stage 2). */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

#include "../src/engine/zesty_config.h"

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
    zdb_engine *eng = zdb_engine_open("tests/data/cfgdb");
    CHECK(eng != NULL);
    zdb_config *cfg = zdb_config_open(eng);
    CHECK(cfg != NULL);

    CHECK(zdb_database_create(cfg, "orders", 3));
    CHECK(zdb_database_create(cfg, "sessions", 1));

    /* duplicate rejected */
    CHECK(!zdb_database_create(cfg, "orders", 2));
    /* invalid replication factor */
    CHECK(!zdb_database_create(cfg, "bad", 0));

    zdb_database_info info;
    CHECK(zdb_database_get(cfg, "orders", &info));
    CHECK(strcmp(info.name, "orders") == 0 && info.replication_factor == 3);

    CHECK(!zdb_database_get(cfg, "nope", &info));

    size_t n = 0;
    zdb_database_info *list = zdb_database_list(cfg, &n);
    CHECK(list != NULL && n == 2);
    free(list);

    /* delete + verify */
    CHECK(zdb_database_delete(cfg, "sessions"));
    list = zdb_database_list(cfg, &n);
    CHECK(n == 1);
    free(list);

    zdb_config_close(cfg);
    zdb_engine_close(eng);

    /* reopen: persistence through the shard engine */
    eng = zdb_engine_open("tests/data/cfgdb");
    cfg = zdb_config_open(eng);
    CHECK(zdb_database_get(cfg, "orders", &info));
    CHECK(info.replication_factor == 3);
    zdb_config_close(cfg);
    zdb_engine_close(eng);
}

static void test_groups(void)
{
    rm_rf("tests/data/cfgrp");
    zdb_engine *eng = zdb_engine_open("tests/data/cfgrp");
    zdb_config *cfg = zdb_config_open(eng);
    CHECK(cfg != NULL);

    CHECK(zdb_group_create(cfg, "admins"));
    CHECK(zdb_group_create(cfg, "readers"));
    CHECK(zdb_group_create(cfg, "writers"));

    /* duplicate rejected */
    CHECK(!zdb_group_create(cfg, "admins"));

    /* bits allocated sequentially starting at 1 */
    zdb_group_info g;
    CHECK(zdb_group_get(cfg, "admins", &g) && g.bit_position == 1);
    CHECK(zdb_group_get(cfg, "readers", &g) && g.bit_position == 2);
    CHECK(zdb_group_get(cfg, "writers", &g) && g.bit_position == 3);

    size_t n = 0;
    zdb_group_info *groups = zdb_group_list(cfg, &n);
    CHECK(groups != NULL && n == 3);
    free(groups);

    /* deleting frees the bit for reuse */
    CHECK(zdb_group_delete(cfg, "readers"));
    CHECK(zdb_group_create(cfg, "auditors"));
    CHECK(zdb_group_get(cfg, "auditors", &g) && g.bit_position == 2);

    zdb_config_close(cfg);
    zdb_engine_close(eng);
}

static void test_users(void)
{
    rm_rf("tests/data/cfusr");
    zdb_engine *eng = zdb_engine_open("tests/data/cfusr");
    zdb_config *cfg = zdb_config_open(eng);
    CHECK(cfg != NULL);

    CHECK(zdb_user_create(cfg, "alice", (1ULL << 0) | (1ULL << 2)));
    CHECK(!zdb_user_create(cfg, "alice", 0));   /* duplicate */

    zdb_user_info u;
    CHECK(zdb_user_get(cfg, "alice", &u));
    CHECK(u.groups == ((1ULL << 0) | (1ULL << 2)));

    CHECK(zdb_user_set_groups(cfg, "alice", 1ULL << 1));
    CHECK(zdb_user_get(cfg, "alice", &u) && u.groups == (1ULL << 1));

    CHECK(!zdb_user_set_groups(cfg, "ghost", 1));   /* unknown user fails */
    CHECK(!zdb_user_get(cfg, "ghost", &u));

    size_t n = 0;
    zdb_user_info *users = zdb_user_list(cfg, &n);
    CHECK(users != NULL && n == 1);
    free(users);

    CHECK(zdb_user_delete(cfg, "alice"));
    users = zdb_user_list(cfg, &n);
    CHECK(n == 0);
    free(users);

    zdb_config_close(cfg);
    zdb_engine_close(eng);
}

static void test_partitions_and_perms(void)
{
    rm_rf("tests/data/cfpart");
    zdb_engine *eng = zdb_engine_open("tests/data/cfpart");
    zdb_config *cfg = zdb_config_open(eng);
    CHECK(cfg != NULL);

    uint64_t admins = 1ULL << 0;      /* group bit 1 */
    uint64_t readers = 1ULL << 1;    /* group bit 2 */
    uint64_t writers = 1ULL << 2;    /* group bit 3 */

    CHECK(zdb_partition_create(cfg, "app", "cache",
                               ZDB_MASK_ALLOW_ALL,   /* create */
                               writers | admins,     /* update */
                               readers | writers | admins,
                               admins));             /* delete */

    /* duplicate partition rejected */
    CHECK(!zdb_partition_create(cfg, "app", "cache", 0, 0, 0, 0));

    zdb_partition_info p;
    CHECK(zdb_partition_get(cfg, "app", "cache", &p));
    CHECK(p.create_mask == 0 && p.delete_mask == admins &&
          p.read_mask == (readers | writers | admins));

    /* mask semantics: 0 allows everything */
    CHECK(zdb_check_perm(p.create_mask, 0, ZDB_PERM_CREATE));
    CHECK(zdb_check_perm(p.create_mask, ~0ULL, ZDB_PERM_CREATE));

    /* bit matching */
    CHECK(zdb_check_perm(p.read_mask, readers, ZDB_PERM_READ));
    CHECK(zdb_check_perm(p.read_mask, admins, ZDB_PERM_READ));
    CHECK(!zdb_check_perm(p.read_mask, 0, ZDB_PERM_READ));
    CHECK(zdb_check_perm(p.update_mask, writers, ZDB_PERM_UPDATE));
    CHECK(!zdb_check_perm(p.update_mask, readers, ZDB_PERM_UPDATE));
    CHECK(zdb_check_perm(p.delete_mask, admins, ZDB_PERM_DELETE));
    CHECK(!zdb_check_perm(p.delete_mask, writers, ZDB_PERM_DELETE));

    /* set_masks updates in place */
    CHECK(zdb_partition_set_masks(cfg, "app", "cache", admins, admins,
                                  admins, admins));
    CHECK(zdb_partition_get(cfg, "app", "cache", &p));
    CHECK(p.read_mask == admins && p.create_mask == admins);
    CHECK(!zdb_partition_set_masks(cfg, "app", "missing", 0, 0, 0, 0));

    /* listing filters by database */
    CHECK(zdb_partition_create(cfg, "other", "cache", 0, 0, 0, 0));
    size_t n = 0;
    zdb_partition_info *parts = zdb_partition_list(cfg, "app", &n);
    CHECK(parts != NULL && n == 1);
    free(parts);
    parts = zdb_partition_list(cfg, "other", &n);
    CHECK(parts != NULL && n == 1);
    free(parts);

    /* deleting a database drops its partitions */
    CHECK(zdb_database_create(cfg, "doomed", 1));
    CHECK(zdb_partition_create(cfg, "doomed", "p1", 0, 0, 0, 0));
    CHECK(zdb_database_delete(cfg, "doomed"));
    parts = zdb_partition_list(cfg, "doomed", &n);
    CHECK(parts != NULL && n == 0);
    free(parts);

    zdb_config_close(cfg);
    zdb_engine_close(eng);
}

static void test_high_bit_positions(void)
{
    rm_rf("tests/data/cfbits");
    zdb_engine *eng = zdb_engine_open("tests/data/cfbits");
    zdb_config *cfg = zdb_config_open(eng);
    CHECK(cfg != NULL);

    char name[32];
    for (int i = 1; i <= ZDB_MAX_GROUPS; i++) {
        snprintf(name, sizeof(name), "g%d", i);
        CHECK(zdb_group_create(cfg, name));
    }
    /* all 63 groups exhausted */
    CHECK(!zdb_group_create(cfg, "overflow"));

    zdb_group_info g;
    CHECK(zdb_group_get(cfg, "g63", &g) && g.bit_position == 63);

    /* high-bit masks round-trip through JSON exactly */
    uint64_t high = 1ULL << 62;
    CHECK(zdb_user_create(cfg, "h", high));
    zdb_user_info u;
    CHECK(zdb_user_get(cfg, "h", &u));
    CHECK(u.groups == high);

    zdb_config_close(cfg);
    zdb_engine_close(eng);
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
