/* Plain C test harness for the ZestyDB shard engine. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include <sys/wait.h>

#include "../src/engine/zesty_engine.h"

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

/* Recursively count *.sqlite files under dir. */
static int count_shards(const char *dir)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "find '%s' -name '*.sqlite' | wc -l", dir);
    FILE *f = popen(cmd, "r");
    if (!f) {
        return -1;
    }
    int n = 0;
    if (fscanf(f, "%d", &n) != 1) {
        n = -1;
    }
    pclose(f);
    return n;
}

static bool json_string_eq(cJSON *obj, const char *field, const char *want)
{
    cJSON *item = cJSON_GetObjectItemCaseSensitive(obj, field);
    return cJSON_IsString(item) &&
           strcmp(item->valuestring, want) == 0;
}

static void test_put_get_delete(void)
{
    rm_rf("tests/data/basic");
    zdb_engine *mgr = zdb_engine_open("tests/data/basic");
    CHECK(mgr != NULL);

    const char *doc = "{\"name\":\"alice\",\"age\":30}";
    CHECK(zdb_put(mgr, "users", "profiles", "u1", doc, -1));

    cJSON *got = zdb_get(mgr, "users", "profiles", "u1");
    CHECK(got != NULL);
    CHECK(json_string_eq(got, "name", "alice"));
    CHECK(cJSON_GetObjectItemCaseSensitive(got, "age")->valueint == 30);
    cJSON_Delete(got);

    /* overwrite */
    CHECK(zdb_put(mgr, "users", "profiles", "u1",
                  "{\"name\":\"alice2\",\"age\":31}", -1));
    got = zdb_get(mgr, "users", "profiles", "u1");
    CHECK(got != NULL && json_string_eq(got, "name", "alice2"));
    cJSON_Delete(got);

    /* missing key */
    CHECK(zdb_get(mgr, "users", "profiles", "nope") == NULL);

    /* soft delete */
    CHECK(zdb_delete(mgr, "users", "profiles", "u1"));
    CHECK(zdb_get(mgr, "users", "profiles", "u1") == NULL);

    zdb_engine_close(mgr);
}

static void test_ttl_expiry(void)
{
    rm_rf("tests/data/ttl");
    zdb_engine *mgr = zdb_engine_open("tests/data/ttl");
    CHECK(mgr != NULL);

    CHECK(zdb_put(mgr, "sess", "tokens", "s1", "{\"v\":1}", 1));
    CHECK(zdb_put(mgr, "sess", "tokens", "s2", "{\"v\":2}", -1));

    cJSON *got = zdb_get(mgr, "sess", "tokens", "s1");
    CHECK(got != NULL);
    cJSON_Delete(got);

    sleep(2);

    CHECK(zdb_get(mgr, "sess", "tokens", "s1") == NULL);
    got = zdb_get(mgr, "sess", "tokens", "s2");
    CHECK(got != NULL);
    cJSON_Delete(got);

    zdb_engine_close(mgr);
}

static void test_filters_and_query(void)
{
    rm_rf("tests/data/filters");
    zdb_engine *mgr = zdb_engine_open("tests/data/filters");
    CHECK(mgr != NULL);

    CHECK(zdb_put(mgr, "kv", "main", "a",
                  "{\"name\":\"Alice\",\"age\":41,\"active\":true,"
                  "\"manager\":{\"age\":50},\"deleted\":null,"
                  "\"tags\":[\"red\",\"blue\"],\"meta\":{\"rank\":1}}",
                  -1));
    CHECK(zdb_put(mgr, "kv", "main", "b",
                  "{\"name\":\"Bob\",\"age\":42,\"active\":false,"
                  "\"manager\":{\"age\":42},\"deleted\":null,"
                  "\"tags\":[\"green\"],\"meta\":{\"rank\":2}}",
                  -1));
    CHECK(zdb_put(mgr, "kv", "main", "c",
                  "{\"name\":\"Carol\",\"age\":60,\"active\":true,"
                  "\"manager\":{\"age\":30},\"deleted\":\"yes\","
                  "\"tags\":[\"red\",\"blue\"],\"meta\":{\"rank\":1}}",
                  -1));

    struct {
        const char *json;
        int expected;
    } cases[] = {
        {"{\"key\":\"age\",\"operator\":\"eq\",\"value\":42}", 1},
        {"{\"key\":\"manager.age\",\"operator\":\"eq\",\"value\":42}", 1},
        {"{\"key\":\"age\",\"operator\":\"gt\",\"value\":42}", 1},
        {"{\"key\":\"age\",\"operator\":\"gte\",\"value\":42}", 2},
        {"{\"key\":\"age\",\"operator\":\"lt\",\"value\":42}", 1},
        {"{\"key\":\"age\",\"operator\":\"lte\",\"value\":42}", 2},
        {"{\"key\":\"name\",\"operator\":\"eq\",\"value\":\"Alice\"}", 1},
        {"{\"key\":\"name\",\"operator\":\"ne\",\"value\":\"Bob\"}", 2},
        {"{\"key\":\"active\",\"operator\":\"eq\",\"value\":true}", 2},
        {"{\"key\":\"deleted\",\"operator\":\"eq\",\"value\":null}", 2},
        {"{\"key\":\"tags\",\"operator\":\"eq\",\"value\":[\"red\",\"blue\"]}", 2},
        {"{\"key\":\"meta\",\"operator\":\"eq\",\"value\":{\"rank\":1}}", 2},
        {"{\"key\":\"missing\",\"operator\":\"eq\",\"value\":1}", 0},
        {"{\"key\":\"missing\",\"operator\":\"ne\",\"value\":1}", 0},
        {"{\"key\":\"age') OR 1=1 --\",\"operator\":\"eq\",\"value\":41}", 0},
        {"[]", 3},
        {"{\"key\":\"age\",\"operator\":\"eq\",\"value\":\"42\"}", 0},
        {"[{\"key\":\"active\",\"operator\":\"eq\",\"value\":true},"
         "{\"key\":\"manager.age\",\"operator\":\"gt\",\"value\":40}]", 1},
    };
    for (size_t i = 0; i < sizeof(cases) / sizeof(cases[0]); i++) {
        cJSON *filters = cJSON_Parse(cases[i].json);
        CHECK(zdb_filters_valid(filters));
        cJSON *rows = zdb_query(mgr, "kv", "main", filters);
        CHECK(rows && cJSON_GetArraySize(rows) == cases[i].expected);
        cJSON_Delete(rows);
        cJSON_Delete(filters);
    }

    const char *invalid[] = {
        "{\"key\":\"age\",\"operator\":\"unknown\",\"value\":42}",
        "{\"key\":\"age\",\"operator\":\"gt\",\"value\":\"42\"}",
        "{\"operator\":\"eq\",\"value\":42}",
        "{\"key\":\"manager..age\",\"operator\":\"eq\",\"value\":42}",
        "42",
    };
    for (size_t i = 0; i < sizeof(invalid) / sizeof(invalid[0]); i++) {
        cJSON *filters = cJSON_Parse(invalid[i]);
        CHECK(!zdb_filters_valid(filters));
        CHECK(zdb_query(mgr, "kv", "main", filters) == NULL);
        cJSON_Delete(filters);
    }

    cJSON *nested = cJSON_Parse(
        "{\"key\":\"manager.age\",\"operator\":\"eq\",\"value\":42}");
    size_t count = 0;
    char **ids = zdb_ids(mgr, "kv", "main", nested, &count);
    CHECK(ids && count == 1 && strcmp(ids[0], "b") == 0);
    zdb_free_strings(ids);
    cJSON *all = zdb_all(mgr, "kv", "main", nested);
    CHECK(all && cJSON_GetArraySize(all) == 1);
    cJSON_Delete(all);
    cJSON_Delete(nested);

    CHECK(zdb_delete(mgr, "kv", "main", "b"));
    cJSON *age = cJSON_Parse(
        "{\"key\":\"age\",\"operator\":\"gte\",\"value\":42}");
    ids = zdb_ids(mgr, "kv", "main", age, &count);
    CHECK(ids && count == 1 && strcmp(ids[0], "c") == 0);
    zdb_free_strings(ids);
    cJSON_Delete(age);

    char shard_path[1024];
    char shard_key[33];
    CHECK(zdb_shard_path(mgr, "kv", "main", shard_path, sizeof(shard_path),
                         shard_key));
    sqlite3 *db = NULL;
    CHECK(sqlite3_open_v2(shard_path, &db, SQLITE_OPEN_READONLY, NULL) ==
          SQLITE_OK);
    sqlite3_stmt *table_check = NULL;
    CHECK(sqlite3_prepare_v2(
              db, "SELECT count(*) FROM sqlite_master"
                  " WHERE type='table' AND name='DataFilter'",
              -1, &table_check, NULL) == SQLITE_OK);
    CHECK(sqlite3_step(table_check) == SQLITE_ROW &&
          sqlite3_column_int(table_check, 0) == 0);
    sqlite3_finalize(table_check);
    sqlite3_close(db);

    zdb_engine_close(mgr);
    mgr = zdb_engine_open("tests/data/filters");
    CHECK(mgr != NULL);
    cJSON *persisted = cJSON_Parse(
        "{\"key\":\"manager.age\",\"operator\":\"lt\",\"value\":40}");
    cJSON *rows = zdb_query(mgr, "kv", "main", persisted);
    CHECK(rows && cJSON_GetArraySize(rows) == 1);
    cJSON_Delete(rows);
    cJSON_Delete(persisted);
    zdb_engine_close(mgr);
}

static void test_shard_layout_and_reopen(void)
{
    rm_rf("tests/data/layout");
    zdb_engine *mgr = zdb_engine_open("tests/data/layout");
    CHECK(mgr != NULL);

    CHECK(zdb_put(mgr, "p1", "k1", "x", "{\"v\":1}", -1));
    CHECK(zdb_put(mgr, "p1", "k2", "x", "{\"v\":2}", -1));
    CHECK(zdb_put(mgr, "p2", "k1", "x", "{\"v\":3}", -1));
    CHECK(zdb_put(mgr, "ab", "c", "same", "{\"v\":4}", -1));
    CHECK(zdb_put(mgr, "a", "bc", "same", "{\"v\":5}", -1));

    CHECK(count_shards("tests/data/layout") == 5);
    cJSON *left = zdb_get(mgr, "ab", "c", "same");
    cJSON *right = zdb_get(mgr, "a", "bc", "same");
    CHECK(left && cJSON_GetObjectItemCaseSensitive(left, "v")->valueint == 4);
    CHECK(right && cJSON_GetObjectItemCaseSensitive(right, "v")->valueint == 5);
    cJSON_Delete(left);
    cJSON_Delete(right);

    zdb_engine_close(mgr);

    /* reopen from disk: all data still there */
    mgr = zdb_engine_open("tests/data/layout");
    CHECK(mgr != NULL);
    size_t n = 0;
    char **ids = zdb_ids(mgr, "p2", "k1", NULL, &n);
    CHECK(ids != NULL && n == 1);
    zdb_free_strings(ids);

    cJSON *got = zdb_get(mgr, "p1", "k2", "x");
    CHECK(got != NULL && got->valueint == 0); /* object, not number */
    CHECK(cJSON_GetObjectItemCaseSensitive(got, "v")->valueint == 2);
    cJSON_Delete(got);

    zdb_engine_close(mgr);
}

static void test_cleanup_pass(void)
{
    rm_rf("tests/data/cleanup");
    zdb_engine *mgr = zdb_engine_open("tests/data/cleanup");
    CHECK(mgr != NULL);

    CHECK(zdb_put(mgr, "tmp", "main", "gone", "{\"v\":1}", 1));
    CHECK(zdb_delete(mgr, "tmp", "main", "gone"));

    /* row is invisible but still present until cleanup + grace window */
    CHECK(zdb_get(mgr, "tmp", "main", "gone") == NULL);

    /* force cleanup pass; grace is 2h so nothing should be purged yet */
    CHECK(zdb_force_cleanup(mgr, "tmp", "main"));

    /* engine still functional after cleanup */
    CHECK(zdb_put(mgr, "tmp", "main", "fresh", "{\"v\":9}", -1));
    cJSON *got = zdb_get(mgr, "tmp", "main", "fresh");
    CHECK(got != NULL);
    cJSON_Delete(got);

    zdb_engine_close(mgr);
}

static void test_concurrent_puts(void)
{
    rm_rf("tests/data/concurrent");
    zdb_engine *mgr = zdb_engine_open("tests/data/concurrent");
    CHECK(mgr != NULL);

    const int writers = 4;
    const int per_writer = 100;
    pid_t pids[4];

    for (int w = 0; w < writers; w++) {
        pid_t pid = fork();
        if (pid == 0) {
            char id[32];
            for (int i = 0; i < per_writer; i++) {
                snprintf(id, sizeof(id), "w%d-%d", w, i);
                char doc[64];
                snprintf(doc, sizeof(doc), "{\"w\":%d,\"i\":%d}", w, i);
                if (!zdb_put(mgr, "load", "test", id, doc, -1)) {
                    _exit(1);
                }
            }
            _exit(0);
        }
        pids[w] = pid;
    }
    for (int w = 0; w < writers; w++) {
        int status = -1;
        waitpid(pids[w], &status, 0);
        CHECK(WIFEXITED(status) && WEXITSTATUS(status) == 0);
    }

    size_t n = 0;
    char **ids = zdb_ids(mgr, "load", "test", NULL, &n);
    CHECK(ids != NULL && n == (size_t)(writers * per_writer));
    zdb_free_strings(ids);

    zdb_engine_close(mgr);
}

#include <sys/wait.h>

int main(void)
{
    test_put_get_delete();
    test_ttl_expiry();
    test_filters_and_query();
    test_shard_layout_and_reopen();
    test_cleanup_pass();
    test_concurrent_puts();

    printf("%d tests, %d failures\n", tests_run, tests_failed);
    return tests_failed ? 1 : 0;
}
