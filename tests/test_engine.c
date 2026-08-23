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
    CHECK(zdb_put(mgr, "users", "profiles", "u1", doc, -1, NULL, 0));

    cJSON *got = zdb_get(mgr, "users", "profiles", "u1");
    CHECK(got != NULL);
    CHECK(json_string_eq(got, "name", "alice"));
    CHECK(cJSON_GetObjectItemCaseSensitive(got, "age")->valueint == 30);
    cJSON_Delete(got);

    /* overwrite */
    CHECK(zdb_put(mgr, "users", "profiles", "u1",
                  "{\"name\":\"alice2\",\"age\":31}", -1, NULL, 0));
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

    CHECK(zdb_put(mgr, "sess", "tokens", "s1", "{\"v\":1}", 1, NULL, 0));
    CHECK(zdb_put(mgr, "sess", "tokens", "s2", "{\"v\":2}", -1, NULL, 0));

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

    const char *f_tenant[] = {"tenant=acme"};
    const char *f_region[] = {"region=eu"};
    const char *f_both[] = {"tenant=acme", "region=eu"};

    CHECK(zdb_put(mgr, "kv", "main", "a", "{\"kind\":\"widget\",\"size\":1}",
                  -1, f_tenant, 1));
    CHECK(zdb_put(mgr, "kv", "main", "b", "{\"kind\":\"gadget\",\"size\":2}",
                  -1, f_both, 2));
    CHECK(zdb_put(mgr, "kv", "main", "c", "{\"kind\":\"widget\",\"size\":3}",
                  -1, f_region, 1));
    CHECK(zdb_put(mgr, "kv", "main", "d", "{\"kind\":\"widget\",\"size\":4}",
                  -1, NULL, 0));

    size_t n = 0;
    char **ids = zdb_ids(mgr, "kv", "main", f_tenant, 1, &n);
    CHECK(ids != NULL && n == 2);
    zdb_free_strings(ids);

    ids = zdb_ids(mgr, "kv", "main", f_both, 2, &n);
    CHECK(ids != NULL && n == 1);
    zdb_free_strings(ids);

    ids = zdb_ids(mgr, "kv", "main", NULL, 0, &n);
    CHECK(ids != NULL && n == 4);
    zdb_free_strings(ids);

    /* all: filter selects a and b */
    cJSON *all = zdb_all(mgr, "kv", "main", f_tenant, 1);
    CHECK(all != NULL && cJSON_GetArraySize(all) == 2);
    cJSON_Delete(all);

    /* query adds field=value matching on top of filters */
    const char *field_widget[] = {"kind=widget"};
    cJSON *q = zdb_query(mgr, "kv", "main", f_tenant, 1, field_widget, 1);
    CHECK(q != NULL && cJSON_GetArraySize(q) == 1);
    cJSON *first = cJSON_GetArrayItem(q, 0);
    CHECK(json_string_eq(first, "kind", "widget"));

    const char *field_size2[] = {"size=2"};
    q = zdb_query(mgr, "kv", "main", NULL, 0, field_size2, 1);
    CHECK(q != NULL && cJSON_GetArraySize(q) == 1);
    first = cJSON_GetArrayItem(q, 0);
    CHECK(json_string_eq(first, "kind", "gadget"));
    cJSON_Delete(q);

    /* deleting removes from ids */
    CHECK(zdb_delete(mgr, "kv", "main", "a"));
    ids = zdb_ids(mgr, "kv", "main", f_tenant, 1, &n);
    CHECK(ids != NULL && n == 1);
    zdb_free_strings(ids);

    zdb_engine_close(mgr);
}

static void test_shard_layout_and_reopen(void)
{
    rm_rf("tests/data/layout");
    zdb_engine *mgr = zdb_engine_open("tests/data/layout");
    CHECK(mgr != NULL);

    CHECK(zdb_put(mgr, "p1", "k1", "x", "{\"v\":1}", -1, NULL, 0));
    CHECK(zdb_put(mgr, "p1", "k2", "x", "{\"v\":2}", -1, NULL, 0));
    CHECK(zdb_put(mgr, "p2", "k1", "x", "{\"v\":3}", -1, NULL, 0));

    /* one shard file per (partition, keyspace) pair */
    CHECK(count_shards("tests/data/layout") == 3);

    zdb_engine_close(mgr);

    /* reopen from disk: all data still there */
    mgr = zdb_engine_open("tests/data/layout");
    CHECK(mgr != NULL);
    size_t n = 0;
    char **ids = zdb_ids(mgr, "p2", "k1", NULL, 0, &n);
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

    CHECK(zdb_put(mgr, "tmp", "main", "gone", "{\"v\":1}", 1, NULL, 0));
    CHECK(zdb_delete(mgr, "tmp", "main", "gone"));

    /* row is invisible but still present until cleanup + grace window */
    CHECK(zdb_get(mgr, "tmp", "main", "gone") == NULL);

    /* force cleanup pass; grace is 2h so nothing should be purged yet */
    CHECK(zdb_force_cleanup(mgr, "tmp", "main"));

    /* engine still functional after cleanup */
    CHECK(zdb_put(mgr, "tmp", "main", "fresh", "{\"v\":9}", -1, NULL, 0));
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
                if (!zdb_put(mgr, "load", "test", id, doc, -1, NULL, 0)) {
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
    char **ids = zdb_ids(mgr, "load", "test", NULL, 0, &n);
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
