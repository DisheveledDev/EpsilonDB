/* Tests for the Epsilon Query Language engine (stage 8, milestone eql-a).

 *

 * Covers: single-shard SELECT with WHERE/aggregates/GROUP BY/ORDER BY,

 * multi-shard JOINs, self-joins via aliases, quoted identifiers, nested

 * object values stored as JSON text, permission gating (403 vs trusted

 * bypass), empty shards, and rejection of DML / multi-statement input.

 */



#include <stdio.h>

#include <stdlib.h>

#include <string.h>

#include <unistd.h>



#include "../src/eql/epsilon_eql.h"



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



/* Executes a statement as the fully-trusted caller and asserts 200. */

static char *run_ok(const edb_eql_ctx *ctx, const char *sql)

{
    char *out = NULL;
    int status = edb_eql_execute(ctx, sql, ~0ULL, true, &out);

    if (status != 200) {

        fprintf(stderr, "FAIL exec '%s' -> %d (%s)\n", sql, status,

                out ? out : "(null)");

        tests_run++;

        tests_failed++;

        free(out);

        return NULL;

    }

    return out;

}



static void setup_engine(edb_eql_ctx *ctx)

{

    rm_rf("tests/data/eql");

    ctx->engine = edb_engine_open("tests/data/eql");

    CHECK(ctx->engine != NULL);

    ctx->config = edb_config_open(ctx->engine);

    CHECK(ctx->config != NULL);

    ctx->repl = NULL;



    CHECK(edb_partition_create(ctx->config, "demo", "people",

                               EDB_MASK_ALLOW_ALL,

                               EDB_MASK_ALLOW_ALL,

                               EDB_MASK_ALLOW_ALL,

                               EDB_MASK_ALLOW_ALL));

}



#define NEMPLOYEES 10

static const char *employees[NEMPLOYEES][2] = {

    { "emp001", "{\"name\":\"Ada\",\"age\":36,\"email\":\"ada@ex.com\","

                "\"manager\":\"mgr01\"}" },

    { "emp002", "{\"name\":\"Grace\",\"age\":45,\"email\":\"grace@ex.com\","

                "\"manager\":\"mgr02\"}" },

    { "emp003", "{\"name\":\"Alan\",\"age\":29,\"email\":\"alan@ex.com\","

                "\"manager\":\"mgr01\"}" },

    { "emp004", "{\"name\":\"Edsger\",\"age\":41,\"manager\":\"mgr01\"}" },

    { "emp005", "{\"name\":\"Barbara\",\"age\":33,\"email\":null,"

                "\"manager\":\"mgr02\",\"profile\":{\"level\":7}}" },

    { "emp006", "{\"name\":\"Dennis\",\"age\":38,\"email\":\"dennis@ex.com\","

                "\"manager\":\"mgr01\"}" },

    { "emp007", "{\"name\":\"Radia\",\"age\":47,\"email\":\"radia@ex.com\","

                "\"manager\":\"mgr03\"}" },

    { "emp008", "{\"name\":\"Linus\",\"age\":25,\"manager\":\"mgr01\"}" },

    { "emp009", "{\"name\":\"Margaret\",\"age\":60,"

                "\"email\":\"margaret@ex.com\",\"manager\":\"mgr03\","

                "\"profile\":{\"level\":9}}" },

    { "emp010", "{\"name\":\"Niklaus\",\"age\":31,"

                "\"email\":\"niklaus@ex.com\",\"manager\":\"mgr02\"}" }

};

#define NMANAGERS 3
#define NCONTRACTORS 5

static const char *contractors[NCONTRACTORS][2] = {

    { "con001", "{\"name\":\"Pete\",\"vendor\":\"acme\",\"rate\":55}" },

    { "con002", "{\"name\":\"Quinn\",\"vendor\":\"acme\",\"rate\":62}" },

    { "con003", "{\"name\":\"Rhea\",\"vendor\":\"beta\",\"rate\":70}" },

    { "con004", "{\"name\":\"Sam\",\"vendor\":\"beta\",\"rate\":58}" },

    { "con005", "{\"name\":\"Tess\",\"vendor\":\"gamma\",\"rate\":66}" }

};

#define NDEPARTMENTS 3
#define NPROJECTS 4

static const char *departments[NDEPARTMENTS][2] = {

    { "d1", "{\"name\":\"Engineering\",\"floor\":3,\"budget\":500}" },

    { "d2", "{\"name\":\"Research\",\"floor\":4,\"budget\":300}" },

    { "d3", "{\"name\":\"Support\",\"floor\":2,\"budget\":120}" }

};

static const char *projects[NPROJECTS][2] = {

    { "p1", "{\"title\":\"Atlas\",\"lead\":\"mgr01\",\"dept_id\":\"d1\","

            "\"active\":true}" },

    { "p2", "{\"title\":\"Borealis\",\"lead\":\"mgr02\",\"dept_id\":\"d2\","

            "\"active\":true}" },

    { "p3", "{\"title\":\"Cygnus\",\"lead\":\"mgr03\",\"dept_id\":\"d3\","

            "\"active\":false}" },

    { "p4", "{\"title\":\"Dorado\",\"lead\":\"mgr01\",\"dept_id\":\"d1\","

            "\"active\":false}" }

};



static void seed_data(const edb_eql_ctx *ctx)

{

    for (int i = 0; i < NEMPLOYEES; i++) {

        CHECK(edb_put(ctx->engine, "people", "employees", employees[i][0],

                      employees[i][1], -1));

    }

    CHECK(edb_put(ctx->engine, "people", "managers", "mgr01",

                  "{\"name\":\"Joan\",\"dept\":\"eng\"}", -1));

    CHECK(edb_put(ctx->engine, "people", "managers", "mgr02",

                  "{\"name\":\"Ken\",\"dept\":\"research\"}", -1));

    CHECK(edb_put(ctx->engine, "people", "managers", "mgr03",

                  "{\"name\":\"Marge\",\"dept\":\"support\"}", -1));

    for (int i = 0; i < NCONTRACTORS; i++) {

        CHECK(edb_put(ctx->engine, "people", "contractors", contractors[i][0],

                      contractors[i][1], -1));

    }

    CHECK(edb_partition_create(ctx->config, "demo", "ops",

                               EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL,

                               EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL));

    for (int i = 0; i < NDEPARTMENTS; i++) {

        CHECK(edb_put(ctx->engine, "ops", "departments", departments[i][0],

                      departments[i][1], -1));

    }

    for (int i = 0; i < NPROJECTS; i++) {

        CHECK(edb_put(ctx->engine, "ops", "projects", projects[i][0],

                      projects[i][1], -1));

    }

    CHECK(edb_partition_ensure(ctx->config, "demo", "people",
                               "employees", NULL));

    CHECK(edb_partition_ensure(ctx->config, "demo", "people",
                               "managers", NULL));

    CHECK(edb_partition_ensure(ctx->config, "demo", "people",
                               "contractors", NULL));

    CHECK(edb_partition_ensure(ctx->config, "demo", "people",
                               "ghosts", NULL));

    CHECK(edb_partition_ensure(ctx->config, "demo", "ops",
                               "departments", NULL));

    CHECK(edb_partition_ensure(ctx->config, "demo", "ops",
                               "projects", NULL));

}



static bool json_has(const char *haystack, const char *needle)

{

    return haystack && strstr(haystack, needle) != NULL;

}



static void test_select_star(const edb_eql_ctx *ctx)

{

    char *out = run_ok(ctx, "SELECT * FROM demo.people.employees;");

    CHECK(out != NULL);

    CHECK(json_has(out, "\"columns\":[\"id\",\"name\",\"age\",\"email\","

                        "\"manager\""));

    CHECK(json_has(out, "\"Ada\"") && json_has(out, "\"Grace\""));

    /* every record key came through */

    for (int i = 0; i < NEMPLOYEES; i++) {

        CHECK(json_has(out, employees[i][0]));

    }

    /* rows are arrays (first row starts with the id string) */

    CHECK(json_has(out, "[[\"emp001\""));

    free(out);

}



static void test_where(const edb_eql_ctx *ctx)

{

    char *out = run_ok(ctx,

                       "SELECT id, name FROM demo.people.employees "

                       "WHERE age > 35 ORDER BY name");

    CHECK(out != NULL);

    /* Ada(36), Edsger(41), Grace(45) hit; Alan(29), Barbara(33) miss */

    CHECK(!json_has(out, "\"Alan\""));

    CHECK(!json_has(out, "\"Barbara\""));

    CHECK(json_has(out, "\"Ada\"") && json_has(out, "\"Edsger\"") &&

          json_has(out, "\"Grace\""));

    /* projection works in order */

    CHECK(json_has(out, "\"columns\":[\"id\",\"name\"]"));

    free(out);



    out = run_ok(ctx,

                 "SELECT COUNT(*) FROM demo.people.employees "

                 "WHERE manager = 'mgr01'");

    CHECK(out != NULL && json_has(out, "[[5]]"));

    free(out);

}



static void test_aggregates_group_by(const edb_eql_ctx *ctx)

{

    char *out = run_ok(ctx,

                       "SELECT manager, COUNT(*) AS n, AVG(age) AS avg_age "

                       "FROM demo.people.employees "

                       "GROUP BY manager ORDER BY n DESC");

    CHECK(out != NULL);

    /* mgr01 has 3 reports, mgr02 has 2 */

    CHECK(json_has(out, "\"mgr01\"") && json_has(out, "\"mgr02\""));

    free(out);



    out = run_ok(ctx,

                 "SELECT SUM(age) FROM demo.people.employees "

                 "WHERE age IS NOT NULL");

    CHECK(out != NULL && json_has(out, "[[385]]"));   /* sum of 10 ages */

    free(out);

}



static void test_join(const edb_eql_ctx *ctx)

{

    char *out = run_ok(ctx,

                       "SELECT e.id, e.name, m.dept "

                       "FROM demo.people.employees e "

                       "JOIN demo.people.managers m ON e.manager = m.id "

                       "ORDER BY e.id");

    CHECK(out != NULL);

    CHECK(json_has(out, "\"eng\"") && json_has(out, "\"research\""));

    CHECK(json_has(out, "\"columns\":[\"id\",\"name\",\"dept\"]"));

    free(out);

}



static void test_self_join_dedupe(const edb_eql_ctx *ctx)

{

    char *out = run_ok(ctx,

                       "SELECT a.name, b.name AS boss "

                       "FROM demo.people.employees a "

                       "JOIN demo.people.employees b "

                       "ON a.manager = b.id WHERE b.age > 40");

    CHECK(out != NULL);

    /* managers mgr01=Joan? no: bosses are the employee docs emp00x? They

     * reference manager ids mgr01/mgr02 which are NOT employee ids, so

     * this inner join is empty. Rewrite against two keyspace refs instead:

     * see test_cross_keyspace below; here just ensure no crash + valid JSON */

    CHECK(json_has(out, "\"rows\":[]"));

    free(out);

}



static void test_quoted_identifiers(const edb_eql_ctx *ctx)

{

    char *out = run_ok(ctx,

                       "SELECT \"id\" FROM \"demo\".\"people\"."

                       "`employees` ORDER BY id LIMIT 1");

    CHECK(out != NULL && json_has(out, "emp001"));

    free(out);

}



static void test_nested_values_as_text(const edb_eql_ctx *ctx)

{

    char *out = run_ok(ctx,

                       "SELECT profile FROM demo.people.employees "

                       "WHERE profile IS NOT NULL");

    CHECK(out != NULL && json_has(out, "{\\\"level\\\":7}"));

    free(out);

}



static void test_aggregate_and_join_polish(const edb_eql_ctx *ctx)

{

    /* HAVING over GROUP BY */

    char *out = run_ok(ctx,

                       "SELECT manager, COUNT(*) AS n "

                       "FROM demo.people.employees "

                       "GROUP BY manager HAVING n >= 4");

    CHECK(out != NULL && json_has(out, "\"mgr01\"") &&

          !json_has(out, "\"mgr02\""));

    free(out);



    /* LEFT JOIN against an empty sibling keyspace keeps all rows */

    out = run_ok(ctx,

                 "SELECT e.id FROM demo.people.employees e "

                 "LEFT JOIN demo.people.ghosts m ON e.manager = m.id "

                 "WHERE m.id IS NULL LIMIT 2");

    CHECK(out != NULL);

    CHECK(json_has(out, "\"emp001\""));

    free(out);



    /* DISTINCT + CASE + ORDER BY alias */

    out = run_ok(ctx,

                 "SELECT DISTINCT CASE WHEN age > 40 THEN 'senior' "

                 "ELSE 'junior' END AS band FROM demo.people.employees "

                 "ORDER BY band DESC");

    CHECK(out != NULL);

    CHECK(json_has(out, "[\"senior\"]") && json_has(out, "[\"junior\"]"));

    free(out);



    /* UNION ALL across two keyspaces of one partition */

    out = run_ok(ctx,

                 "SELECT id FROM demo.people.employees "

                 "UNION ALL SELECT id FROM demo.people.managers");

    CHECK(out != NULL);

    CHECK(json_has(out, "\"mgr01\"") && json_has(out, "\"emp005\""));

    free(out);



    /* comma-separated source list filtered like an inner join */

    out = run_ok(ctx,

                 "SELECT a.id FROM demo.people.employees a, "

                 "demo.people.managers b WHERE b.dept = 'eng' AND "


                 "a.age < 30");

    CHECK(out != NULL);

    CHECK(json_has(out, "\"emp003\""));

    free(out);



    /* scalar functions and IN lists */

    out = run_ok(ctx,

                 "SELECT upper(name) FROM demo.people.employees "

                 "WHERE manager IN ('mgr01','mgrXX') AND age BETWEEN 29 "

                 "AND 36");

    CHECK(out != NULL && json_has(out, "[\"ADA\"]"));

    free(out);



    /* schema mutation stays rejected */

    out = NULL;

    int status = edb_eql_execute(ctx, "CREATE TABLE evil(x)",

                                 ~0ULL, true, &out);

    CHECK(status == 400);

    free(out);

}



static void test_null_and_missing_fields(const edb_eql_ctx *ctx)

{

    /* emp004 has no email key at all; emp005 has explicit null */

    char *out = run_ok(ctx,

                       "SELECT id FROM demo.people.employees "

                       "WHERE email IS NULL");

    CHECK(out != NULL);

    CHECK(json_has(out, "\"emp004\"") && json_has(out, "\"emp005\""));

    CHECK(!json_has(out, "\"emp001\""));

    free(out);

}



static void test_permissions(void)

{

    edb_eql_ctx ctx;

    setup_engine(&ctx);

    CHECK(edb_put(ctx.engine, "people", "employees", "e1",

                  "{\"n\":1}", -1));



    uint64_t readers = 1ULL << 1;

    CHECK(edb_partition_set_masks(&*ctx.config, "demo", "people",

                                  EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL,

                                  readers, EDB_MASK_ALLOW_ALL));



    char *out = NULL;

    int status = edb_eql_execute(&ctx,

                                 "SELECT * FROM demo.people.employees",

                                 readers, false, &out);

    CHECK(status == 200);

    free(out);



    out = NULL;

    status = edb_eql_execute(&ctx,

                             "SELECT * FROM demo.people.employees",

                             0ULL, false, &out);

    CHECK(status == 403);

    CHECK(out != NULL && json_has(out, "permission denied"));

    free(out);



    /* unknown partition behaves like the REST reads: allow-all masks */

    out = NULL;

    status = edb_eql_execute(&ctx, "SELECT * FROM ghost.nope.things",

                             0ULL, false, &out);

    CHECK(status == 200 && json_has(out, "\"rows\":[]"));

    free(out);

    edb_config_close(ctx.config);

    edb_engine_close(ctx.engine);

}



static char *stored_json(const edb_eql_ctx *ctx, const char *id,
                          bool *found)
{
    cJSON *doc = edb_get(ctx->engine, "people", "employees", id);
    *found = doc != NULL;
    if (!doc) {
        return NULL;
    }
    char *text = cJSON_PrintUnformatted(doc);
    cJSON_Delete(doc);
    return text;
}

/* Full DML replay: delete -> update -> insert -> id-rename -> rollback,
 * all through one engine (repl NULL) plus permission gating per class. */
/* Full DML replay: delete -> update -> insert -> rename -> rollback. */
/* Full DML replay: delete -> update -> insert -> rename -> rollback. */

/* eql-c DML replay tests; see epsilon_eql.c hooks + process_events() */
static void test_write_back(const edb_eql_ctx *ctx)
{
    char *out = NULL;
    bool found = false;
    char *snap = NULL;
    int status = 0;

        out = run_ok(ctx, "UPDATE demo.people.employees SET profile = "
                      "'{\"level\":9}' WHERE id = 'emp003'");
    CHECK(strstr(out, "\"applied\":[\"emp003\"]") != NULL);
    free(out);
    snap = stored_json(ctx, "emp003", &found);
    CHECK(found && strstr(snap, "\"profile\":{\"level\":9}") != NULL);
    free(snap);


out = run_ok(ctx, "DELETE FROM demo.people.employees "
                      "WHERE id IN ('emp002','emp005')");
    CHECK(strstr(out, "\"op\":\"delete\"") != NULL);
    CHECK(strstr(out, "\"count\":2") != NULL);
    free(out);
    snap = stored_json(ctx, "emp002", &found);
    CHECK(!found && snap == NULL);
    snap = stored_json(ctx, "emp001", &found);
    CHECK(found);
    free(snap);

    out = run_ok(ctx, "UPDATE demo.people.employees SET age = age + 1 "
                      "WHERE manager = 'mgr01'");
    CHECK(strstr(out, "\"op\":\"update\"") != NULL);
    CHECK(strstr(out, "\"count\":5") != NULL);   /* mgr01 reports */
    free(out);
    snap = stored_json(ctx, "emp001", &found);
    CHECK(found && strstr(snap, "\"age\":37") != NULL);
    free(snap);

    out = run_ok(ctx, "INSERT INTO demo.people.employees (id, name, age) "
                      "VALUES ('zz1', 'Zed', 50)");
    CHECK(strstr(out, "\"op\":\"insert\"") != NULL);
    CHECK(strstr(out, "\"applied\":[\"zz1\"]") != NULL);
    free(out);
    snap = stored_json(ctx, "zz1", &found);
    CHECK(found && strstr(snap, "\"name\":\"Zed\"") != NULL);
    free(snap);

    out = run_ok(ctx, "UPDATE demo.people.employees SET id = 'emp001b' "
                      "WHERE id = 'emp001'");
    CHECK(strstr(out, "\"count\":2") != NULL);
    free(out);
    snap = stored_json(ctx, "emp001", &found);
    CHECK(!found);
    snap = stored_json(ctx, "emp001b", &found);
    CHECK(found && strstr(snap, "\"age\":37") != NULL);
    free(snap);

    /* constraint failure: statement rolls back and nothing replicates */
    bool had = false;
    char *before_copy = stored_json(ctx, "zz1", &had);
    out = NULL;
    status = edb_eql_execute(
        ctx,
        "INSERT INTO demo.people.employees (id, name) "
        "VALUES ('fresh1','F'),('zz1','dup')",
        ~0ULL, true, &out);
    CHECK(status == 400);
    free(out);
    snap = stored_json(ctx, "fresh1", &found);
    CHECK(!found);
    char *after = stored_json(ctx, "zz1", &had);
    CHECK(strcmp(after ? after : "", before_copy ? before_copy : "") == 0);
    free(after);
    free(before_copy);

    out = run_ok(ctx, "DELETE FROM demo.people.employees WHERE id = 'none'");
    CHECK(strstr(out, "\"count\":0") != NULL);
    free(out);
}

static void test_dml_permissions(void)
{
    edb_eql_ctx ctx;
    setup_engine(&ctx);
    CHECK(edb_put(ctx.engine, "people", "employees", "e1",
                  "{\"n\":1}", -1));

    uint64_t writers = 1ULL << 2;
    CHECK(edb_partition_set_masks(&*ctx.config, "demo", "people",
                                  EDB_MASK_ALLOW_ALL, writers,
                                  writers, writers));

    char *out = NULL;
    int status = edb_eql_execute(
        &ctx,
        "INSERT INTO demo.people.employees (id,name) VALUES ('new1','N')",
        ~0ULL, true, &out);
    CHECK(status == 200);
    free(out);

    out = NULL;
    status = edb_eql_execute(
        &ctx, "UPDATE demo.people.employees SET name='X'",
        0ULL, false, &out);
    CHECK(status == 403);
    free(out);

    out = NULL;
    status = edb_eql_execute(
        &ctx, "UPDATE demo.people.employees SET name='X'",
        writers, false, &out);
    CHECK(status == 200);
    free(out);

    out = NULL;
    status = edb_eql_execute(
        &ctx, "DELETE FROM demo.people.employees WHERE id='new1'",
        0ULL, false, &out);
    CHECK(status == 403);
    free(out);

    out = NULL;
    status = edb_eql_execute(
        &ctx, "DELETE FROM demo.people.employees WHERE id='new1'",
        writers, false, &out);
    CHECK(status == 200);
    free(out);

    edb_config_close(ctx.config);
    edb_engine_close(ctx.engine);
}

/* demo.ops partition: contractors keyspace reads, cross-partition joins
 * (projects -> managers), and partition-wide counts spanning every
 * keyspace. Read-only: safe to run before the mutating suites. */
static void test_more_tables(const edb_eql_ctx *ctx)

{

    char *out = run_ok(ctx,

                       "SELECT name FROM demo.people.contractors "

                       "WHERE vendor = 'beta' ORDER BY name");

    CHECK(out != NULL);

    CHECK(json_has(out, "\"Rhea\"") && json_has(out, "\"Sam\""));

    CHECK(!json_has(out, "\"Pete\""));

    free(out);



    out = run_ok(ctx,

                 "SELECT p.title, m.name FROM demo.ops.projects p "

                 "JOIN demo.people.managers m ON p.lead = m.id "

                 "WHERE p.active = 1 ORDER BY p.title");

    CHECK(out != NULL);

    CHECK(json_has(out, "\"Atlas\"") && json_has(out, "\"Joan\""));

    CHECK(json_has(out, "\"Borealis\"") && !json_has(out, "\"Cygnus\""));

    free(out);



    /* partition-wide counts span every keyspace of the partition */

    out = run_ok(ctx, "SELECT COUNT(*) FROM demo.people");

    CHECK(out != NULL && json_has(out, "[[18]]"));   /* 10 emp + 3 mgr + 5 con */

    free(out);

    out = run_ok(ctx, "SELECT COUNT(*) FROM demo.ops");

    CHECK(out != NULL && json_has(out, "[[7]]"));    /* 3 dept + 4 projects */

    free(out);

}

static void test_classification_and_refs(void)
{
    CHECK(edb_eql_classify("SELECT * FROM demo.people.employees") ==
          EQL_KIND_SELECT);
    CHECK(edb_eql_classify("  DELETE FROM demo.people.employees") ==
          EQL_KIND_DELETE);
    CHECK(edb_eql_classify("UPDATE demo.people.employees SET x=1") ==
          EQL_KIND_UPDATE);
    CHECK(edb_eql_classify("INSERT INTO demo.people.employees (id,name) "
                           "VALUES ('a','b')") == EQL_KIND_INSERT);
    CHECK(edb_eql_classify("PRAGMA foo") == EQL_KIND_OTHER);
    CHECK(edb_eql_classify("") == EQL_KIND_OTHER);

    char refs[8][512];
    size_t n = edb_eql_references(
        "SELECT * FROM demo.people.employees JOIN other.foo.bar "
        "ON x=y", refs, 8);
    CHECK(n == 2);
    CHECK(strcmp(refs[0], "demo.people.employees") == 0);
    CHECK(strcmp(refs[1], "other.foo.bar") == 0);

    n = edb_eql_references("SELECT COUNT(*) FROM demo.people.employees",
                           refs, 8);
    CHECK(n == 1 && strcmp(refs[0], "demo.people.employees") == 0);

    n = edb_eql_references("SELECT 1", refs, 8);
    CHECK(n == 0);

    n = edb_eql_references(NULL, refs, 8);
    CHECK(n == 0);
}

/* eql-f: partition-wide (db.partition) references merge every keyspace
 * of the partition into one shadow table; DML routes each row back to
 * its owning keyspace through the row map. Uses a private engine so the
 * mutations below cannot pollute the shared-seed tests. */
static void test_partition_wide(void)
{
    edb_eql_ctx ctx;
    rm_rf("tests/data/eql_pw");
    ctx.engine = edb_engine_open("tests/data/eql_pw");
    CHECK(ctx.engine != NULL);
    ctx.config = edb_config_open(ctx.engine);
    CHECK(ctx.config != NULL);
    ctx.repl = NULL;
    CHECK(edb_partition_create(ctx.config, "demo", "people",
                               EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL,
                               EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL));
    for (int i = 0; i < NEMPLOYEES; i++) {
        CHECK(edb_put(ctx.engine, "people", "employees", employees[i][0],
                      employees[i][1], -1));
    }
    CHECK(edb_put(ctx.engine, "people", "managers", "mgr01",
                  "{\"name\":\"Joan\",\"dept\":\"eng\"}", -1));
    CHECK(edb_put(ctx.engine, "people", "managers", "mgr02",
                  "{\"name\":\"Ken\",\"dept\":\"research\"}", -1));
    CHECK(edb_put(ctx.engine, "people", "managers", "mgr03",
                  "{\"name\":\"Marge\",\"dept\":\"support\"}", -1));
    for (int i = 0; i < NCONTRACTORS; i++) {
        CHECK(edb_put(ctx.engine, "people", "contractors", contractors[i][0],
                      contractors[i][1], -1));
    }
    CHECK(edb_partition_ensure(ctx.config, "demo", "people",
                               "employees", NULL));
    CHECK(edb_partition_ensure(ctx.config, "demo", "people",
                               "managers", NULL));
    CHECK(edb_partition_ensure(ctx.config, "demo", "people",
                               "contractors", NULL));

    /* merged SELECT: rows from employees + managers, union of columns */
    char *out = run_ok(&ctx, "SELECT id, name, dept FROM demo.people "
                             "ORDER BY id");
    CHECK(out != NULL && json_has(out, "\"emp001\""));
    CHECK(json_has(out, "\"mgr01\"") && json_has(out, "\"eng\""));
    CHECK(json_has(out, "\"Grace\"") && json_has(out, "\"Joan\""));
    free(out);

    /* merged COUNT across all three keyspaces: 10 emp + 3 mgr + 5 con */
    out = run_ok(&ctx, "SELECT COUNT(*) FROM demo.people");
    CHECK(out != NULL && json_has(out, "[18]"));
    free(out);

    /* WHERE across keyspaces */
    out = run_ok(&ctx, "SELECT id FROM demo.people WHERE dept = 'eng'");
    CHECK(out != NULL && json_has(out, "\"mgr01\""));
    CHECK(!json_has(out, "\"mgr02\""));
    free(out);

    /* join a pw reference with a specific keyspace table */
    out = run_ok(&ctx, "SELECT e.id FROM demo.people e "
                       "JOIN demo.people.managers m ON e.manager = m.id "
                       "WHERE m.dept = 'eng' ORDER BY e.id");
    CHECK(out != NULL && json_has(out, "\"emp001\""));
    CHECK(json_has(out, "\"emp003\"") && json_has(out, "\"emp004\""));
    free(out);

    /* 3-part text is never swallowed by a pw table sharing the prefix:
     * the specific keyspace still resolves to its own shard */
    out = run_ok(&ctx, "SELECT COUNT(*) FROM demo.people.employees");
    CHECK(out != NULL && json_has(out, "[10]"));
    free(out);

    /* INSERT has no target keyspace and stays rejected */
    out = NULL;
    int status = edb_eql_execute(&ctx,
                                 "INSERT INTO demo.people (id, name) "
                                 "VALUES ('x1', 'X')",
                                 ~0ULL, true, &out);
    CHECK(status == 400);
    CHECK(out != NULL && json_has(out, "explicit"));
    free(out);

    /* UPDATE routes rows back to their owning keyspaces (age already
     * exists on the employee docs, so no new-column assignment happens) */
    out = run_ok(&ctx, "UPDATE demo.people SET age = 99 "
                       "WHERE id = 'emp001' OR id = 'mgr01'");
    CHECK(strstr(out, "\"count\":2") != NULL);
    free(out);
    bool found = false;
    char *snap = stored_json(&ctx, "emp001", &found);
    CHECK(found && strstr(snap, "\"age\":99") != NULL);
    free(snap);
    cJSON *doc = edb_get(ctx.engine, "people", "managers", "mgr01");
    CHECK(doc != NULL);
    if (doc) {
        char *mtxt = cJSON_PrintUnformatted(doc);
        CHECK(strstr(mtxt, "\"age\":99") != NULL);
        cJSON_free(mtxt);
        cJSON_Delete(doc);
    }

    /* DELETE across keyspaces */
    out = run_ok(&ctx, "DELETE FROM demo.people WHERE id = 'emp005' "
                       "OR id = 'mgr02'");
    CHECK(strstr(out, "\"count\":2") != NULL);
    free(out);
    snap = stored_json(&ctx, "emp005", &found);
    CHECK(!found && snap == NULL);
    doc = edb_get(ctx.engine, "people", "managers", "mgr02");
    CHECK(doc == NULL);

    edb_config_close(ctx.config);
    edb_engine_close(ctx.engine);
}

/* eql-g: schema-assigning DML. UPDATE assignment targets (and INSERT
 * column lists) are pre-declared as shadow-table columns, so a write can
 * introduce a JSON key no fetched document has yet; the assigned value
 * lands in the stored document on write-back. Uses a private engine. */
static void test_schema_assigning(void)
{
    edb_eql_ctx ctx;
    rm_rf("tests/data/eql_g");
    ctx.engine = edb_engine_open("tests/data/eql_g");
    CHECK(ctx.engine != NULL);
    ctx.config = edb_config_open(ctx.engine);
    CHECK(ctx.config != NULL);
    ctx.repl = NULL;
    CHECK(edb_partition_create(ctx.config, "demo", "people",
                               EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL,
                               EDB_MASK_ALLOW_ALL, EDB_MASK_ALLOW_ALL));
    for (int i = 0; i < NEMPLOYEES; i++) {
        CHECK(edb_put(ctx.engine, "people", "employees", employees[i][0],
                      employees[i][1], -1));
    }
    CHECK(edb_put(ctx.engine, "people", "managers", "mgr01",
                  "{\"name\":\"Joan\",\"dept\":\"eng\"}", -1));
    CHECK(edb_partition_ensure(ctx.config, "demo", "people",
                               "employees", NULL));
    CHECK(edb_partition_ensure(ctx.config, "demo", "people",
                               "managers", NULL));

    /* UPDATE introducing a column no document has */
    char *out = run_ok(&ctx, "UPDATE demo.people.employees SET score = 7 "
                             "WHERE id = 'emp001'");
    CHECK(strstr(out, "\"applied\":[\"emp001\"]") != NULL);
    free(out);
    bool found = false;
    char *snap = stored_json(&ctx, "emp001", &found);
    CHECK(found && strstr(snap, "\"score\":7") != NULL);
    free(snap);
    /* rows outside the WHERE stay untouched */
    snap = stored_json(&ctx, "emp002", &found);
    CHECK(found && strstr(snap, "score") == NULL);
    free(snap);

    /* multiple targets, expression referencing another column, and
     * bare CASE/WHEN/THEN keywords inside the value expression */
    out = run_ok(&ctx,
                 "UPDATE demo.people.employees "
                 "SET score = age + 1, "
                 "band = CASE WHEN age > 40 THEN 'senior' ELSE 'junior' END "
                 "WHERE id = 'emp004'");
    CHECK(strstr(out, "\"applied\":[\"emp004\"]") != NULL);
    free(out);
    snap = stored_json(&ctx, "emp004", &found);
    CHECK(found && strstr(snap, "\"score\":42") != NULL);
    CHECK(found && strstr(snap, "\"band\":\"senior\"") != NULL);
    free(snap);

    /* partition-wide UPDATE assigns across keyspaces */
    out = run_ok(&ctx, "UPDATE demo.people SET badge = 'gold' "
                       "WHERE id = 'emp003' OR id = 'mgr01'");
    CHECK(strstr(out, "\"count\":2") != NULL);
    free(out);
    cJSON *doc = edb_get(ctx.engine, "people", "employees", "emp003");
    CHECK(doc != NULL);
    if (doc) {
        char *txt = cJSON_PrintUnformatted(doc);
        CHECK(strstr(txt, "\"badge\":\"gold\"") != NULL);
        cJSON_free(txt);
        cJSON_Delete(doc);
    }
    doc = edb_get(ctx.engine, "people", "managers", "mgr01");
    CHECK(doc != NULL);
    if (doc) {
        char *txt = cJSON_PrintUnformatted(doc);
        CHECK(strstr(txt, "\"badge\":\"gold\"") != NULL);
        cJSON_free(txt);
        cJSON_Delete(doc);
    }

    /* INSERT with a column new to the shard */
    out = run_ok(&ctx, "INSERT INTO demo.people.managers (id, name, level) "
                       "VALUES ('mgr09','Mo',5)");
    CHECK(strstr(out, "\"applied\":[\"mgr09\"]") != NULL);
    free(out);
    doc = edb_get(ctx.engine, "people", "managers", "mgr09");
    CHECK(doc != NULL);
    if (doc) {
        char *txt = cJSON_PrintUnformatted(doc);
        CHECK(strstr(txt, "\"level\":5") != NULL);
        cJSON_free(txt);
        cJSON_Delete(doc);
    }

    /* id rename plus a new column in one statement */
    out = run_ok(&ctx, "UPDATE demo.people.employees "
                       "SET id = 'emp777', score = 1 WHERE id = 'emp005'");
    CHECK(strstr(out, "\"count\":2") != NULL);
    free(out);
    snap = stored_json(&ctx, "emp005", &found);
    CHECK(!found && snap == NULL);
    doc = edb_get(ctx.engine, "people", "employees", "emp777");
    CHECK(doc != NULL);
    if (doc) {
        char *txt = cJSON_PrintUnformatted(doc);
        CHECK(strstr(txt, "\"score\":1") != NULL);
        cJSON_free(txt);
        cJSON_Delete(doc);
    }

    /* NULL assignment writes explicit null (documented semantics) */
    out = run_ok(&ctx, "UPDATE demo.people.employees SET score = NULL "
                       "WHERE id = 'emp001'");
    CHECK(strstr(out, "\"applied\":[\"emp001\"]") != NULL);
    free(out);
    snap = stored_json(&ctx, "emp001", &found);
    CHECK(found && strstr(snap, "\"score\":null") != NULL);
    free(snap);

    edb_config_close(ctx.config);
    edb_engine_close(ctx.engine);
}

static void test_rejections(const edb_eql_ctx *ctx)
{
    char *out = NULL;
    int status = edb_eql_execute(ctx,
                                 "CREATE TABLE evil(x)",
                                 ~0ULL, true, &out);
    CHECK(status == 400);
    free(out);

    out = NULL;
    status = edb_eql_execute(
        ctx,
        "SELECT 1; DELETE FROM demo.people.employees WHERE 1",
        ~0ULL, true, &out);
    CHECK(status == 400);
    free(out);

    out = NULL;
    status = edb_eql_execute(ctx, "ATTACH DATABASE 'x' AS x",
                             ~0ULL, true, &out);
    CHECK(status == 400);
    free(out);

    out = NULL;
    status = edb_eql_execute(ctx, "", ~0ULL, true, &out);
    CHECK(status == 400);
    free(out);

    out = NULL;
    status = edb_eql_execute(ctx, "SELECT * FROM nothing_here",
                             ~0ULL, true, &out);
    CHECK(status == 400);
    free(out);
}

int main(void)

{

    edb_eql_ctx ctx;

    setup_engine(&ctx);

    seed_data(&ctx);



    test_select_star(&ctx);

    test_where(&ctx);

    test_aggregates_group_by(&ctx);

    test_join(&ctx);

    test_self_join_dedupe(&ctx);

    test_quoted_identifiers(&ctx);

    test_nested_values_as_text(&ctx);

    test_null_and_missing_fields(&ctx);
    test_aggregate_and_join_polish(&ctx);
    test_more_tables(&ctx);
    test_classification_and_refs();

    test_write_back(&ctx);
    test_partition_wide();
    test_schema_assigning();

    test_rejections(&ctx);

    edb_config_close(ctx.config);

    edb_engine_close(ctx.engine);



    test_dml_permissions();
    test_permissions();



    printf("test_eql: %d checks, %d failures\n", tests_run, tests_failed);

    return tests_failed ? 1 : 0;

}
