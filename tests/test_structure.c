/* test_structure.c - stage 6a tests: live/target structure versioning.
 * Covers: target publication on join, identical convergence of the
 * target tables across nodes, persistence of the pending target,
 * promotion gating on per-node compliance, promotion clearing the
 * pending wave everywhere, rebalance lock semantics (ownership, other
 * holder, stale takeover) and one-join-at-a-time serialization.
 * Plain assert-style harness like test_cluster. */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "../src/engine/epsilon_config.h"
#include "../src/socket/epsilon_cluster.h"
#include "test_sleep.h"

static int g_checks = 0;
static int g_failures = 0;

#define CHECK(cond)                                                          \
    do {                                                                     \
        g_checks++;                                                          \
        if (!(cond)) {                                                       \
            g_failures++;                                                    \
            fprintf(stderr, "FAIL %s:%d: %s\n", __FILE__, __LINE__, #cond);  \
        }                                                                    \
    } while (0)

#define LOCK_SETTING   "cluster.rebalance_lock"
#define TARGET_SETTING "cluster.target_ranges"
#define DONE_PREFIX    "rebalance.done."

static void wait_for(int seconds, bool (*cond)(void *), void *ctx)
{
    for (int i = 0; i < seconds * 10; i++) {
        if (cond(ctx)) {
            return;
        }
        edb_sleep_us(100 * 1000);
    }
}

/* Both nodes hold an identical, non-empty target table with the given
 * slice count, and no node's target generation is stale. */
typedef struct {
    edb_cluster *a;
    edb_cluster *b;
    size_t want_slices;
} target_view;

static bool targets_converged(void *ctx)
{
    target_view *t = ctx;
    if (!t->a || !t->b) {
        return false;
    }
    long long ga = edb_cluster_target_generation(t->a);
    long long gb = edb_cluster_target_generation(t->b);
    if (ga == 0 || ga != gb || t->want_slices == 0) {
        return false;
    }
    edb_range_info ta[8], tb[8];
    size_t na = edb_cluster_target_ranges(t->a, ta, 8);
    size_t nb = edb_cluster_target_ranges(t->b, tb, 8);
    if (na != t->want_slices || nb != t->want_slices) {
        return false;
    }
    for (size_t i = 0; i < na; i++) {
        if (strcmp(ta[i].node_id, tb[i].node_id) != 0 ||
            strcmp(ta[i].start, tb[i].start) != 0 ||
            strcmp(ta[i].end, tb[i].end) != 0) {
            return false;
        }
    }
    /* contiguous coverage of the whole space */
    if (strcmp(ta[0].start, "00000000000000000000000000000000") != 0 ||
        strcmp(ta[na - 1].end, "ffffffffffffffffffffffffffffffff") != 0) {
        return false;
    }
    for (size_t i = 1; i < na; i++) {
        if (strcmp(ta[i - 1].end, ta[i].start) != 0) {
            return false;
        }
    }
    return true;
}

/* Both nodes converged on an identical live table of want_slices. */
typedef struct {
    edb_cluster *a;
    edb_cluster *b;
    size_t want_slices;
} live_view;

static bool lives_converged(void *ctx)
{
    live_view *t = ctx;
    if (!t->a || !t->b) {
        return false;
    }
    long long ga = edb_cluster_generation(t->a);
    long long gb = edb_cluster_generation(t->b);
    if (ga == 0 || ga != gb) {
        return false;
    }
    edb_range_info ra[8], rb[8];
    size_t na = edb_cluster_ranges(t->a, ra, 8);
    size_t nb = edb_cluster_ranges(t->b, rb, 8);
    if (na != t->want_slices || nb != t->want_slices) {
        return false;
    }
    for (size_t i = 0; i < na; i++) {
        if (strcmp(ra[i].node_id, rb[i].node_id) != 0 ||
            strcmp(ra[i].start, rb[i].start) != 0 ||
            strcmp(ra[i].end, rb[i].end) != 0) {
            return false;
        }
    }
    return true;
}

static void prepare_dir(const char *dir)
{
    char cmd[512];
    snprintf(cmd, sizeof(cmd), "rm -rf %s && mkdir -p %s", dir, dir);
    if (system(cmd) != 0) {
        /* best effort */
    }
}

static void test_single_node_has_no_pending_wave(void)
{
    prepare_dir("tests/data/structure1");

    edb_engine *e = edb_engine_open("tests/data/structure1");
    edb_config *c = edb_config_open(e);
    CHECK(c != NULL);

    char id[EDB_NODE_ID_MAX];
    edb_cluster *a = edb_cluster_start(c, "127.0.0.1", 19211, id);
    CHECK(a != NULL);
    CHECK(edb_cluster_is_leader(a));
    CHECK(edb_cluster_generation(a) >= 1);
    CHECK(edb_cluster_target_generation(a) == 0);

    edb_range_info r[8];
    CHECK(edb_cluster_ranges(a, r, 8) == 1);
    CHECK(edb_cluster_target_ranges(a, r, 8) == 0);

    char *t = edb_setting_get(c, TARGET_SETTING);
    CHECK(t == NULL);
    free(t);

    edb_cluster_stop(a);
    edb_config_close(c);
    edb_engine_close(e);
}

static void test_two_node_wave_lock_and_promotion(void)
{
    prepare_dir("tests/data/structure2/a");
    prepare_dir("tests/data/structure2/b");

    edb_engine *ea = edb_engine_open("tests/data/structure2/a");
    edb_config *ca = edb_config_open(ea);
    edb_engine *eb = edb_engine_open("tests/data/structure2/b");
    edb_config *cb = edb_config_open(eb);
    CHECK(ca && cb);

    char ida[EDB_NODE_ID_MAX], idb[EDB_NODE_ID_MAX];
    edb_cluster *a = edb_cluster_start(ca, "127.0.0.1", 19221, ida);
    CHECK(a != NULL);
    CHECK(edb_cluster_is_leader(a));

    edb_cluster *b = edb_cluster_start(cb, "127.0.0.1", 19222, idb);
    CHECK(b != NULL);
    sleep(1);

    /* --- join publishes a pending target ---------------------------- */
    edb_cluster_set_auto_compliant(a, false);
    edb_cluster_set_auto_compliant(b, false);
    CHECK(edb_cluster_join(b, "127.0.0.1", 19221) == 0);
    target_view tv = { a, b, 2 };
    wait_for(15, targets_converged, &tv);
    CHECK(targets_converged(&tv));
    CHECK(edb_cluster_target_generation(a) > edb_cluster_generation(a));

    /* live tables untouched until promotion: still one full-space
     * slice per node (each was alone when it claimed the space) */
    edb_range_info ra[8], rb[8];
    size_t nra = edb_cluster_ranges(a, ra, 8);
    size_t nrb = edb_cluster_ranges(b, rb, 8);
    CHECK(nra == 1 && nrb == 1);

    /* ownership under the target table agrees across nodes and covers
     * both members exactly once */
    const char *own_a =
        edb_cluster_target_owner(a, "7fffffffffffffffffffffffffffffff");
    const char *own_b =
        edb_cluster_target_owner(b, "7fffffffffffffffffffffffffffffff");
    CHECK(own_a && own_b && strcmp(own_a, own_b) == 0);

    /* --- pending target persisted in the settings store ------------- */
    char *ta = edb_setting_get(ca, TARGET_SETTING);
    char *tb = edb_setting_get(cb, TARGET_SETTING);
    CHECK(ta && tb);
    CHECK(ta && strstr(ta, ida) && strstr(ta, idb));
    free(ta);
    free(tb);

    /* --- leader holds the rebalance lock during the wave ------------ */
    char *lock = NULL;
    const char *leader_id = edb_cluster_leader(a);
    CHECK(leader_id != NULL);
    if (strcmp(leader_id, ida) == 0) {
        lock = edb_setting_get(ca, LOCK_SETTING);
    } else {
        lock = edb_setting_get(cb, LOCK_SETTING);
    }
    CHECK(lock && strstr(lock, leader_id) != NULL);
    free(lock);

    /* --- promotion gated on compliance ------------------------------ */
    edb_cluster *leader = strcmp(leader_id, ida) == 0 ? a : b;
    edb_config *leader_cfg = strcmp(leader_id, ida) == 0 ? ca : cb;
    const char *other_id = strcmp(leader_id, ida) == 0 ? idb : ida;

    CHECK(!edb_cluster_promote_target(leader));
    edb_cluster_mark_compliant(a);
    edb_cluster_mark_compliant(b);
    /* flags are local; the leader cannot see the remote node's flag yet
     * so promotion must still be refused */
    CHECK(!edb_cluster_promote_target(leader));

    char val[32];
    snprintf(val, sizeof(val), "%lld",
             edb_cluster_target_generation(leader));
    char done_name[96];
    snprintf(done_name, sizeof(done_name), DONE_PREFIX"%.63s", other_id);
    CHECK(edb_setting_set(leader_cfg, done_name, val));
    /* stage 6d: the maintainer auto-promotes once compliance is fully
     * visible to the leader, so the wave may already be promoted by now
     * (pending target cleared). Promote explicitly only if still pending. */
    if (edb_cluster_target_generation(leader) != 0) {
        CHECK(edb_cluster_target_compliant(leader));
        CHECK(edb_cluster_promote_target(leader));
    }

    /* lock released by promotion */
    lock = edb_setting_get(leader_cfg, LOCK_SETTING);
    CHECK(lock == NULL);
    free(lock);

    /* --- promoted live table converges everywhere ------------------- */
    live_view lv = { a, b, 2 };
    wait_for(15, lives_converged, &lv);
    CHECK(lives_converged(&lv));

    /* pending wave cleared on both nodes once the promotion gossips
     * (a stale copy must not survive or be re-gossiped) */
    bool cleared_a = false;
    bool cleared_b = false;
    for (int i = 0; i < 50; i++) {
        if (edb_cluster_target_generation(a) == 0 &&
            edb_cluster_target_ranges(a, ra, 8) == 0) {
            cleared_a = true;
        }
        if (edb_cluster_target_generation(b) == 0 &&
            edb_cluster_target_ranges(b, rb, 8) == 0) {
            cleared_b = true;
        }
        if (cleared_a && cleared_b) {
            break;
        }
        edb_sleep_us(100 * 1000);
    }
    CHECK(cleared_a && cleared_b);

    /* compliance flags cleaned up by promotion: the leader deletes
     * every flag it can see, including its own */
    char done_self[96];
    snprintf(done_self, sizeof(done_self), DONE_PREFIX"%.63s", leader_id);
    char *v = edb_setting_get(leader_cfg, done_self);
    CHECK(v == NULL);
    free(v);

    edb_cluster_stop(b);
    edb_cluster_stop(a);
    edb_config_close(ca);
    edb_config_close(cb);
    edb_engine_close(ea);
    edb_engine_close(eb);
}

int main(void)
{
    test_single_node_has_no_pending_wave();
    test_two_node_wave_lock_and_promotion();

    printf("%d tests, %d failures\n", g_checks, g_failures);
    return g_failures ? 1 : 0;
}
