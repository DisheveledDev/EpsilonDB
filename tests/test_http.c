/* Integration tests for the stage 3 REST API.
 * Starts a real epsilond instance on an ephemeral port and talks HTTP over
 * real sockets. */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>
#include "test_sleep.h"

#ifndef INADDR_LOOPBACK
#define INADDR_LOOPBACK 0x7f000001UL
#endif

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

static int g_port = 0;
static const char *g_password = "test-pass-123";

static int http_request_pw(const char *method, const char *path,
                           const char *auth, const char *password,
                           const char *body, char **body_out);

static int http_request(const char *method, const char *path,
                        const char *auth, const char *body, char **body_out)
{
    return http_request_pw(method, path, auth, g_password, body, body_out);
}

static int http_request_pw(const char *method, const char *path,
                           const char *auth, const char *password,
                           const char *body, char **body_out)
{
    int fd = socket(AF_INET, SOCK_STREAM, 0);
    if (fd < 0) {
        return -1;
    }
    struct sockaddr_in addr = {0};
    addr.sin_family = AF_INET;
    addr.sin_port = htons((uint16_t)g_port);
    addr.sin_addr.s_addr = htonl(INADDR_LOOPBACK);
    if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
        close(fd);
        return -1;
    }

    char head[2048];
    char auth_hdr[600] = "";
    char pw_hdr[600] = "";
    if (auth) {
        snprintf(auth_hdr, sizeof(auth_hdr),
                 "Authorization: Bearer %s\r\n", auth);
    }
    if (password && *password) {
        snprintf(pw_hdr, sizeof(pw_hdr),
                 "X-Epsilon-Password: %s\r\n", password);
    }
    int n = snprintf(head, sizeof(head),
                     "%s %s HTTP/1.1\r\n"
                     "Host: localhost\r\n"
                     "%s%s"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     method, path,
                     auth_hdr, pw_hdr,
                     body ? strlen(body) : 0);
    size_t sent = 0;
    while (sent < (size_t)n) {
        ssize_t w = send(fd, head + sent, (size_t)n - sent, 0);
        if (w <= 0) {
            close(fd);
            return -1;
        }
        sent += (size_t)w;
    }
    /* send headers and body in separate packets: the server must buffer
     * partial requests correctly */
    if (body && *body) {
        size_t blen = strlen(body);
        size_t bsent = 0;
        while (bsent < blen) {
            ssize_t w = send(fd, body + bsent, blen - bsent, 0);
            if (w <= 0) {
                close(fd);
                return -1;
            }
            bsent += (size_t)w;
        }
    }

    static char resp[1024 * 1024];
    size_t total = 0;
    for (;;) {
        if (total >= sizeof(resp) - 1) break;
        ssize_t r = recv(fd, resp + total, sizeof(resp) - 1 - total, 0);
        if (r <= 0) break;
        total += (size_t)r;
    }
    close(fd);
    resp[total] = '\0';

    char *hdr_end = strstr(resp, "\r\n\r\n");
    if (!hdr_end) {
        return -1;
    }
    *hdr_end = '\0';
    int status = 0;
    sscanf(resp, "HTTP/1.%*d %d", &status);
    if (body_out) {
        *body_out = hdr_end + 4;
    }
    return status;
}

static bool body_contains(const char *hay, const char *needle)
{
    return hay && strstr(hay, needle) != NULL;
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        return 2;
    }
    g_port = atoi(argv[1]);

    /* wait for the server to accept connections */
    bool up = false;
    for (int i = 0; i < 50 && !up; i++) {
        edb_sleep_us(100000);
        int s = http_request("GET", "/status", NULL, NULL, NULL);
        up = (s == 200);
    }
    CHECK(up);

    /* ---- setup: admin user + database + partitions ---- */
    int s = http_request("POST", "/admin/databases", NULL,
                         "{\"name\":\"app\",\"replication_factor\":1}", NULL);
    CHECK(s == 201);

    s = http_request("POST", "/admin/databases", NULL,
                     "{\"name\":\"app\",\"replication_factor\":1}", NULL);
    CHECK(s == 409);   /* duplicate */

    s = http_request("POST", "/admin/groups", NULL,
                     "{\"name\":\"admins\"}", NULL);
    CHECK(s == 201);
    s = http_request("POST", "/admin/groups", NULL,
                     "{\"name\":\"readers\"}", NULL);
    CHECK(s == 201);

    /* admins=bit1 readers=bit2 */
    uint64_t admins = 1ULL << 0;
    uint64_t readers = 1ULL << 1;

    s = http_request("POST", "/admin/users", NULL,
                     "{\"name\":\"root\",\"groups\":1,\"password\":\"test-pass-123\"}",
                     NULL);   /* bit1 */
    CHECK(s == 201);
    /* from here on the store is bootstrapped: unauthenticated requests
     * have no groups, so the test must authenticate as root */
    s = http_request("POST", "/admin/users", "root",
                     "{\"name\":\"viewer\",\"groups\":2,\"password\":\"test-pass-123\"}",
                     NULL); /* bit2 */
    CHECK(s == 201);

    char pjson[512];
    snprintf(pjson, sizeof(pjson),
             "{\"database\":\"app\",\"name\":\"cache\","
             "\"create_mask\":%llu,\"update_mask\":%llu,"
             "\"read_mask\":%llu,\"delete_mask\":%llu}",
             (unsigned long long)admins,
             (unsigned long long)(admins | readers),
             (unsigned long long)(admins | readers),
             (unsigned long long)admins);
    s = http_request("POST", "/admin/partitions", "root", pjson, NULL);
    CHECK(s == 201);

    /* ---- data operations ---- */
    const char *doc = "{\"kind\":\"widget\",\"color\":\"blue\",\"n\":7}";
    s = http_request("PUT", "/data/app/cache/main/w1", "root", doc, NULL);
    CHECK(s == 200);

    char *body = NULL;
    s = http_request("GET", "/data/app/cache/main/w1", "root", NULL, &body);
    CHECK(s == 200);
    CHECK(body_contains(body, "widget"));
    CHECK(body_contains(body, "blue"));

    /* viewer can read */
    s = http_request("GET", "/data/app/cache/main/w1", "viewer", NULL, &body);
    CHECK(s == 200);

    /* viewer cannot create */
    s = http_request("PUT", "/data/app/cache/main/w2", "viewer",
                     "{\"x\":1}", NULL);
    CHECK(s == 403);

    /* unknown user rejected outright */
    s = http_request("GET", "/data/app/cache/main/w1", "ghost", NULL, &body);
    CHECK(s == 401);

    /* a username without a password is rejected (no impersonation) */
    s = http_request_pw("GET", "/data/app/cache/main/w1", "root", "",
                        NULL, &body);
    CHECK(s == 401);
    /* a wrong password is rejected */
    s = http_request_pw("GET", "/data/app/cache/main/w1", "root",
                        "wrong-pass", NULL, &body);
    CHECK(s == 401);

    /* missing document */
    s = http_request("GET", "/data/app/cache/main/nope", "root", NULL, &body);
    CHECK(s == 404);

    /* more docs for structured JSON filters */
    s = http_request("PUT", "/data/app/cache/main/w2", "root",
                     "{\"kind\":\"widget\",\"color\":\"red\",\"n\":8,"
                     "\"manager\":{\"age\":42}}", NULL);
    CHECK(s == 200);
    s = http_request("PUT", "/data/app/cache/main/g1", "root",
                     "{\"kind\":\"gadget\",\"color\":\"green\",\"n\":9,"
                     "\"manager\":{\"age\":51}}", NULL);
    CHECK(s == 200);

    s = http_request("PUT", "/data/app/cache/main/legacy?filter=color=green",
                     "root", "{\"legacy\":true}", &body);
    CHECK(s == 400);

    s = http_request("POST", "/data/app/cache/main/ids", "root",
                     "{\"key\":\"color\",\"operator\":\"eq\","
                     "\"value\":\"green\"}", &body);
    CHECK(s == 200 && body_contains(body, "g1"));

    s = http_request("POST", "/data/app/cache/main/all", "root",
                     "{\"key\":\"n\",\"operator\":\"lte\",\"value\":8}",
                     &body);
    CHECK(s == 200 && body_contains(body, "blue") &&
          body_contains(body, "red") && !body_contains(body, "green"));

    s = http_request("GET", "/data/app/cache/main/all", "root", NULL, &body);
    CHECK(s == 200);
    CHECK(body_contains(body, "widget") && body_contains(body, "gadget"));

    s = http_request("POST", "/data/app/cache/main/query", "root",
                     "{\"key\":\"manager.age\",\"operator\":\"gt\","
                     "\"value\":42}", &body);
    CHECK(s == 200 && body_contains(body, "gadget") &&
          !body_contains(body, "red"));

    s = http_request("POST", "/data/app/cache/main/query", "root",
                     "{\"filters\":["
                     "{\"key\":\"n\",\"operator\":\"gte\",\"value\":8},"
                     "{\"key\":\"kind\",\"operator\":\"eq\","
                     "\"value\":\"widget\"}]}", &body);
    CHECK(s == 200 && body_contains(body, "red") &&
          !body_contains(body, "green"));

    s = http_request("POST", "/data/app/cache/main/query", "root",
                     "{\"key\":\"n\",\"operator\":\"wat\",\"value\":8}",
                     &body);
    CHECK(s == 400);
    s = http_request("GET", "/data/app/cache/main/ids?filter=color=green",
                     "root", NULL, &body);
    CHECK(s == 400);

    /* ttl expiry param accepted (not expired yet) */
    s = http_request("PUT", "/data/app/cache/main/ttl1?ttl=3600", "root",
                     "{\"t\":1}", NULL);
    CHECK(s == 200);

    /* delete */
    s = http_request("DELETE", "/data/app/cache/main/w2", "root", NULL,
                     NULL);
    CHECK(s == 200);
    s = http_request("GET", "/data/app/cache/main/w2", "root", NULL, &body);
    CHECK(s == 404);

    /* viewer cannot delete */
    s = http_request("DELETE", "/data/app/cache/main/w1", "viewer", NULL,
                     NULL);
    CHECK(s == 403);

    /* ---- admin listing via CLI-style endpoints ---- */
    s = http_request("GET", "/admin/databases", "root", NULL, &body);
    CHECK(s == 200 && body_contains(body, "app"));

    s = http_request("GET", "/admin/partitions?database=app", "root", NULL,
                     &body);
    CHECK(s == 200 && body_contains(body, "cache"));

    s = http_request("GET", "/admin/users", "root", NULL, &body);
    CHECK(s == 200 && body_contains(body, "root"));

    /* non-admin cannot list users (viewer lacks bit 1) */
    s = http_request("GET", "/admin/users", "viewer", NULL, &body);
    CHECK(s == 403);

    /* ---- DELETE endpoints ---- */
    s = http_request("POST", "/admin/groups", "root",
                     "{\"name\":\"temporary\"}", NULL);
    CHECK(s == 201);
    s = http_request("DELETE", "/admin/groups/temporary", "root", NULL,
                     NULL);
    CHECK(s == 200);
    s = http_request("GET", "/admin/groups", "root", NULL, &body);
    CHECK(!body_contains(body, "temporary"));

    s = http_request("POST", "/admin/users", "root",
                     "{\"name\":\"shortlived\",\"groups\":2}", NULL);
    CHECK(s == 201);
    s = http_request("DELETE", "/admin/users/shortlived", "root", NULL,
                     NULL);
    CHECK(s == 200);

    /* passwordless users cannot authenticate over HTTP at all */
    s = http_request("POST", "/admin/users", "root",
                     "{\"name\":\"nopass\",\"groups\":1}", NULL);
    CHECK(s == 201);
    s = http_request_pw("GET", "/admin/users", "nopass", "", NULL, &body);
    CHECK(s == 401);

    char pj[512];
    snprintf(pj, sizeof(pj),
             "{\"database\":\"app\",\"name\":\"temp\","
             "\"create_mask\":1,\"update_mask\":1,\"read_mask\":1,"
             "\"delete_mask\":1}");
    s = http_request("POST", "/admin/partitions", "root", pj, NULL);
    CHECK(s == 201);
    s = http_request("DELETE", "/admin/partitions/app/temp", "root", NULL,
                     NULL);
    CHECK(s == 200);

    /* delete requires admin */
    s = http_request("DELETE", "/admin/users/bob", "viewer", NULL, NULL);
    CHECK(s == 403 || s == 404);   /* bob may not exist; viewer is denied */

    /* ---- settings CRUD ---- */
    s = http_request("POST", "/admin/settings/max.connections", "root",
                     "500", &body);
    CHECK(s == 200);

    body = NULL;
    s = http_request("GET", "/admin/settings/max.connections", "root",
                     NULL, &body);
    CHECK(s == 200 && body_contains(body, "500"));

    /* overwrite with a JSON object value */
    s = http_request("POST", "/admin/settings/cluster", "root",
                     "{\"name\":\"pod1\",\"replicas\":3}", &body);
    CHECK(s == 200);

    body = NULL;
    s = http_request("GET", "/admin/settings/cluster", "root", NULL,
                     &body);
    CHECK(s == 200 && body_contains(body, "pod1") &&
          body_contains(body, "replicas"));

    body = NULL;
    s = http_request("GET", "/admin/settings", "root", NULL, &body);
    CHECK(s == 200 && body_contains(body, "max.connections"));

    s = http_request("GET", "/admin/settings/nope", "root", NULL, &body);
    CHECK(s == 404);

    /* invalid JSON rejected */
    s = http_request("POST", "/admin/settings/bad", "root", "{oops", NULL);
    CHECK(s == 400);

    s = http_request("DELETE", "/admin/settings/max.connections", "root",
                     NULL, NULL);
    CHECK(s == 200);
    s = http_request("GET", "/admin/settings/max.connections", "root",
                     NULL, &body);
    CHECK(s == 404);

    printf("%d tests, %d failures\n", tests_run, tests_failed);
    return tests_failed ? 1 : 0;
}
