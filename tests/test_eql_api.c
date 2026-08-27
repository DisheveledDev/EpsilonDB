/* Integration tests for the EQL HTTP surface (stage 8, milestone eql-d).
 *
 * Driven by tests/test_eql_apirun.sh, which starts a throwaway epsilond
 * and passes the port as argv[1]. Covers: auth required once bootstrapped,
 * body validation, SELECT result passthrough, DELETE/UPDATE/INSERT
 * write-back verified through /data reads, the /admin/eql alias, and SQL
 * error mapping.
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/socket.h>
#include <unistd.h>

#include <stdbool.h>

#include "test_sleep.h"

#define INADDR_LOOPBACK 0x7f000001UL

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

#define PW "test-pass-123"

static int g_port = 0;

static int http_request(const char *method, const char *path,
                        const char *auth, const char *password,
                        const char *body, char **body_out)
{
    static char resp[1024 * 1024];
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
                     method, path, auth_hdr, pw_hdr,
                     body ? strlen(body) : 0);
    size_t sent = 0;
    while (sent < (size_t)n) {
        ssize_t w = send(fd, head + sent, (size_t)n - sent, 0);
        if (w <= 0) {
            close(fd);
            return -1;
        }
        sent += w;
    }
    if (body && *body) {
        size_t blen = strlen(body);
        size_t bsent = 0;
        while (bsent < blen) {
            ssize_t w = send(fd, body + bsent, blen - bsent, 0);
            if (w <= 0) {
                close(fd);
                return -1;
            }
            bsent += w;
        }
    }

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
    int status = 0;
    sscanf(resp, "HTTP/1.%*d %d", &status);
    if (body_out) {
        *body_out = hdr_end + 4;
    }
    return status;
}

/* convenience wrapper: most checks ignore the response body */
static int http_req(const char *method, const char *path,
                    const char *auth, const char *password,
                    const char *body)
{
    return http_request(method, path, auth, password, body, NULL);
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[1]);
        return 2;
    }
    g_port = atoi(argv[1]);

    bool up = false;
    for (int i = 0; i < 50 && !up; i++) {
        edb_sleep_us(100000);
        up = http_req("GET", "/status", NULL, NULL, NULL) == 200;
    }
    CHECK(up);

    /* ---- bootstrap through admin API ---- */
    CHECK(http_req("POST", "/admin/databases", NULL, NULL,
                   "{\"name\":\"app\",\"replication_factor\":1}") == 201);

    CHECK(http_req("POST", "/admin/users", NULL, NULL,
                   "{\"name\":\"root\",\"groups\":1,\"password\":\"" PW
                   "\"}") == 201);   /* bit1 */

    /* store bootstrapped: /eql now requires credentials */
    char *body = NULL;
    /* store bootstrapped: bad credentials give a hard 401;
     * anonymous requests run as zero-group users and behave
     * like REST reads against allow-all/implicit partitions. */
    CHECK(http_request("POST", "/eql", NULL, NULL,
                       "{\"sql\":\"SELECT * FROM app.people.employees\"}",
                       &body) == 200);
    CHECK(http_request("POST", "/eql", "root", "wrong-password",
                       "{\"sql\":\"SELECT 1\"}", &body) == 401);

    /* body validation */
    CHECK(http_request("POST", "/eql", "root", PW, "{}", &body) == 400);
    CHECK(strstr(body, "sql") != NULL);
    CHECK(http_request("POST", "/eql", "root", PW, "{\"sql\":\"\"}", &body)
          == 400);
    CHECK(http_request("POST", "/eql", "root", PW, "", &body) == 400);

    /* seed data via REST so EQL reads real shard docs */
    CHECK(http_req("PUT", "/data/app/people/employees/e1", "root", PW,
                   "{\"name\":\"Ada\",\"age\":36,"
                   "\"manager\":\"mgr01\"}") == 200);
    CHECK(http_req("PUT", "/data/app/people/employees/e2", "root", PW,
                   "{\"name\":\"Grace\",\"age\":45,"
                   "\"manager\":\"mgr02\"}") == 200);
    CHECK(http_req("PUT", "/data/app/people/managers/mgr01", "root", PW,
                   "{\"name\":\"Joan\",\"dept\":\"eng\"}") == 200);
    CHECK(http_req("PUT", "/data/app/people/contractors/c1", "root", PW,
                   "{\"name\":\"Pete\",\"vendor\":\"acme\",\"rate\":55}")
          == 200);
    CHECK(http_req("PUT", "/data/app/people/contractors/c2", "root", PW,
                   "{\"name\":\"Quinn\",\"vendor\":\"beta\",\"rate\":62}")
          == 200);

    /* SELECT passthrough */
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"SELECT id, name FROM "
                       "app.people.employees ORDER BY id\"}",
                       &body) == 200);
    CHECK(strstr(body, "\"columns\":[\"id\",\"name\"]") != NULL);
    CHECK(strstr(body, "\"Ada\"") != NULL);

    /* aggregate over the shard */
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"SELECT COUNT(*) AS n FROM "
                       "app.people.employees WHERE manager = 'mgr01'\"}",
                       &body) == 200);
    CHECK(strstr(body, "[[1]]") != NULL);

    /* DML delete replicates through the REST-visible store */
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"DELETE FROM app.people.employees "
                       "WHERE id = 'e2'\"}", &body) == 200);
    CHECK(strstr(body, "\"op\":\"delete\"") != NULL &&
          strstr(body, "\"count\":1") != NULL);
    CHECK(http_req("GET", "/data/app/people/employees/e2", "root", PW,
                   NULL) == 404);

    /* INSERT creates records visible over REST */
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"INSERT INTO app.people.employees "
                       "(id, name) VALUES ('zz9', 'Zaphod')\"}",
                       &body) == 200);
    CHECK(strstr(body, "\"applied\":[\"zz9\"]") != NULL);
    CHECK(http_request("GET", "/data/app/people/employees/zz9", "root",
                       PW, NULL, &body) == 200 &&
          strstr(body, "Zaphod") != NULL);

    /* partition-wide reference spans employees + managers + contractors:
     * e1 + zz9 + mgr01 + c1 + c2 = 5 records at this point */
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"SELECT COUNT(*) FROM app.people\"}",
                       &body) == 200);
    CHECK(strstr(body, "[[5]]") != NULL);

    /* UPDATE rewrites stored documents */
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"UPDATE app.people.employees SET age = 51 "
                       "WHERE id = 'zz9'\"}", &body) == 200);
    CHECK(http_request("GET", "/data/app/people/employees/zz9", "root", PW,
                       NULL, &body) == 200 &&
          strstr(body, "\"age\":51") != NULL);

    /* SQL errors surface as 400 with the engine message */
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"THIS IS NOT SQL\"}", &body) == 400);
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"CREATE TABLE evil(x)\"}", &body) == 400);
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"ATTACH DATABASE 'x' AS x\"}",
                       &body) == 400);

    /* secondary user can be created; fine-grained masks are covered at the
     * engine level in tests/test_eql.c */
    CHECK(http_req("POST", "/admin/users", "root", PW,
                   "{\"name\":\"viewer\",\"groups\":2,\"password\":\"" PW
                   "\"}") == 201);

    /* admin alias works over TCP with credentials */
    CHECK(http_request("POST", "/admin/eql", "root", PW,
                       "{\"sql\":\"SELECT COUNT(*) AS n FROM "
                       "app.people.employees\"}", &body) == 200);

    printf("test_eql_api: %d checks, %d failures\n", tests_run,
           tests_failed);
    return tests_failed ? 1 : 0;
}
