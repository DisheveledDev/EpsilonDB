/* Integration tests for the Lua scripting admin surface.
 *
 * Driven by tests/test_lua_apirun.sh, which starts a throwaway epsilond
 * and passes the port as argv[1]. Covers: admin auth on /admin/code,
 * compile validation, skeleton generation for named functions and
 * database actions, the insert/update split over REST, return-based
 * write-back, before* veto (403), after* best-effort handling, EQL
 * write-back firing handlers, and error mapping.
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
    addr.sin_addr.s_addr = htonl(0x7f000001UL);
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
        if (total >= sizeof(resp) - 1) {
            break;
        }
        ssize_t r = recv(fd, resp + total, sizeof(resp) - 1 - total, 0);
        if (r <= 0) {
            break;
        }
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

    /* ---- bootstrap admin (before any user exists, HTTP is open) ---- */
    CHECK(http_req("POST", "/admin/users", NULL, NULL,
                   "{\"name\":\"root\",\"groups\":1,\"password\":\"" PW
                   "\"}") == 201);

    /* ---- admin auth is enforced once bootstrapped ---- */
    /* (anonymous callers run as zero-group users: 403 admin access
     * required, mirroring the other /admin endpoints) */
    char *body = NULL;
    CHECK(http_req("GET", "/admin/code", NULL, NULL, NULL) == 403);
    CHECK(http_req("POST", "/admin/code", NULL, NULL,
                   "{\"type\":\"function\",\"name\":\"x\",\"code\":\"a=1\"}")
          == 403);

    /* ---- compile validation without saving ---- */
    CHECK(http_request("POST", "/admin/code/validate", "root", PW,
                       "{\"code\":\"function f(entity, id) return entity "
                       "end\"}", &body) == 200);
    CHECK(strstr(body, "\"valid\":true") != NULL);
    CHECK(http_request("POST", "/admin/code/validate", "root", PW,
                       "{\"code\":\"function f( end\"}", &body) == 200);
    CHECK(strstr(body, "\"valid\":false") != NULL);
    CHECK(strstr(body, "error") != NULL);
    CHECK(http_request("POST", "/admin/code/validate", "root", PW,
                       "{}", &body) == 400);

    /* ---- create a named function without code: skeleton generated ---- */
    CHECK(http_request("POST", "/admin/code", "root", PW,
                       "{\"type\":\"function\",\"name\":\"generateId\"}",
                       &body) == 200);
    CHECK(http_request("GET", "/admin/code/generateId", "root", PW, NULL,
                       &body) == 200);
    CHECK(strstr(body, "\"type\":\"function\"") != NULL);
    CHECK(strstr(body, "function generateId ()") != NULL);

    /* ---- named function bodies are compile-checked ---- */
    CHECK(http_request("POST", "/admin/code", "root", PW,
                       "{\"type\":\"function\",\"name\":\"badfn\","
                       "\"code\":\"function f( end\"}", &body) == 400);
    CHECK(strstr(body, "compile") != NULL);
    /* non-identifier names are rejected (the skeleton must compile) */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"function\",\"name\":\"my-func\"}") == 400);
    /* malformed bodies are rejected */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"function\"}") == 400);
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"wat\",\"name\":\"n\"}") == 400);

    /* ---- create a database action without code: named + skeleton ---- */
    CHECK(http_request("POST", "/admin/code", "root", PW,
                       "{\"type\":\"action\",\"database\":\"app\","
                       "\"partition\":\"people\","
                       "\"keyspace\":\"employees\","
                       "\"event\":\"beforeUpdate\"}", &body) == 200);
    CHECK(http_request("GET",
                       "/admin/code/app_people_employees_beforeUpdate",
                       "root", PW, NULL, &body) == 200);
    CHECK(strstr(body, "\"type\":\"action\"") != NULL);
    CHECK(strstr(body, "\"database\":\"app\"") != NULL);
    CHECK(strstr(body, "\"event\":\"beforeUpdate\"") != NULL);
    CHECK(strstr(body,
                 "function app_people_employees_beforeUpdate (entity, id)")
          != NULL);

    /* invalid events, missing scopes and non-identifier scopes are
     * rejected */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"partition\":\"people\",\"keyspace\":\"employees\","
                   "\"event\":\"sometimes\"}") == 400);
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"event\":\"afterDelete\"}") == 400);
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"partition\":\"my.part\",\"keyspace\":\"x\","
                   "\"event\":\"afterDelete\"}") == 400);

    /* ---- the generated skeleton actually runs: beforeUpdate rewrites
     * the document through the returned entity ---- */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"partition\":\"people\",\"keyspace\":\"employees\","
                   "\"event\":\"beforeUpdate\","
                   "\"code\":\"function app_people_employees_beforeUpdate"
                   "(entity, id) if entity.name then entity.name = "
                   "string.upper(entity.name) end return entity end\"}")
          == 200);
    CHECK(http_req("PUT", "/data/app/people/employees/s1", "root", PW,
                   "{\"name\":\"Sam\",\"age\":29}") == 200);
    CHECK(http_req("PUT", "/data/app/people/employees/s1", "root", PW,
                   "{\"name\":\"sam\"}") == 200);
    CHECK(http_request("GET", "/data/app/people/employees/s1", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"name\":\"SAM\"") != NULL);

    /* ---- beforeInsert fires for new records only ---- */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"partition\":\"people\",\"keyspace\":\"employees\","
                   "\"event\":\"beforeInsert\","
                   "\"code\":\"function app_people_employees_beforeInsert"
                   "(entity, id) entity.created = true return entity "
                   "end\"}") == 200);
    CHECK(http_req("PUT", "/data/app/people/employees/s2", "root", PW,
                   "{\"name\":\"new\"}") == 200);
    CHECK(http_request("GET", "/data/app/people/employees/s2", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"created\":true") != NULL);
    CHECK(strstr(body, "\"name\":\"new\"") != NULL);

    /* ---- beforeInsert veto rejects the write with 4xx ---- */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"partition\":\"people\",\"keyspace\":\"contractors\","
                   "\"event\":\"beforeInsert\","
                   "\"code\":\"function app_people_contractors_beforeInsert"
                   "(entity, id) if not entity.name then rollback('name is "
                   "required') end end\"}") == 200);
    CHECK(http_request("PUT", "/data/app/people/contractors/c1", "root", PW,
                       "{\"vendor\":\"acme\"}", &body) == 403);
    CHECK(strstr(body, "name is required") != NULL);
    CHECK(http_req("PUT", "/data/app/people/contractors/c2", "root", PW,
                   "{\"name\":\"Pete\"}") == 200);

    /* ---- beforeDelete sees the entity and can veto ---- */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"partition\":\"people\",\"keyspace\":\"protected\","
                   "\"event\":\"beforeDelete\","
                   "\"code\":\"function app_people_protected_beforeDelete"
                   "(entity, id) rollback('cannot delete') end\"}") == 200);
    CHECK(http_req("PUT", "/data/app/people/protected/p1", "root", PW,
                   "{\"x\":1}") == 200);
    CHECK(http_request("DELETE", "/data/app/people/protected/p1", "root",
                       PW, NULL, &body) == 403);
    CHECK(strstr(body, "cannot delete") != NULL);

    /* ---- afterInsert fires exactly once per insert (counter) ---- */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"partition\":\"people\",\"keyspace\":\"employees\","
                   "\"event\":\"afterInsert\","
                   "\"code\":\"function app_people_employees_afterInsert"
                   "(entity, id) local a = get('people', 'audit', 'hits') "
                   "or {} a.count = (a.count or 0) + 1 put('people', "
                   "'audit', 'hits', a) end\"}") == 200);
    CHECK(http_req("PUT", "/data/app/people/employees/s3", "root", PW,
                   "{\"name\":\"Zed\"}") == 200);
    CHECK(http_request("GET", "/data/app/people/audit/hits", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"count\":1") != NULL);
    /* an update does not fire afterInsert */
    CHECK(http_req("PUT", "/data/app/people/employees/s3", "root", PW,
                   "{\"name\":\"Zed\"}") == 200);
    CHECK(http_request("GET", "/data/app/people/audit/hits", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"count\":1") != NULL);

    /* ---- afterDelete sees the deleted record ---- */
    CHECK(http_req("POST", "/admin/code", "root", PW,
                   "{\"type\":\"action\",\"database\":\"app\","
                   "\"partition\":\"people\",\"keyspace\":\"employees\","
                   "\"event\":\"afterDelete\","
                   "\"code\":\"function app_people_employees_afterDelete"
                   "(entity, id) put('people', 'audit', 'del', { key = id, "
                   "name = entity.name }) end\"}") == 200);
    CHECK(http_req("PUT", "/data/app/people/employees/s4", "root", PW,
                   "{\"name\":\"gone\"}") == 200);
    CHECK(http_request("GET", "/data/app/people/audit/hits", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"count\":2") != NULL);
    CHECK(http_req("DELETE", "/data/app/people/employees/s4", "root", PW,
                   NULL) == 200);
    CHECK(http_request("GET", "/data/app/people/audit/del", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"key\":\"s4\"") != NULL);
    CHECK(strstr(body, "\"name\":\"gone\"") != NULL);

    /* ---- EQL write-back fires the matching after_* events ---- */
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"INSERT INTO app.people.employees "
                       "(id, name) VALUES ('e1', 'Eve')\"}", &body) == 200);
    CHECK(http_request("GET", "/data/app/people/audit/hits", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"count\":3") != NULL);
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"UPDATE app.people.employees SET name = "
                       "'eve2' WHERE id = 'e1'\"}", &body) == 200);
    CHECK(http_request("GET", "/data/app/people/employees/e1", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"name\":\"eve2\"") != NULL);
    CHECK(http_request("POST", "/eql", "root", PW,
                       "{\"sql\":\"DELETE FROM app.people.employees WHERE "
                       "id = 'e1'\"}", &body) == 200);
    CHECK(http_request("GET", "/data/app/people/audit/del", "root", PW,
                       NULL, &body) == 200);
    CHECK(strstr(body, "\"key\":\"e1\"") != NULL);
    CHECK(strstr(body, "\"name\":\"eve2\"") != NULL);

    /* ---- fetch + delete single records ---- */
    CHECK(http_request("GET", "/admin/code/generateId", "root", PW, NULL,
                       &body) == 200);
    CHECK(strstr(body, "\"type\":\"function\"") != NULL);
    CHECK(http_req("GET", "/admin/code/nosuch", "root", PW, NULL) == 404);
    CHECK(http_req("DELETE",
                   "/admin/code/app_people_employees_beforeUpdate",
                   "root", PW, NULL) == 200);
    CHECK(http_req("DELETE",
                   "/admin/code/app_people_employees_beforeUpdate",
                   "root", PW, NULL) == 404);
    CHECK(http_request("GET", "/admin/code?type=action", "root", PW, NULL,
                       &body) == 200);
    CHECK(strstr(body, "app_people_employees_beforeUpdate") == NULL);
    CHECK(strstr(body, "generateId") == NULL);
    CHECK(http_request("GET", "/admin/code?type=function", "root", PW, NULL,
                       &body) == 200);
    CHECK(strstr(body, "generateId") != NULL);
    CHECK(http_request("GET", "/admin/code?type=wat", "root", PW, NULL,
                       &body) == 400);

    /* ---- non-admin users are refused ---- */
    CHECK(http_req("POST", "/admin/users", "root", PW,
                   "{\"name\":\"viewer\",\"groups\":2,\"password\":\"" PW
                   "\"}") == 201);
    CHECK(http_req("GET", "/admin/code", "viewer", PW, NULL) == 403);

    printf("%d checks, %d failures\n", tests_run, tests_failed);
    return tests_failed == 0 ? 0 : 1;
}
