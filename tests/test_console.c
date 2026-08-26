/* Integration tests for the embedded admin console, first-time setup, and
 * password sessions. Spawned by tests/test_console_run.sh. */

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

static int http_request(const char *method, const char *path,
                        const char *auth, const char *body, char **body_out)
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
    if (auth) {
        snprintf(auth_hdr, sizeof(auth_hdr),
                 "Authorization: Bearer %s\r\n", auth);
    }
    int n = snprintf(head, sizeof(head),
                     "%s %s HTTP/1.1\r\n"
                     "Host: localhost\r\n"
                     "%s"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     method, path, auth_hdr, body ? strlen(body) : 0);
    size_t sent = 0;
    while (sent < (size_t)n) {
        ssize_t w = send(fd, head + sent, (size_t)n - sent, 0);
        if (w <= 0) {
            close(fd);
            return -1;
        }
        sent += (size_t)w;
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

static bool body_has(const char *hay, const char *needle)
{
    return hay && strstr(hay, needle) != NULL;
}

/* Extracts "token":"<value>" from a JSON response body. */
static void extract_token(const char *body, char out[128])
{
    out[0] = '\0';
    const char *k = strstr(body, "\"token\"");
    if (!k) {
        return;
    }
    const char *colon = strchr(k, ':');
    if (!colon) {
        return;
    }
    const char *v = strchr(colon, '"');
    if (!v) {
        return;
    }
    v++;
    const char *end = strchr(v, '"');
    if (!end) {
        return;
    }
    size_t len = (size_t)(end - v);
    if (len >= 128) {
        len = 127;
    }
    memcpy(out, v, len);
    out[len] = '\0';
}

int main(int argc, char **argv)
{
    if (argc < 2) {
        fprintf(stderr, "usage: %s <port>\n", argv[0]);
        return 2;
    }
    g_port = atoi(argv[1]);

    bool up = false;
    for (int i = 0; i < 50 && !up; i++) {
        edb_sleep_us(100000);
        up = http_request("GET", "/status", NULL, NULL, NULL) == 200;
    }
    CHECK(up);

    /* embedded console is served from the binary */
    char *body = NULL;
    int s = http_request("GET", "/admin", NULL, NULL, &body);
    CHECK(s == 200);
    CHECK(body_has(body, "<html") && body_has(body, "EpsilonDB Admin"));

    /* first-time state: no admin yet */
    s = http_request("GET", "/admin/console/state", NULL, NULL, &body);
    CHECK(s == 200 && body_has(body, "\"setup_required\":true"));

    /* setup creates the admin user and returns a session token */
    char token[128];
    s = http_request("POST", "/admin/setup", NULL,
                     "{\"username\":\"admin\",\"password\":\"secret123\"}",
                     &body);
    CHECK(s == 200);
    extract_token(body, token);
    CHECK(token[0] != '\0');

    /* once configured, setup is no longer required and we are authenticated */
    s = http_request("GET", "/admin/console/state", token, NULL, &body);
    CHECK(s == 200 && body_has(body, "\"setup_required\":false"));
    CHECK(body_has(body, "\"authenticated\":true"));

    /* a second setup attempt is refused */
    s = http_request("POST", "/admin/setup", NULL,
                     "{\"username\":\"root\",\"password\":\"other123\"}",
                     &body);
    CHECK(s == 409);

    /* wrong password rejected, correct password accepted */
    s = http_request("POST", "/admin/login", NULL,
                     "{\"username\":\"admin\",\"password\":\"wrong\"}", &body);
    CHECK(s == 401);
    s = http_request("POST", "/admin/login", NULL,
                     "{\"username\":\"admin\",\"password\":\"secret123\"}",
                     &body);
    CHECK(s == 200);
    char token2[128];
    extract_token(body, token2);
    CHECK(token2[0] != '\0' && strcmp(token2, token) != 0);

    /* the session token authorizes API calls */
    s = http_request("GET", "/admin/databases", token, NULL, &body);
    CHECK(s == 200);

    /* first run seeds the demo company database with sample data */
    CHECK(body_has(body, "demo"));
    s = http_request("GET", "/data/demo/People/employees/all", token, NULL,
                     &body);
    CHECK(s == 200 && body_has(body, "Alice Johnson"));
    s = http_request("POST", "/data/demo/Departments/depts/query", token,
                     "{\"filters\":[{\"key\":\"headcount\",\"operator\":\"gte\",\"value\":20}]}",
                     &body);
    CHECK(s == 200 && body_has(body, "Engineering"));

    /* keyspace-less query spans the whole partition */
    s = http_request("POST", "/data/demo/People/query", token,
                     "{\"filters\":[{\"key\":\"salary\",\"operator\":\"gte\",\"value\":90000}]}",
                     &body);
    CHECK(s == 200 && body_has(body, "Alice Johnson") &&
          body_has(body, "Eve Davis"));

    /* first run created the default SysAdmins group and the admin user */
    s = http_request("GET", "/admin/groups", token, NULL, &body);
    CHECK(s == 200 && body_has(body, "SysAdmins"));
    s = http_request("GET", "/admin/users", token, NULL, &body);
    CHECK(s == 200 && body_has(body, "admin"));

    /* logout invalidates the token */
    s = http_request("POST", "/admin/logout", token, NULL, &body);
    CHECK(s == 200);
    s = http_request("GET", "/admin/databases", token, NULL, &body);
    CHECK(s == 401);

    printf("%d tests, %d failures\n", tests_run, tests_failed);
    return tests_failed ? 1 : 0;
}
