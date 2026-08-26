/* epsilonbench - standalone performance benchmark client for EpsilonDB.
 *
 * Runs the server-side workload benchmark (POST /admin/benchmark) against a
 * running epsilond and renders the report. Talks to the local admin socket
 * by default (trusted, no auth); use -h/-p/-u for a remote node.
 *
 * Usage:
 *   epsilonbench [-s socket] [-h host -p port -u user] [--json]
 *                [records] [replication_factor] [cache_size]
 *                [journal_mode] [threads]
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <errno.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "../vendor/cjson/cJSON.h"
#include "epsilon_banner.h"

#define DEFAULT_ADMIN_SOCK "epsilon-admin.sock"

static const char *g_host = NULL;
static int g_port = 8123;
static const char *g_user = "";
static const char *g_sockpath = DEFAULT_ADMIN_SOCK;
static bool g_json = false;
static bool g_quiet = false;

/* ------------------------------------------------------------------ */
/* HTTP plumbing                                                       */

static int send_all(int fd, const void *data, size_t len)
{
    const char *p = data;
    while (len > 0) {
        ssize_t n = send(fd, p, len, 0);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return -1;
        }
        p += n;
        len -= (size_t)n;
    }
    return 0;
}

static char *read_all(int fd, size_t *len_out)
{
    size_t cap = 65536, len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        return NULL;
    }
    for (;;) {
        if (cap - len < 2) {
            cap *= 2;
            char *grown = realloc(buf, cap);
            if (!grown) {
                free(buf);
                return NULL;
            }
            buf = grown;
        }
        ssize_t n = read(fd, buf + len, cap - len - 1);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            free(buf);
            return NULL;
        }
        if (n == 0) {
            break;
        }
        len += (size_t)n;
    }
    buf[len] = '\0';
    if (len_out) {
        *len_out = len;
    }
    return buf;
}

/* Returns malloc'd body or NULL on connection failure. */
static char *http_request_raw(const char *method, const char *path,
                              const char *body, size_t body_len,
                              size_t *resp_len_out, int *status_out)
{
    int fd;
    if (!g_host) {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            return NULL;
        }
        struct sockaddr_un addr = {0};
        addr.sun_family = AF_UNIX;
        snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", g_sockpath);
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            if (!g_quiet) {
                fprintf(stderr,
                        "epsilonbench: cannot connect to admin socket '%s'\n"
                        "(is epsilond running here? use -h/-p for a remote "
                        "node)\n", g_sockpath);
            }
            close(fd);
            return NULL;
        }
    } else {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return NULL;
        }
        struct sockaddr_in addr = {0};
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)g_port);
        if (inet_pton(AF_INET, g_host, &addr.sin_addr) != 1) {
            if (!g_quiet) {
                fprintf(stderr, "epsilonbench: invalid host '%s'\n", g_host);
            }
            close(fd);
            return NULL;
        }
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            if (!g_quiet) {
                fprintf(stderr, "epsilonbench: cannot connect to %s:%d\n",
                        g_host, g_port);
            }
            close(fd);
            return NULL;
        }
    }

    int n = snprintf(NULL, 0,
                     "%s %s HTTP/1.1\r\n"
                     "Host: %s:%d\r\n"
                     "Authorization: Bearer %s\r\n"
                     "Content-Type: application/json\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     method, path, g_host ? g_host : "local", g_port, g_user,
                     body ? body_len : 0);
    if (n < 0) {
        close(fd);
        return NULL;
    }
    char *head = malloc((size_t)n + 1);
    if (!head) {
        close(fd);
        return NULL;
    }
    snprintf(head, (size_t)n + 1,
             "%s %s HTTP/1.1\r\n"
             "Host: %s:%d\r\n"
             "Authorization: Bearer %s\r\n"
             "Content-Type: application/json\r\n"
             "Content-Length: %zu\r\n"
             "Connection: close\r\n"
             "\r\n",
             method, path, g_host ? g_host : "local", g_port, g_user,
             body ? body_len : 0);
    if (send_all(fd, head, (size_t)n) != 0) {
        free(head);
        close(fd);
        return NULL;
    }
    free(head);
    if (body && body_len > 0 && send_all(fd, body, body_len) != 0) {
        close(fd);
        return NULL;
    }

    /* read the status line and headers */
    char line[1024];
    size_t llen = 0;
    int status = 0;
    size_t content_length = 0;
    bool header_done = false;
    while (!header_done) {
        char c;
        ssize_t r = read(fd, &c, 1);
        if (r != 1) {
            close(fd);
            return NULL;
        }
        if (c == '\n') {
            if (llen > 0 && line[llen - 1] == '\r') {
                line[llen - 1] = '\0';
            } else {
                line[llen] = '\0';
            }
            llen = 0;
            if (line[0] == '\0') {
                header_done = true;
                break;
            }
            if (strncmp(line, "HTTP/", 5) == 0) {
                status = atoi(line + 9);
            } else if (strncasecmp(line, "Content-Length:", 15) == 0) {
                content_length = strtoull(line + 15, NULL, 10);
            }
        } else {
            if (llen < sizeof(line) - 1) {
                line[llen++] = c;
            }
        }
    }
    if (status_out) {
        *status_out = status;
    }

    char *body_buf = NULL;
    if (content_length > 0) {
        body_buf = malloc(content_length + 1);
        if (!body_buf) {
            close(fd);
            return NULL;
        }
        size_t got = 0;
        while (got < content_length) {
            ssize_t r = read(fd, body_buf + got, content_length - got);
            if (r <= 0) {
                if (r < 0 && errno == EINTR) {
                    continue;
                }
                break;
            }
            got += (size_t)r;
        }
        body_buf[got] = '\0';
        if (resp_len_out) {
            *resp_len_out = got;
        }
    } else {
        body_buf = read_all(fd, resp_len_out);
    }
    close(fd);
    return body_buf;
}

static cJSON *http_json(const char *method, const char *path,
                        const char *body, int *status_out)
{
    char *resp = http_request_raw(method, path, body, body ? strlen(body) : 0,
                                  NULL, status_out);
    if (!resp) {
        return NULL;
    }
    cJSON *json = cJSON_Parse(resp);
    free(resp);
    return json;
}

/* ------------------------------------------------------------------ */
/* report rendering                                                    */

static long long num(const cJSON *o, const char *key)
{
    const cJSON *v = o ? cJSON_GetObjectItemCaseSensitive(o, key) : NULL;
    return cJSON_IsNumber(v) ? (long long)v->valuedouble : 0;
}

static double dbl(const cJSON *o, const char *key)
{
    const cJSON *v = o ? cJSON_GetObjectItemCaseSensitive(o, key) : NULL;
    return cJSON_IsNumber(v) ? v->valuedouble : 0;
}

static const char *str(const cJSON *o, const char *key, char *buf,
                       size_t cap)
{
    const cJSON *v = o ? cJSON_GetObjectItemCaseSensitive(o, key) : NULL;
    if (cJSON_IsString(v) && v->valuestring) {
        return v->valuestring;
    }
    if (cJSON_IsNumber(v)) {
        /* integers print without a decimal point */
        if (v->valuedouble == (double)(long long)v->valuedouble) {
            snprintf(buf, cap, "%lld", (long long)v->valuedouble);
        } else {
            snprintf(buf, cap, "%.2f", v->valuedouble);
        }
        return buf;
    }
    return "";
}

static void print_row(const char *a, const char *b, const char *c,
                      const char *d)
{
    printf("  %-12s %14s %12s %14s\n", a, b, c, d);
}

static int render_report(const cJSON *report)
{
    const char *phases[] = { "writes", "gets", "queries", "updates",
                             "deletes" };
    char numbuf[32];

    printf("Benchmark on %s\n", str(report, "database", numbuf,
                                    sizeof(numbuf)));
    printf("  %-22s %s\n", "Partitions",
           str(report, "partitions", numbuf, sizeof(numbuf)));
    printf("  %-22s %s\n", "Records per partition",
           str(report, "records_per_partition", numbuf, sizeof(numbuf)));
    printf("  %-22s %s\n", "Total records",
           str(report, "total_records", numbuf, sizeof(numbuf)));
    printf("  %-22s %s\n", "Threads",
           str(report, "threads", numbuf, sizeof(numbuf)));
    printf("  %-22s %s\n", "Replication factor",
           str(report, "replication_factor", numbuf, sizeof(numbuf)));
    printf("  %-22s %s\n", "Cache size (KB)",
           str(report, "cache_size", numbuf, sizeof(numbuf)));
    printf("  %-22s %s\n", "Journal mode",
           str(report, "journal_mode", numbuf, sizeof(numbuf)));
    printf("\n");

    print_row("Operation", "Count", "Seconds", "Ops/sec");
    printf("  %s\n",
           "------------  --------------  ------------  --------------");
    for (size_t i = 0; i < sizeof(phases) / sizeof(phases[0]); i++) {
        const cJSON *ph = cJSON_GetObjectItemCaseSensitive(report,
                                                           phases[i]);
        if (!cJSON_IsObject(ph)) {
            continue;
        }
        double seconds = dbl(ph, "seconds");
        long long count = num(ph, "count");
        double rate = seconds > 0 ? (double)count / seconds : 0;
        char count_s[32], sec_s[32], rate_s[32];
        snprintf(count_s, sizeof(count_s), "%lld", count);
        snprintf(sec_s, sizeof(sec_s), "%.3f", seconds);
        snprintf(rate_s, sizeof(rate_s), "%.0f", rate);
        print_row(phases[i], count_s, sec_s, rate_s);
    }
    return 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */

static void print_usage(void)
{
    printf("usage: epsilonbench [-s socket] | [-h host -p port -u user] "
           "[--json]\n");
    printf("                      [records] [replication_factor] "
           "[cache_size]\n");
    printf("                      [journal_mode] [threads]\n");
    printf("  Runs the server-side workload benchmark against a running\n");
    printf("  epsilond and prints ops/sec per phase. Defaults to the local\n");
    printf("  admin socket; -h/-p/-u switch to a remote node. --json dumps\n");
    printf("  the raw report object.\n");
}

int main(int argc, char **argv)
{
    /* shared EpsilonDB banner */
    for (int i = 0; i < EDB_BANNER_LINES; i++) {
        printf("%s\n", edb_banner[i]);
    }
    printf("        performance benchmark client\n\n");

    int argi = 1;
    while (argi < argc && argv[argi][0] == '-' &&
           strcmp(argv[argi], "-") != 0) {
        if ((strcmp(argv[argi], "-s") == 0 ||
             strcmp(argv[argi], "--socket") == 0) && argi + 1 < argc) {
            g_sockpath = argv[argi + 1];
            argi += 2;
        } else if ((strcmp(argv[argi], "-h") == 0 ||
                    strcmp(argv[argi], "--host") == 0) && argi + 1 < argc) {
            g_host = argv[argi + 1];
            argi += 2;
        } else if ((strcmp(argv[argi], "-p") == 0 ||
                    strcmp(argv[argi], "--port") == 0) &&
                   argi + 1 < argc) {
            g_port = atoi(argv[argi + 1]);
            if (!g_host) {
                g_host = "127.0.0.1";
            }
            argi += 2;
        } else if ((strcmp(argv[argi], "-u") == 0 ||
                    strcmp(argv[argi], "--user") == 0) && argi + 1 < argc) {
            g_user = argv[argi + 1];
            argi += 2;
        } else if (strcmp(argv[argi], "--json") == 0) {
            g_json = true;
            argi++;
        } else if (strcmp(argv[argi], "-q") == 0) {
            g_quiet = true;
            argi++;
        } else if (strcmp(argv[argi], "-h") == 0 ||
                   strcmp(argv[argi], "--help") == 0) {
            print_usage();
            return 0;
        } else {
            print_usage();
            return 2;
        }
    }

    long records = argi < argc ? strtol(argv[argi], NULL, 10) : 100000;
    if (records < 1) {
        records = 100000;
    }
    long rf = argi + 1 < argc ? strtol(argv[argi + 1], NULL, 10) : 1;
    if (rf < 1) {
        rf = 1;
    }
    long cache = argi + 2 < argc ? strtol(argv[argi + 2], NULL, 10) : 0;
    const char *journal = argi + 3 < argc ? argv[argi + 3] : "TRUNCATE";
    long threads = argi + 4 < argc ? strtol(argv[argi + 4], NULL, 10) : 0;
    if (threads < 0) {
        threads = 0;
    }

    char body[512];
    snprintf(body, sizeof(body),
             "{\"records\":%ld,\"replication_factor\":%ld,"
             "\"cache_size\":%ld,\"journal_mode\":\"%s\","
             "\"threads\":%ld}",
             records, rf, cache, journal, threads);

    int status = 0;
    cJSON *report = http_json("POST", "/admin/benchmark", body, &status);
    if (!report) {
        return 1;
    }
    if (status >= 400) {
        const cJSON *err =
            cJSON_GetObjectItemCaseSensitive(report, "error");
        fprintf(stderr, "epsilonbench: benchmark failed (%s)\n",
                cJSON_IsString(err) && err->valuestring ? err->valuestring
                                                        : "HTTP error");
        cJSON_Delete(report);
        return 1;
    }

    if (g_json) {
        char *printed = cJSON_Print(report);
        if (printed) {
            puts(printed);
            free(printed);
        }
    } else {
        render_report(report);
    }
    cJSON_Delete(report);
    return 0;
}
