/* eql.c - Epsilon Query Language console.
 *
 * Thin REPL front-end for the EQL engine exposed by epsilond:
 *   - connects like epsilonctl: local admin socket by default (-s to
 *     override, trusted/full rights) or TCP with -h/-p/-u/-P
 *   - reads one statement per line (quotes and trailing semicolons are
 *     handled), sends it to POST /eql or POST /admin/eql
 *   - renders SELECT results as the same aligned colour-coded table the
 *     epsilonctl TUI uses, and DML results as status lines
 *
 * Dot-commands: .help .quit .exit .raw   plus any SQL statement.
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <errno.h>
#include <strings.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <unistd.h>

#include "../vendor/cjson/cJSON.h"

#define DEFAULT_ADMIN_SOCK "epsilon-admin.sock"
#define EQL_DEFAULT_PORT 8123

static const char *g_host = NULL;
static int g_port = EQL_DEFAULT_PORT;
static const char *g_sockpath = DEFAULT_ADMIN_SOCK;
static const char *g_user = NULL;
static char g_password[1024] = "";
static bool g_raw = false;

/* ------------------------------------------------------------------ */
/* transport                                                            */

static int request_path(const char *path, const char *body, char **out)
{
    int fd;
    if (!g_host) {
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            return -1;
        }
        struct sockaddr_un addr = {0};
        addr.sun_family = AF_UNIX;
        snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", g_sockpath);
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            fprintf(stderr,
                    "eql: cannot connect to admin socket '%s'\n"
                    "(is epsilond running here? use -h/-p for a remote "
                    "node)\n",
                    g_sockpath);
            close(fd);
            return -1;
        }
    } else {
        fd = socket(AF_INET, SOCK_STREAM, 0);
        if (fd < 0) {
            return -1;
        }
        struct sockaddr_in addr = {0};
        addr.sin_family = AF_INET;
        addr.sin_port = htons((uint16_t)g_port);
        if (inet_pton(AF_INET, g_host, &addr.sin_addr) != 1) {
            fprintf(stderr, "eql: invalid host '%s'\n", g_host);
            close(fd);
            return -1;
        }
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            fprintf(stderr, "eql: cannot connect to %s:%d\n", g_host,
                    g_port);
            close(fd);
            return -1;
        }
    }

    char auth_hdr[600] = "";
    char pw_hdr[600] = "";
    if (g_user && *g_user) {
        snprintf(auth_hdr, sizeof(auth_hdr),
                 "Authorization: Bearer %s\r\n", g_user);
    }
    if (g_password[0]) {
        snprintf(pw_hdr, sizeof(pw_hdr),
                 "X-Epsilon-Password: %s\r\n", g_password);
    }

    int n = snprintf(NULL, 0,
                     "POST %s HTTP/1.1\r\n"
                     "Host: %s:%d\r\n"
                     "%s%s"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     path, g_host ? g_host : "local", g_port, auth_hdr,
                     pw_hdr, body ? strlen(body) : 0);
    char *head = malloc((size_t)n + 1);
    if (!head) {
        close(fd);
        return -1;
    }
    snprintf(head, (size_t)n + 1,
             "POST %s HTTP/1.1\r\n"
             "Host: %s:%d\r\n"
             "%s%s"
             "Content-Length: %zu\r\n"
             "Connection: close\r\n"
             "\r\n",
             path, g_host ? g_host : "local", g_port, auth_hdr, pw_hdr,
             body ? strlen(body) : 0);

    size_t sent = 0;
    while (sent < (size_t)n) {
        ssize_t w = send(fd, head + sent, (size_t)n - sent, 0);
        if (w < 0 && errno == EINTR) {
            continue;
        }
        if (w <= 0) {
            free(head);
            close(fd);
            return -1;
        }
        sent += w;
    }
    free(head);
    if (body && *body) {
        size_t blen = strlen(body);
        size_t bsent = 0;
        while (bsent < blen) {
            ssize_t w = send(fd, body + bsent, blen - bsent, 0);
            if (w < 0 && errno == EINTR) {
                continue;
            }
            if (w <= 0) {
                close(fd);
                return -1;
            }
            bsent += w;
        }
    }

    size_t cap = 1024 * 1024;
    char *resp = malloc(cap);
    if (!resp) {
        close(fd);
        return -1;
    }
    size_t total = 0;
    for (;;) {
        if (total >= cap - 1) break;
        ssize_t r = recv(fd, resp + total, cap - 1 - total, 0);
        if (r < 0 && errno == EINTR) {
            continue;
        }
        if (r <= 0) break;
        total += (size_t)r;
    }
    close(fd);
    resp[total] = '\0';

    char *hdr_end = strstr(resp, "\r\n\r\n");
    if (!hdr_end) {
        free(resp);
        return -1;
    }
    int status = 0;
    sscanf(resp, "HTTP/1.%*d %d", &status);
    *out = strdup(hdr_end + 4);
    free(resp);
    return status;
}


/* ------------------------------------------------------------------ */
/* rendering (mirrors epsilonctl's TUI table style)                     */

#define MAX_COLS 62
#define T_NUM "\033[36m"     /* cyan */
#define T_STR "\033[33m"     /* yellow */
#define T_TRUE "\033[32m"    /* green */
#define T_FALSE "\033[31m"   /* red */
#define T_NULL "\033[2m"     /* dim */
#define T_CONT "\033[35m"    /* magenta */
#define RST "\033[0m"

static void s_append(char *line, size_t cap, const char *text)
{
    size_t len = strlen(line);
    if (len + 1 >= cap) {
        return;
    }
    snprintf(line + len, cap - len, "%s", text);
}

static void scalar_text_depth(const cJSON *v, char *buf, size_t cap,
                              const char **colour, int depth)
{
    *colour = "";
    if (!v || cJSON_IsNull(v)) {
        *colour = T_NULL;
        snprintf(buf, cap, "null");
    } else if (cJSON_IsBool(v)) {
        *colour = cJSON_IsTrue(v) ? T_TRUE : T_FALSE;
        snprintf(buf, cap, cJSON_IsTrue(v) ? "true" : "false");
    } else if (cJSON_IsNumber(v)) {
        *colour = T_NUM;
        if (v->valuedouble == (double)v->valueint) {
            snprintf(buf, cap, "%d", v->valueint);
        } else {
            snprintf(buf, cap, "%g", v->valuedouble);
        }
    } else if (cJSON_IsString(v)) {
        *colour = T_STR;
        snprintf(buf, cap, "%s", v->valuestring ? v->valuestring : "");
    } else if (cJSON_IsArray(v) && depth < 4) {
        *colour = T_CONT;
        buf[0] = '\0';
        const cJSON *el = NULL;
        bool first = true;
        cJSON_ArrayForEach(el, v) {
            char cell[160];
            const char *c2;
            scalar_text_depth(el, cell, sizeof(cell), &c2, depth + 1);
            if (!first) {
                s_append(buf, cap, ", ");
            }
            s_append(buf, cap, cell);
            first = false;
        }
        if (buf[0] == '\0') {
            snprintf(buf, cap, "(empty)");
        }
    } else {
        char *printed = cJSON_PrintUnformatted(v);
        *colour = T_CONT;
        snprintf(buf, cap, "%s", printed ? printed : "?");
        free(printed);
    }
}

static bool use_colour(void)
{
    static int cached = -1;
    if (cached < 0) {
        cached = isatty(fileno(stdout)) ? 1 : 0;
    }
    return cached == 1;
}

/* Renders the engine response: {"columns":[..],"rows":[[..]]} tables and
 * {"op":...,"applied":[...]} status documents. Text falls through. */
static void render_result(const char *json, int status)
{
    cJSON *parsed = cJSON_Parse(json);
    if (!parsed || g_raw) {
        printf("%s\n", json ? json : "");
        cJSON_Delete(parsed);
        return;
    }

    if (status != 200) {
        const cJSON *msg =
            cJSON_GetObjectItemCaseSensitive(parsed, "message");
        const cJSON *err =
            cJSON_GetObjectItemCaseSensitive(parsed, "error");
        printf("%serror%s: %s\n", "\033[31m", RST,
               msg && cJSON_IsString(msg)
                   ? msg->valuestring
                   : (err && cJSON_IsString(err) ? err->valuestring : "?"));
        cJSON_Delete(parsed);
        return;
    }

    /* result set */
    const cJSON *cols = cJSON_GetObjectItemCaseSensitive(parsed, "columns");
    const cJSON *rows = cJSON_GetObjectItemCaseSensitive(parsed, "rows");
    if (cJSON_IsArray(cols) && cJSON_IsArray(rows)) {
        size_t widths[MAX_COLS];
        const char *names[MAX_COLS];
        int ncols = 0;
        const cJSON *c = NULL;
        cJSON_ArrayForEach(c, cols) {
            if (ncols >= MAX_COLS) break;
            names[ncols] = cJSON_IsString(c) && c->valuestring
                               ? c->valuestring
                               : "";
            size_t name_len = strlen(names[ncols]);
            widths[ncols] = name_len;
            ncols++;
        }
        const cJSON *row = NULL;
        cJSON_ArrayForEach(row, rows) {
            for (int i = 0; i < ncols; i++) {
                char cell[512];
                const char *colour;
                scalar_text_depth(
                    cJSON_GetArrayItem(row, i), cell, sizeof(cell),
                    &colour, 0);
                size_t len = strlen(cell);
                if (len > widths[i]) {
                    widths[i] = len;
                }
            }
        }

        bool colour = use_colour();
        char line[8192];
        line[0] = '\0';
        for (int i = 0; i < ncols; i++) {
            char cell[300];
            snprintf(cell, sizeof(cell), "%-*s", (int)widths[i],
                     names[i]);
            s_append(line, sizeof(line), cell);
            if (i + 1 < ncols) {
                s_append(line, sizeof(line), "  ");
            }
        }
        puts(line);
        line[0] = '\0';
        for (int i = 0; i < ncols; i++) {
            char dashes[300];
            memset(dashes, '-', widths[i]);
            dashes[widths[i]] = '\0';
            s_append(line, sizeof(line), dashes);
            if (i + 1 < ncols) {
                s_append(line, sizeof(line), "  ");
            }
        }
        puts(line);

        cJSON_ArrayForEach(row, rows) {
            line[0] = '\0';
            for (int i = 0; i < ncols; i++) {
                char cell[512];
                const char *c2;
                scalar_text_depth(cJSON_GetArrayItem(row, i), cell,
                                  sizeof(cell), &c2, 0);
                char piece[600];
                snprintf(piece, sizeof(piece), "%s%s%s",
                         colour ? c2 : "", cell, colour ? RST : "");
                s_append(line, sizeof(line), piece);
                size_t cur = strlen(cell);
                while (cur++ < widths[i]) {
                    s_append(line, sizeof(line), " ");
                }
                if (i + 1 < ncols) {
                    s_append(line, sizeof(line), "  ");
                }
            }
            puts(line);
        }
        size_t nrows = (size_t)cJSON_GetArraySize(rows);
        printf("(%zu row%s)\n", nrows, nrows == 1 ? "" : "s");
        cJSON_Delete(parsed);
        return;
    }

    /* DML / status document */
    const cJSON *count = cJSON_GetObjectItemCaseSensitive(parsed, "count");
    const cJSON *op = cJSON_GetObjectItemCaseSensitive(parsed, "op");
    const cJSON *applied =
        cJSON_GetObjectItemCaseSensitive(parsed, "applied");
    const cJSON *failed =
        cJSON_GetObjectItemCaseSensitive(parsed, "failed");

    size_t nfailed = failed && cJSON_IsArray(failed)
                         ? (size_t)cJSON_GetArraySize(failed)
                         : 0;
    long total = count && cJSON_IsNumber(count) ? count->valueint : -1;

    if (op && cJSON_IsString(op)) {
        const char *opname =
            op->valuestring ? op->valuestring : "?";
        const char *past = strcmp(opname, "delete") == 0 ? "deleted"
                           : strcmp(opname, "update") == 0 ? "updated"
                           : strcmp(opname, "insert") == 0 ? "inserted"
                           : opname;
        printf("%ld row%s %s", total, total == 1 ? "" : "s", past);
        if (nfailed > 0) {
            printf(" (%zu failed)", nfailed);
        }
        printf("\n");
    } else {
        char *printed = cJSON_PrintUnformatted(parsed);
        printf("%s\n", printed ? printed : "");
        free(printed);
    }
(void)applied;
    cJSON_Delete(parsed);
}

/* ------------------------------------------------------------------ */
/* statement input                                                      */

/* Strips SQL comments outside quotes, trims, removes one trailing ';'. */
static void normalize_statement(const char *in, char *out, size_t cap)
{
    size_t o = 0;
    size_t i = 0;
    size_t len = strlen(in);
    while (i < len && o + 1 < cap) {
        char c = in[i];
        if (c == '\'') {
            out[o++] = c;
            i++;
            while (i < len && o + 1 < cap) {
                out[o++] = in[i];
                if (in[i] == '\'' && in[i - 1] == '\\') {
                    i++;
                    continue;
                }
                if (in[i] == '\'') {
                    i++;
                    break;
                }
                i++;
            }
            continue;
        }
        if (c == '-' && i + 1 < len && in[i + 1] == '-') {
            while (i < len && in[i] != '\n') {
                i++;
            }
            continue;
        }
        if (c == '/' && i + 1 < len && in[i + 1] == '*') {
            const char *end = strstr(in + i + 2, "*/");
            i = end ? (size_t)(end - in) + 2 : len;
            continue;
        }
        out[o++] = c;
        i++;
    }
    out[o] = '\0';
    /* right-trim whitespace/semicolons */
    size_t t = strlen(out);
    while (t > 0 && (out[t - 1] == ';' || out[t - 1] == ' ' ||
                     out[t - 1] == '\t' || out[t - 1] == '\n' ||
                     out[t - 1] == '\r')) {
        t--;
    }
    out[t] = '\0';
    /* left-trim */
    size_t off = strspn(out, " \t\n\r");
    if (off > 0) {
        memmove(out, out + off, t - off + 1);
    }
}

static void print_help(void)
{
    printf("EQL console commands:\n");
    printf("  <sql>                 run any single SQL-like statement\n");
    printf("  help                  the same help, without the dot\n");
    printf("  .help                 this help\n");
    printf("  .raw                  toggle raw JSON output\n");
    printf("  .quit | .exit | exit  leave the console\n");
    printf("\n");
    printf("References are Database.Partition.Keyspace. Three parts read one\n");
    printf("table; two parts (db.partition) merge every keyspace in the\n");
    printf("partition into one table.\n");
    printf("\n");
    printf("Examples:\n");
    printf("  -- one keyspace\n");
    printf("  SELECT * FROM demo.people.employees;\n");
    printf("  SELECT id, name FROM demo.people.employees WHERE age > 30 ORDER BY name;\n");
    printf("  SELECT manager, COUNT(*) AS n, AVG(age) AS avg_age\n");
    printf("    FROM demo.people.employees GROUP BY manager ORDER BY n DESC;\n");
    printf("  SELECT e.name, m.dept FROM demo.people.employees e\n");
    printf("    JOIN demo.people.managers m ON e.manager = m.id;\n");
    printf("\n");
    printf("  -- partition-wide: employees + managers + contractors in one query\n");
    printf("  SELECT * FROM demo.people;\n");
    printf("  SELECT COUNT(*) FROM demo.people WHERE department IS NOT NULL;\n");
    printf("  UPDATE demo.people SET active = false WHERE id = 'e1003';\n");
    printf("  DELETE FROM demo.people WHERE id = 'con003';\n");
    printf("\n");
    printf("  -- cross-partition joins\n");
    printf("  SELECT p.title, m.dept FROM demo.ops.projects p\n");
    printf("    JOIN demo.people.managers m ON p.lead = m.id WHERE p.active;\n");
    printf("\n");
    printf("  -- schema-assigning writes: new keys are created on the record\n");
    printf("  UPDATE demo.people.employees SET score = 7 WHERE id = 'e1001';\n");
    printf("  INSERT INTO demo.people.employees (id, name, age)\n");
    printf("    VALUES ('e2001', 'Zoe', 28);\n");
    printf("  -- removing a key: SET the key to NULL\n");
    printf("  UPDATE demo.people.employees SET score = NULL WHERE id = 'e1001';\n");
}

static void print_banner(void)
{
    printf(
        "  ______ _____ _                    _     _ \n"
        " |  ____/ ____| |        /\\        | |   | |\n"
        " | |__ | |    | |       /  \\   __ _| |_ _| |\n"
        " |  __|| |    | |      / /\\ \\ / _` | |/ _` |\n"
        " | |___| |____| |____ / ____ \\ (_| | | (_| |\n"
        " |______\\_____|______/_/    \\_\\__,_|_|\\__,_|\n"
        "\n"
        " Epsilon Query Language console. Type 'help' for examples,\n"
        " '.quit' or 'exit' to leave.\n\n");
}

/* ------------------------------------------------------------------ */

int main(int argc, char **argv)
{
    int argi = 1;
    while (argi < argc && argv[argi][0] == '-' &&
           strcmp(argv[argi], "-") != 0) {
        if ((strcmp(argv[argi], "-s") == 0 ||
             strcmp(argv[argi], "--socket") == 0) && argi + 1 < argc) {
            g_sockpath = argv[++argi];
        } else if ((strcmp(argv[argi], "-h") == 0 ||
                    strcmp(argv[argi], "--host") == 0) && argi + 1 < argc) {
            g_host = argv[++argi];
        } else if ((strcmp(argv[argi], "-p") == 0 ||
                    strcmp(argv[argi], "--port") == 0) &&
                   argi + 1 < argc) {
            g_port = atoi(argv[++argi]);
            g_host = g_host ? g_host : "127.0.0.1";
        } else if ((strcmp(argv[argi], "-u") == 0 ||
                    strcmp(argv[argi], "--user") == 0) &&
                   argi + 1 < argc) {
            g_user = argv[++argi];
        } else if ((strcmp(argv[argi], "-P") == 0 ||
                    strcmp(argv[argi], "--password") == 0) &&
                   argi + 1 < argc) {
            snprintf(g_password, sizeof(g_password), "%s", argv[++argi]);
        } else if ((strcmp(argv[argi], "-r") == 0 ||
                    strcmp(argv[argi], "--raw") == 0)) {
            g_raw = true;
        } else {
            fprintf(stderr,
                    "usage: eql [-s socket] [-r] |\n"
                    "           [-h host -p port -u user [-P password] [-r]]\n");
            return 1;
        }
        argi++;
    }

    /* optional command words from argv after options (one-shot mode) */
    char oneshot[65536];
    oneshot[0] = '\0';
    if (argi < argc) {
        for (; argi < argc; argi++) {
            if (oneshot[0]) {
                strncat(oneshot, " ",
                        sizeof(oneshot) - strlen(oneshot) - 1);
            }
            strncat(oneshot, argv[argi],
                    sizeof(oneshot) - strlen(oneshot) - 1);
        }
    }

    char stmt[65536];
    char body[131072];
    char *input = oneshot;
    bool interactive = isatty(fileno(stdin));
    if (interactive && !oneshot[0]) {
        print_banner();
    }

    for (;;) {
        if (interactive && !input[0]) {
            printf("eql> ");
            fflush(stdout);
        }
        if (!input[0]) {
            char rawline[65536];
            if (!fgets(rawline, sizeof(rawline), stdin)) {
                if (interactive) {
                    printf("\n");
                }
                break;
            }
            normalize_statement(rawline, stmt, sizeof(stmt));
        } else {
            normalize_statement(input, stmt, sizeof(stmt));
            input = "";   /* consumed: fall back to stdin afterwards */
        }
        if (stmt[0] == '\0') {
            continue;
        }
        if (strcasecmp(stmt, ".quit") == 0 || strcasecmp(stmt, ".exit") == 0 ||
            strcasecmp(stmt, "exit") == 0 || strcasecmp(stmt, "quit") == 0) {
            break;
        }
        if (strcasecmp(stmt, ".help") == 0 || strcasecmp(stmt, "help") == 0) {
            print_help();
            continue;
        }
        if (strcasecmp(stmt, ".raw") == 0) {
            g_raw = !g_raw;
            printf("raw output: %s\n", g_raw ? "on" : "off");
            continue;
        }

        /* safe serialization of the statement into the envelope */
        {
            cJSON *env = cJSON_CreateObject();
            if (!env) {
                fprintf(stderr, "eql: out of memory\n");
                return 1;
            }
            cJSON *sitem = cJSON_CreateString(stmt);
            if (!sitem) {
                cJSON_Delete(env);
                fprintf(stderr, "eql: out of memory\n");
                return 1;
            }
            cJSON_AddItemToObject(env, "sql", sitem);
            char *text = cJSON_PrintUnformatted(env);
            cJSON_Delete(env);
            if (!text) {
                fprintf(stderr, "eql: out of memory\n");
                return 1;
            }
            snprintf(body, sizeof(body), "%s", text);
            free(text);
        }

        char *result = NULL;
        int status = request_path("/eql", body, &result);
        if (status < 0) {
            /* admin socket may refuse /eql-less alias? both routes exist;
             * retry once against the admin alias for older servers */
            status = request_path("/admin/eql", body, &result);
        }
        if (status < 0) {
            fprintf(stderr, "eql: request failed\n");
        } else {
            render_result(result ? result : "", status);
        }
        free(result);

        if (input == NULL) {
            break;   /* unreachable guard: keeps -Wall quiet about input */
        }
    }
    return 0;
}
