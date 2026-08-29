/* epsilonctl - EpsilonDB command line client.
 *
 * Connects to a node's HTTP endpoint and issues admin/data requests.
 *
 * Usage: epsilonctl [-s socket] | [-h host] [-p port] [-u user [-P password]]
 *        <command> [args...]
 *
 * By default epsilonctl connects to the server's local admin socket (no
 * authentication; treated as admin). Pass -h/-p to talk to a remote node's
 * HTTP port instead, in which case -u and a password (-P, or a prompt) are
 * required for privileged commands.
 *
 * Commands:
 *   status
 *   list databases|groups|users|partitions|settings
 *   create database <name> <replication_factor>
 *   delete database <name>
 *   create group <name>
 *   delete group <name>
 *   create user <name> <group_mask> [password]
 *   set user <name> <group_mask> [password]
 *   delete user <name>
 *   create partition <db> <name> <create_mask> <update_mask> <read_mask> <delete_mask>
 *   delete partition <db> <name>
 *   get <db>/<partition>/<keyspace>/<id>
 *   put <db>/<partition>/<keyspace>/<id> <json | -> [ttl_seconds]
 *   rm <db>/<partition>/<keyspace>/<id>
 *   query <db>/<partition>/<keyspace> [--filter <json>]...
 *   ids <db>/<partition>/<keyspace> [--filter <json>]...
 *   all <db>/<partition>/<keyspace> [--filter <json>]...
 *   get setting <name>
 *   set setting <name> <json | ->
 *   delete setting <name>
 *
 * A body argument of "-" reads JSON from stdin (CLI mode only).
 * Mask 0 = allow all groups.
 *
 * Running epsilonctl with no command opens a full-screen interactive
 * console (alternate screen, nano-style): ASCII banner with a live
 * server info panel, colour-coded tabular query output, a command bar
 * at the bottom with ^X/^L/^C/arrow-key editing and command history.
 */

#include <arpa/inet.h>
#include <netinet/in.h>
#include <stdarg.h>
#include <stdbool.h>
#include <sys/ioctl.h>
#include <sys/un.h>
#include <termios.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <errno.h>
#include <poll.h>
#include <signal.h>
#include <sys/socket.h>
#include <time.h>

/* SIGWINCH is a common BSD extension, not part of POSIX.1-2008. */
#ifndef SIGWINCH
#define SIGWINCH 28
#endif

#include "../vendor/cjson/cJSON.h"
#include "epsilonctl_internal.h"

#define RESP_CAP (4 * 1024 * 1024)
#define DEFAULT_ADMIN_SOCK "epsilon-admin.sock"

const char *g_host = NULL;      /* set => TCP HTTP mode */
int g_port = 8123;
const char *g_user = "";
const char *g_password = "";
const char *g_sockpath = DEFAULT_ADMIN_SOCK;
volatile sig_atomic_t g_exit_requested = false;
bool g_quiet_stderr = false;   /* suppress chatter in console mode */

/* Prompts for a hidden password on the controlling terminal. Returns a
 * pointer to a static buffer, or NULL when stdin is not a terminal. */
const char *prompt_password(const char *user)
{
    if (!isatty(STDIN_FILENO)) {
        return NULL;
    }
    static char pw[256];
    struct termios oldt, newt;
    if (tcgetattr(STDIN_FILENO, &oldt) != 0) {
        return NULL;
    }
    newt = oldt;
    newt.c_lflag &= (tcflag_t)~ECHO;
    fprintf(stderr, "password for %s: ", user ? user : "");
    fflush(stderr);
    int ok = tcsetattr(STDIN_FILENO, TCSAFLUSH, &newt) == 0 &&
             fgets(pw, sizeof(pw), stdin) != NULL;
    tcsetattr(STDIN_FILENO, TCSAFLUSH, &oldt);
    if (!ok) {
        pw[0] = '\0';
    }
    size_t len = strlen(pw);
    while (len > 0 && (pw[len - 1] == '\n' || pw[len - 1] == '\r')) {
        pw[--len] = '\0';
    }
    fprintf(stderr, "\n");
    return ok ? pw : NULL;
}

/* ------------------------------------------------------------------ */
/* HTTP plumbing                                                       */

char *read_stdin(size_t *len_out)
{
    size_t cap = 65536, len = 0;
    char *buf = malloc(cap);
    if (!buf) {
        return NULL;
    }
    size_t n;
    while ((n = fread(buf + len, 1, cap - len - 1, stdin)) > 0) {
        len += n;
        if (cap - len < 2) {
            cap *= 2;
            char *grown = realloc(buf, cap);
            if (!grown) {
                free(buf);
                return NULL;
            }
            buf = grown;
        }
    }
    buf[len] = '\0';
    *len_out = len;
    return buf;
}

/* Returns HTTP status, or -1 on connection failure. Body via body_out
 * (malloc'd, may be empty). */
int http_request(const char *method, const char *path,
                        const char *body, char **body_out)
{
    int fd;
    if (!g_host) {
        /* local admin socket: trusted, no auth needed */
        fd = socket(AF_UNIX, SOCK_STREAM, 0);
        if (fd < 0) {
            return -1;
        }
        struct sockaddr_un addr = {0};
        addr.sun_family = AF_UNIX;
        snprintf(addr.sun_path, sizeof(addr.sun_path), "%s", g_sockpath);
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            if (!g_quiet_stderr) {
                fprintf(stderr,
                        "epsilonctl: cannot connect to admin socket '%s'\n"
                        "(is epsilond running here? use -h/-p for a remote node)\n",
                        g_sockpath);
            }
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
            if (!g_quiet_stderr) {
                fprintf(stderr, "epsilonctl: invalid host '%s'\n", g_host);
            }
            close(fd);
            return -1;
        }
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            if (!g_quiet_stderr) {
                fprintf(stderr, "epsilonctl: cannot connect to %s:%d\n", g_host,
                        g_port);
            }
            close(fd);
            return -1;
        }
    }

    int n = snprintf(NULL, 0,
                     "%s %s HTTP/1.1\r\n"
                     "Host: %s:%d\r\n"
                     "Authorization: Bearer %s\r\n"
                     "X-Epsilon-Password: %s\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     method, path, g_host ? g_host : "local", g_port, g_user,
                     g_password, body ? strlen(body) : 0);
    if (n < 0) {
        close(fd);
        return -1;
    }
    char *head = malloc((size_t)n + 1);
    if (!head) {
        close(fd);
        return -1;
    }
    snprintf(head, (size_t)n + 1,
             "%s %s HTTP/1.1\r\n"
             "Host: %s:%d\r\n"
             "Authorization: Bearer %s\r\n"
             "X-Epsilon-Password: %s\r\n"
             "Content-Length: %zu\r\n"
             "Connection: close\r\n"
             "\r\n",
             method, path, g_host ? g_host : "local", g_port, g_user,
             g_password, body ? strlen(body) : 0);

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
        sent += (size_t)w;
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
            bsent += (size_t)w;
        }
    }

    static char resp[RESP_CAP];
    size_t total = 0;
    for (;;) {
        if (total >= RESP_CAP - 1) break;
        ssize_t r = recv(fd, resp + total, RESP_CAP - 1 - total, 0);
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
        char *b = strdup(hdr_end + 4);
        *body_out = b ? b : strdup("");
    }
    return status;
}

/* ------------------------------------------------------------------ */
/* full-screen console (nano-style TUI)                                */

#define ZTUI_ART_LINES 8
void out_line(const char *text)
{
    if (g_output) {
        sb_push(g_output, text ? text : "");
    } else {
        puts(text ? text : "");
    }
}

/* One-shot run() used by CLI mode; routes through g_output when set. */
int run(const char *method, const char *path, const char *body)
{
    scrollback tmp = {0};
    int status = run_sb(&tmp, method, path, body);
    for (size_t i = 0; i < tmp.count; i++) {
        out_line(tmp.lines[i]);
    }
    sb_free(&tmp);
    return (status == 200 || status == 201) ? 0 : 1;
}

/* ------------------------------------------------------------------ */
/* helpers                                                             */

void print_usage(void)
{
    static const char *lines[] = {
        "usage: epsilonctl [-s socket] | [-h host -p port -u user [-P password]]",
        "                <command> [args...]",
        "",
        "server:",
        "  status",
        "  install                 install binaries to /usr/bin + service setup",
        "  setup                   re-ask, upgrade binaries, update the service",
        "",
        "admin:",
        "  list databases|groups|users|partitions|keyspaces|settings",
        "  create database <name> <replication_factor>",
        "  delete database <name>",
        "  create group <name>",
        "  delete group <name>",
        "  create user <name> <group_mask> [password]",
        "  set user <name> <group_mask> [password]",
        "  delete user <name>",
        "  create partition <db> <name> <create_mask> <update_mask>",
        "                   <read_mask> <delete_mask>",
        "  delete partition <db> <name>",
        "  get setting <name> | set setting <name> <json|->",
        "  delete setting <name>",
        "",
        "data (partitions/keyspaces are created automatically on put):",
        "  get <db>/<partition>/<keyspace>/<id>",
        "  put <db>/<partition>/<keyspace>/<id> <json|-> [ttl]",
        "  rm <db>/<partition>/<keyspace>/<id>",
        "  all <db>/<partition>/<keyspace> [--filter <json>]...",
        "  ids <db>/<partition>/<keyspace> [--filter <json>]...",
        "  query <db>/<partition>/<keyspace> [--filter <json>]...",
        "",
        "cluster (requires epsilond -n <peer_port>):",
        "  join node <addr> <port> [secret]   join an existing mesh via seed",
        "  remove node <id>                  tombstone a node + re-shard",
        "  list nodes | cluster        membership, leader and ranges",
        "",
        "performance:",
        "  bench [records] [replication_factor] [threads]",
        NULL,
    };
    for (int i = 0; lines[i]; i++) {
        out_line(lines[i]);
    }
}

/* Builds "/data/{...}" path prefix from a slash-joined spec. */
static bool uint64_argument(const char *text)
{
    if (!text || !*text) {
        return false;
    }
    errno = 0;
    char *end = NULL;
    strtoull(text, &end, 10);
    return errno == 0 && end && *end == '\0';
}

/* Builds the JSON body for creating/updating a user, optionally carrying a
 * password. Returns a malloc'd string (caller frees) or NULL on failure. */
static char *user_json(const char *name, const char *groups,
                       const char *password)
{
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return NULL;
    }
    cJSON_AddStringToObject(obj, "name", name);
    cJSON_AddStringToObject(obj, "groups", groups);
    if (password && *password) {
        cJSON_AddStringToObject(obj, "password", password);
    }
    char *json = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    return json;
}


static bool parse_data_spec(const char *spec, char *path, size_t cap)
{
    snprintf(path, cap, "/data/%s", spec);
    return 1;
}

static char *build_filter_body(const char **filters, size_t count)
{
    if (count == 0) {
        return NULL;
    }
    cJSON *root = cJSON_CreateObject();
    cJSON *array = root ? cJSON_AddArrayToObject(root, "filters") : NULL;
    for (size_t i = 0; array && i < count; i++) {
        cJSON *filter = cJSON_Parse(filters[i]);
        if (!cJSON_IsObject(filter)) {
            cJSON_Delete(filter);
            cJSON_Delete(root);
            return NULL;
        }
        cJSON_AddItemToArray(array, filter);
    }
    char *body = array ? cJSON_PrintUnformatted(root) : NULL;
    cJSON_Delete(root);
    return body;
}

/* Collects repeated structured --filter JSON objects. */
static bool collect_filters(int argc, char **argv, int *index,
                            const char ***filters, size_t *count)
{
    size_t capacity = 4;
    *filters = malloc(capacity * sizeof(char *));
    if (!*filters) {
        return false;
    }
    while (*index < argc && strcmp(argv[*index], "--filter") == 0 &&
           *index + 1 < argc) {
        if (*count == capacity) {
            capacity *= 2;
            const char **grown = realloc((void *)*filters,
                                         capacity * sizeof(char *));
            if (!grown) {
                free((void *)*filters);
                *filters = NULL;
                *count = 0;
                return false;
            }
            *filters = grown;
        }
        (*filters)[(*count)++] = argv[*index + 1];
        *index += 2;
    }
    return true;
}

char *body_from_arg(const char *arg)
{
    if (arg && strcmp(arg, "-") == 0 && g_output) {
        return NULL;   /* stdin is not usable from the console */
    }
    if (!arg || strcmp(arg, "-") != 0) {
        return arg ? strdup(arg) : NULL;
    }
    size_t len = 0;
    char *buf = read_stdin(&len);
    if (buf && len == 0) {
        free(buf);
        return NULL;
    }
    return buf;
}

/* ------------------------------------------------------------------ */
/* command dispatch                                                    */

/* interactive install/setup routines (defined below the dispatch) */

/* Executes a command from an argv-style array. Output goes to stdout.
 * Returns process-style exit code (0 = success). */
int execute_command(int argc, char **argv)
{
    int argi = 0;
    const char *cmd = argi < argc ? argv[argi++] : NULL;
    const char *sub = argi < argc ? argv[argi++] : NULL;

    if (!cmd) {
        return 2;
    }

    /* ---- status ---- */
    if (strcmp(cmd, "status") == 0) {
        return run("GET", "/status", NULL);
    }

    /* ---- install / setup (interactive, no server needed for install) ---- */
    if (strcmp(cmd, "install") == 0) {
        return cmd_install(argc, argv);
    }
    if (strcmp(cmd, "setup") == 0) {
        return cmd_setup(argc, argv);
    }

    /* ---- performance benchmark ---- */
    if (strcmp(cmd, "bench") == 0) {
        char body[512];
        long records = argi < argc ? strtol(argv[argi], NULL, 10) : 100000;
        if (records < 1) {
            records = 100000;
        }
        long rf = argi + 1 < argc ? strtol(argv[argi + 1], NULL, 10) : 1;
        if (rf < 1) {
            rf = 1;
        }
        long threads = argi + 2 < argc ? strtol(argv[argi + 2], NULL, 10) : 0;
        if (threads < 0) {
            threads = 0;
        }
        snprintf(body, sizeof(body),
                 "{\"records\":%ld,\"replication_factor\":%ld,"
                 "\"threads\":%ld}",
                 records, rf, threads);
        return run("POST", "/admin/benchmark", body);
    }

    /* ---- help / exit inside the shell ---- */
    if (strcmp(cmd, "help") == 0) {
        print_usage();
        return 0;
    }
    if (strcmp(cmd, "exit") == 0 || strcmp(cmd, "quit") == 0) {
        g_exit_requested = true;
        return 0;
    }

    /* ---- list ---- */
    if (strcmp(cmd, "list") == 0 && sub) {
        if (strcmp(sub, "nodes") == 0) {
            return run("GET", "/admin/cluster", NULL);
        }
        if (strcmp(sub, "databases") == 0) {
            return run("GET", "/admin/databases", NULL);
        }
        if (strcmp(sub, "groups") == 0) {
            return run("GET", "/admin/groups", NULL);
        }
        if (strcmp(sub, "users") == 0) {
            return run("GET", "/admin/users", NULL);
        }
        if (strcmp(sub, "partitions") == 0) {
            static char pbuf[512];
            if (argi < argc) {
                snprintf(pbuf, sizeof(pbuf),
                         "/admin/partitions?database=%s", argv[argi]);
            } else {
                snprintf(pbuf, sizeof(pbuf), "/admin/partitions");
            }
            return run("GET", pbuf, NULL);
        }
        if (strcmp(sub, "keyspaces") == 0) {
            static char kbuf[512];
            if (argi < argc) {
                snprintf(kbuf, sizeof(kbuf),
                         "/admin/keyspaces?database=%s", argv[argi]);
            } else {
                snprintf(kbuf, sizeof(kbuf), "/admin/keyspaces");
            }
            return run("GET", kbuf, NULL);
        }
        if (strcmp(sub, "settings") == 0) {
            return run("GET", "/admin/settings", NULL);
        }
        print_usage();
        return 1;
    }

    /* ---- cluster ---- */
    if (strcmp(cmd, "cluster") == 0) {
        return run("GET", "/admin/cluster", NULL);
    }
    if (sub && strcmp(cmd, "join") == 0 && strcmp(sub, "node") == 0 &&
        argi + 1 < argc) {
        char body[512];
        const char *secret = argi + 2 < argc ? argv[argi + 2] : NULL;
        if (secret) {
            snprintf(body, sizeof(body),
                     "{\"addr\":\"%s\",\"port\":%s,\"secret\":\"%s\"}",
                     argv[argi], argv[argi + 1], secret);
        } else {
            snprintf(body, sizeof(body), "{\"addr\":\"%s\",\"port\":%s}",
                     argv[argi], argv[argi + 1]);
        }
        return run("POST", "/admin/join", body);
    }
    if (sub && strcmp(cmd, "remove") == 0 && strcmp(sub, "node") == 0 &&
        argi < argc) {
        char body[192];
        snprintf(body, sizeof(body), "{\"node_id\":\"%s\"}", argv[argi]);
        return run("POST", "/admin/remove-node", body);
    }

    /* ---- create/delete entities ---- */
    if (sub && strcmp(cmd, "create") == 0) {
        char body[1024];
        if (strcmp(sub, "database") == 0 && argi < argc) {
            const char *name = argv[argi];
            long rf = argi + 1 < argc ? strtol(argv[argi + 1], NULL, 10) : 1;
            snprintf(body, sizeof(body),
                     "{\"name\":\"%s\",\"replication_factor\":%ld}", name,
                     rf);
            return run("POST", "/admin/databases", body);
        }
        if (strcmp(sub, "group") == 0 && argi < argc) {
            snprintf(body, sizeof(body), "{\"name\":\"%s\"}", argv[argi]);
            return run("POST", "/admin/groups", body);
        }
        if (strcmp(sub, "user") == 0 && argi + 1 < argc) {
            if (!uint64_argument(argv[argi + 1])) {
                return 1;
            }
            const char *password = argi + 2 < argc ? argv[argi + 2] : NULL;
            char *json = user_json(argv[argi], argv[argi + 1], password);
            if (!json) {
                return 1;
            }
            int rc = run("POST", "/admin/users", json);
            free(json);
            return rc;
        }
        if (strcmp(sub, "partition") == 0 && argi + 5 < argc) {
            for (int i = 2; i <= 5; i++) {
                if (!uint64_argument(argv[argi + i])) {
                    return 1;
                }
            }
            snprintf(body, sizeof(body),
                     "{\"database\":\"%s\",\"name\":\"%s\","
                     "\"create_mask\":\"%s\",\"update_mask\":\"%s\","
                     "\"read_mask\":\"%s\",\"delete_mask\":\"%s\"}",
                     argv[argi], argv[argi + 1], argv[argi + 2],
                     argv[argi + 3], argv[argi + 4], argv[argi + 5]);
            return run("POST", "/admin/partitions", body);
        }
        print_usage();
        return 1;
    }

    if (sub && strcmp(cmd, "delete") == 0) {
        char path[768];
        if ((strcmp(sub, "database") == 0 || strcmp(sub, "group") == 0 ||
             strcmp(sub, "user") == 0 || strcmp(sub, "setting") == 0) &&
            argi < argc) {
            const char *kind =
                strcmp(sub, "database") == 0 ? "databases"
                : strcmp(sub, "group") == 0  ? "groups"
                : strcmp(sub, "user") == 0   ? "users"
                                             : "settings";
            snprintf(path, sizeof(path), "/admin/%s/%s", kind, argv[argi]);
            return run("DELETE", path, NULL);
        }
        if (strcmp(sub, "partition") == 0 && argi + 1 < argc) {
            snprintf(path, sizeof(path), "/admin/partitions/%s/%s",
                     argv[argi], argv[argi + 1]);
            return run("DELETE", path, NULL);
        }
        print_usage();
        return 1;
    }

    /* ---- user management ---- */
    if (sub && strcmp(cmd, "set") == 0 && strcmp(sub, "user") == 0 &&
        argi + 1 < argc) {
        if (!uint64_argument(argv[argi + 1])) {
            return 1;
        }
        const char *password = argi + 2 < argc ? argv[argi + 2] : NULL;
        char *json = user_json(argv[argi], argv[argi + 1], password);
        if (!json) {
            return 1;
        }
        int rc = run("PUT", "/admin/users", json);
        free(json);
        return rc;
    }

    /* ---- settings shortcuts ---- */
    if (sub && strcmp(cmd, "get") == 0 && strcmp(sub, "setting") == 0 &&
        argi < argc) {
        char path[512];
        snprintf(path, sizeof(path), "/admin/settings/%s", argv[argi]);
        return run("GET", path, NULL);
    }
    if (sub && strcmp(cmd, "set") == 0 && strcmp(sub, "setting") == 0 &&
        argi < argc) {
        char path[512];
        snprintf(path, sizeof(path), "/admin/settings/%s", argv[argi]);
        char *body = body_from_arg(argi + 1 < argc ? argv[argi + 1] : "-");
        int rc = run("POST", path, body);
        free(body);
        return rc;
    }

    /* ---- data operations ---- */
    if (sub && (strcmp(cmd, "get") == 0 || strcmp(cmd, "put") == 0 ||
                strcmp(cmd, "rm") == 0 || strcmp(cmd, "all") == 0 ||
                strcmp(cmd, "ids") == 0 || strcmp(cmd, "query") == 0)) {
        char path[2048];
        parse_data_spec(sub, path, sizeof(path));

        if (strcmp(cmd, "get") == 0) {
            return run("GET", path, NULL);
        }
        if (strcmp(cmd, "put") == 0) {
            char *doc = body_from_arg(argi < argc ? argv[argi] : "-");
            if (!doc) {
                out_line("epsilonctl: no document supplied");
                return 1;
            }
            size_t length = strlen(path);
            if (argi + 1 < argc) {
                size_t written = snprintf(path + length,
                                          sizeof(path) - length, "?ttl=%s",
                                          argv[argi + 1]);
                if (written >= sizeof(path) - length) {
                    free(doc);
                    return 1;
                }
            }
            int result = run("PUT", path, doc);
            free(doc);
            return result;
        }
        if (strcmp(cmd, "rm") == 0) {
            return run("DELETE", path, NULL);
        }

        const char **filters = NULL;
        size_t filter_count = 0;
        if (!collect_filters(argc, argv, &argi, &filters, &filter_count) ||
            argi != argc) {
            free((void *)filters);
            return 1;
        }
        size_t length = strlen(path);
        if (snprintf(path + length, sizeof(path) - length, "/%s", cmd) >=
            (int)(sizeof(path) - length)) {
            free((void *)filters);
            return 1;
        }
        char *body = build_filter_body(filters, filter_count);
        free((void *)filters);
        if (filter_count > 0 && !body) {
            out_line("epsilonctl: each --filter must be a JSON object");
            return 1;
        }
        const char *method = filter_count > 0 || strcmp(cmd, "query") == 0
                                 ? "POST"
                                 : "GET";
        int result = run(method, path, body);
        free(body);
        return result;
    }

    /* ---- cluster: handled above with the other joins ---- */

    print_usage();
    return 1;
}

/* ------------------------------------------------------------------ */
/* install / setup: interactive server configuration                   */
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
            g_host = g_host ? g_host : "127.0.0.1";   /* -p implies TCP */
        } else if ((strcmp(argv[argi], "-u") == 0 ||
                    strcmp(argv[argi], "--user") == 0) &&
                   argi + 1 < argc) {
            g_user = argv[++argi];
        } else if ((strcmp(argv[argi], "-P") == 0 ||
                    strcmp(argv[argi], "--password") == 0) &&
                   argi + 1 < argc) {
            g_password = argv[++argi];
        } else {
            print_usage();
            return 1;
        }
        argi++;
    }

    /* remote user auth requires a password: prompt when possible,
     * otherwise refuse to run (the server rejects passwordless users) */
    if (g_host && g_user && *g_user && g_password[0] == '\0' &&
        !g_quiet_stderr) {
        const char *pw = prompt_password(g_user);
        if (pw) {
            g_password = pw;
        } else {
            fprintf(stderr,
                    "epsilonctl: user '%s' needs a password over HTTP "
                    "(pass -P <password>)\n",
                    g_user);
            return 1;
        }
    }

    if (argi >= argc) {
        shell_loop();      /* no command: full-screen console */
        return 0;
    }

    return execute_command(argc - argi, argv + argi);
}