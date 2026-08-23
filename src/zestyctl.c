/* zestyctl - ZestyDB command line client.
 *
 * Connects to a node's HTTP endpoint and issues admin/data requests.
 *
 * Usage: zestyctl [-s socket] | [-h host] [-p port] [-u user] <command> [args...]
 *
 * By default zestyctl connects to the server's local admin socket (no
 * authentication; treated as admin). Pass -h/-p to talk to a remote node's
 * HTTP port instead, in which case -u is required for privileged commands.
 *
 * Commands:
 *   status
 *   list databases|groups|users|partitions|settings
 *   create database <name> <replication_factor>
 *   delete database <name>
 *   create group <name>
 *   delete group <name>
 *   create user <name> <group_mask>
 *   set user <name> <group_mask>
 *   delete user <name>
 *   create partition <db> <name> <create_mask> <update_mask> <read_mask> <delete_mask>
 *   delete partition <db> <name>
 *   get <db>/<partition>/<keyspace>/<id>
 *   put <db>/<partition>/<keyspace>/<id> <json | -> [--filter k=v]... [ttl_seconds]
 *   rm <db>/<partition>/<keyspace>/<id>
 *   query <db>/<partition>/<keyspace> [--filter k=v]... [--field k=v]...
 *   ids <db>/<partition>/<keyspace> [--filter k=v]...
 *   all <db>/<partition>/<keyspace> [--filter k=v]...
 *   get setting <name>
 *   set setting <name> <json | ->
 *   delete setting <name>
 *
 * A body argument of "-" reads JSON from stdin (CLI mode only).
 * Mask 0 = allow all groups.
 *
 * Running zestyctl with no command opens a full-screen interactive
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

#define RESP_CAP (4 * 1024 * 1024)
#define DEFAULT_ADMIN_SOCK "zesty-admin.sock"

static const char *g_host = NULL;      /* set => TCP HTTP mode */
static int g_port = 8123;
static const char *g_user = "";
static const char *g_sockpath = DEFAULT_ADMIN_SOCK;
static volatile sig_atomic_t g_exit_requested = false;
static bool g_quiet_stderr = false;   /* suppress chatter in console mode */

/* ------------------------------------------------------------------ */
/* HTTP plumbing                                                       */

static char *read_stdin(size_t *len_out)
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
static int http_request(const char *method, const char *path,
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
                        "zestyctl: cannot connect to admin socket '%s'\n"
                        "(is zestyd running here? use -h/-p for a remote node)\n",
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
                fprintf(stderr, "zestyctl: invalid host '%s'\n", g_host);
            }
            close(fd);
            return -1;
        }
        if (connect(fd, (struct sockaddr *)&addr, sizeof(addr)) != 0) {
            if (!g_quiet_stderr) {
                fprintf(stderr, "zestyctl: cannot connect to %s:%d\n", g_host,
                        g_port);
            }
            close(fd);
            return -1;
        }
    }

    char head[4096];
    int n = snprintf(head, sizeof(head),
                     "%s %s HTTP/1.1\r\n"
                     "Host: %s:%d\r\n"
                     "Authorization: Bearer %s\r\n"
                     "Content-Length: %zu\r\n"
                     "Connection: close\r\n"
                     "\r\n",
                     method, path,
                     g_host ? g_host : "local", g_port, g_user,
                     body ? strlen(body) : 0);

    /* send headers then body separately: exercises server buffering */
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

static const char *g_art[ZTUI_ART_LINES] = {
    "  ______         _         _____  ____  ",
    " |___  /        | |       |  __ \\|  _ \\ ",
    "    / / ___  ___| |_ _   _| |  | | |_) |",
    "   / / / _ \\/ __| __| | | | |  | |  _ < ",
    "  / /_|  __/\\__ \\ |_| |_| | |__| | |_) |",
    " /_____\\___||___/\\__|\\__, |_____/|____/ ",
    "                      __/ |             ",
    "                     |___/             ",
};

typedef struct {
    char **lines;      /* scrollback content */
    size_t count;
    size_t cap;
} scrollback;

static void sb_push(scrollback *sb, const char *text)
{
    if (!text) text = "";
    if (sb->count == sb->cap) {
        sb->cap = sb->cap ? sb->cap * 2 : 128;
        char **grown = realloc(sb->lines, sb->cap * sizeof(char *));
        if (!grown) return;
        sb->lines = grown;
    }
    sb->lines[sb->count] = strdup(text);
    if (sb->lines[sb->count]) {
        sb->count++;
    }
}

static void sb_printf(scrollback *sb, const char *fmt, ...)
{
    char buf[8192];
    va_list ap;
    va_start(ap, fmt);
    vsnprintf(buf, sizeof(buf), fmt, ap);
    va_end(ap);
    sb_push(sb, buf);
}

static void sb_free(scrollback *sb)
{
    for (size_t i = 0; i < sb->count; i++) {
        free(sb->lines[i]);
    }
    free(sb->lines);
    sb->lines = NULL;
    sb->count = sb->cap = 0;
}

/* --- terminal raw mode (POSIX termios, no curses) ------------------ */

struct termios g_saved_termios;
static bool g_raw_mode = false;

static bool enable_raw_mode(void)
{
    if (tcgetattr(STDIN_FILENO, &g_saved_termios) != 0) {
        return false;
    }
    struct termios raw = g_saved_termios;
    /* cfmakeraw equivalent but keep signal handling off ISIG so we can
     * read CTRL+key combos ourselves */
    raw.c_iflag &= ~(unsigned)(BRKINT | ICRNL | INPCK | ISTRIP | IXON);
    raw.c_oflag &= ~(unsigned)(OPOST);
    raw.c_lflag &= ~(unsigned)(ECHO | ICANON | IEXTEN);
    raw.c_cflag |= (CS8);
    raw.c_cc[VMIN] = 1;
    raw.c_cc[VTIME] = 0;
    if (tcsetattr(STDIN_FILENO, TCSAFLUSH, &raw) != 0) {
        return false;
    }
    g_raw_mode = true;
    return true;
}

static void disable_raw_mode(void)
{
    if (g_raw_mode) {
        tcsetattr(STDIN_FILENO, TCSAFLUSH, &g_saved_termios);
        g_raw_mode = false;
    }
}

static int terminal_width(void)
{
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_col > 0) {
        return (int)ws.ws_col;
    }
    return 80;
}

static int terminal_height(void)
{
    struct winsize ws;
    if (ioctl(STDOUT_FILENO, TIOCGWINSZ, &ws) == 0 && ws.ws_row > 0) {
        return (int)ws.ws_row;
    }
    return 24;
}

/* --- colourised output rendering ----------------------------------- */

#define MAX_COLS 62

static void s_append(char *line, size_t cap, const char *text)
{
    size_t len = strlen(line);
    if (len + 1 >= cap) {
        return;
    }
    snprintf(line + len, cap - len, "%s", text);
}

/* Renders the plain display text of a JSON scalar and picks a colour
 * for its type: numbers cyan, strings yellow, true green, false red,
 * null dim, containers magenta. */
static void scalar_text(const cJSON *v, char *buf, size_t cap,
                        const char **colour)
{
    *colour = "";
    if (!v || cJSON_IsNull(v)) {
        *colour = "\033[2m";
        snprintf(buf, cap, "null");
    } else if (cJSON_IsBool(v)) {
        *colour = cJSON_IsTrue(v) ? "\033[32m" : "\033[31m";
        snprintf(buf, cap, cJSON_IsTrue(v) ? "true" : "false");
    } else if (cJSON_IsNumber(v)) {
        *colour = "\033[36m";
        if (v->valuedouble == (double)v->valueint) {
            snprintf(buf, cap, "%d", v->valueint);
        } else {
            snprintf(buf, cap, "%g", v->valuedouble);
        }
    } else if (cJSON_IsString(v)) {
        *colour = "\033[33m";
        snprintf(buf, cap, "%s", v->valuestring ? v->valuestring : "");
    } else if (cJSON_IsArray(v)) {
        /* compact inline list: a, b, c */
        *colour = "\033[35m";
        buf[0] = '\0';
        const cJSON *el = NULL;
        bool first = true;
        cJSON_ArrayForEach(el, v) {
            char cell[160];
            const char *c2;
            scalar_text(el, cell, sizeof(cell), &c2);
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
        *colour = "\033[35m";
        snprintf(buf, cap, "%s", printed ? printed : "?");
        free(printed);
    }
}

/* Pretty-print a JSON array of flat objects as an aligned, colour-coded
 * table. Arrays of scalars render as a single-column list; a bare
 * object renders as a one-row table; non-JSON falls through as text. */
static void render_json_table(scrollback *sb, const char *json)
{
    cJSON *parsed = cJSON_Parse(json);
    if (!parsed) {
        sb_push(sb, json ? json : "");
        return;
    }

    char line[8192];

    /* array of scalars (e.g. the ids endpoint): single-column listing */
    if (cJSON_IsArray(parsed) &&
        (cJSON_GetArraySize(parsed) == 0 ||
         !cJSON_IsObject(cJSON_GetArrayItem(parsed, 0)))) {
        if (cJSON_GetArraySize(parsed) == 0) {
            sb_push(sb, "\033[2m(empty)\033[0m");
            cJSON_Delete(parsed);
            return;
        }
        line[0] = '\0';
        cJSON *item = NULL;
        int n = 0;
        cJSON_ArrayForEach(item, parsed) {
            char cell[512];
            const char *colour;
            scalar_text(item, cell, sizeof(cell), &colour);
            char piece[600];
            snprintf(piece, sizeof(piece), "%s%s\033[0m", colour, cell);
            if (n > 0) {
                s_append(line, sizeof(line), "  ");
            }
            s_append(line, sizeof(line), piece);
            n++;
        }
        sb_push(sb, line);
        cJSON_Delete(parsed);
        return;
    }

    cJSON *rows = parsed;
    bool wrapped = false;
    if (!cJSON_IsArray(parsed)) {
        rows = cJSON_CreateArray();
        cJSON_AddItemToArray(rows, parsed);   /* transfers ownership */
        wrapped = true;
    }

    /* gather column names in first-appearance order */
    const char *cols[MAX_COLS];
    int ncols = 0;
    cJSON *item = NULL;
    cJSON_ArrayForEach(item, rows) {
        if (!cJSON_IsObject(item)) continue;
        cJSON *field = NULL;
        cJSON_ArrayForEach(field, item) {
            bool seen = false;
            for (int i = 0; i < ncols; i++) {
                if (strcmp(cols[i], field->string) == 0) {
                    seen = true;
                    break;
                }
            }
            if (!seen && ncols < MAX_COLS) {
                cols[ncols++] = field->string;
            }
        }
    }

    if (ncols == 0) {
        char *printed = cJSON_PrintUnformatted(rows);
        sb_push(sb, printed ? printed : "");
        free(printed);
        cJSON_Delete(wrapped ? rows : parsed);
        return;
    }

    /* compute widths from plain text */
    size_t widths[MAX_COLS];
    for (int i = 0; i < ncols; i++) {
        widths[i] = strlen(cols[i]);
    }
    cJSON_ArrayForEach(item, rows) {
        if (!cJSON_IsObject(item)) continue;
        for (int i = 0; i < ncols; i++) {
            char cell[512];
            const char *colour;
            scalar_text(cJSON_GetObjectItemCaseSensitive(item, cols[i]),
                        cell, sizeof(cell), &colour);
            size_t len = strlen(cell);
            if (len > widths[i]) {
                widths[i] = len;
            }
            (void)colour;
        }
    }

    /* header: bold cyan */
    line[0] = '\0';
    s_append(line, sizeof(line), "\033[1;96m");
    for (int i = 0; i < ncols; i++) {
        char cell[256];
        snprintf(cell, sizeof(cell), "%-*s", (int)widths[i], cols[i]);
        s_append(line, sizeof(line), cell);
        if (i + 1 < ncols) {
            s_append(line, sizeof(line), "  ");
        }
    }
    s_append(line, sizeof(line), "\033[0m");
    sb_push(sb, line);

    /* separator: dim dashes */
    line[0] = '\0';
    s_append(line, sizeof(line), "\033[2m");
    for (int i = 0; i < ncols; i++) {
        for (size_t c = 0; c < widths[i]; c++) {
            s_append(line, sizeof(line), "-");
        }
        if (i + 1 < ncols) {
            s_append(line, sizeof(line), "  ");
        }
    }
    s_append(line, sizeof(line), "\033[0m");
    sb_push(sb, line);

    /* rows: cells colour-coded by value type */
    cJSON_ArrayForEach(item, rows) {
        if (!cJSON_IsObject(item)) {
            continue;
        }
        line[0] = '\0';
        for (int i = 0; i < ncols; i++) {
            char cell[512];
            const char *colour;
            scalar_text(cJSON_GetObjectItemCaseSensitive(item, cols[i]),
                        cell, sizeof(cell), &colour);
            char piece[600];
            snprintf(piece, sizeof(piece), "%s%-*.*s\033[0m", colour,
                     (int)widths[i], (int)widths[i], cell);
            s_append(line, sizeof(line), piece);
            if (i + 1 < ncols) {
                s_append(line, sizeof(line), "  ");
            }
        }
        sb_push(sb, line);
    }

    cJSON_Delete(wrapped ? rows : parsed);
}

/* Runs a request, appending the tabular/colour-coded body to sb.
 * Returns HTTP status or -1. Successful responses show only their
 * payload; failures surface a coloured error line. */
static int run_sb(scrollback *sb, const char *method, const char *path,
                  const char *body)
{
    char *resp_body = NULL;
    int status = http_request(method, path, body, &resp_body);
    if (status < 0) {
        sb_push(sb, "\033[31mconnection failed\033[0m");
        return -1;
    }
    if (status < 200 || status >= 300) {
        const char *status_colour =
            status >= 400 && status < 500 ? "\033[33m"
            : status >= 500 ? "\033[31m" : "";
        const char *status_text =
            status == 400 ? "Bad Request"
            : status == 401 ? "Unauthorized"
            : status == 403 ? "Forbidden"
            : status == 404 ? "Not Found"
            : status == 409 ? "Conflict"
            : status >= 500 ? "Server Error" : "Failed";
        char headline[512];
        snprintf(headline, sizeof(headline), "%s%s\033[0m", status_colour,
                 status_text);
        sb_push(sb, headline);
    }
    if (resp_body && *resp_body) {
        render_json_table(sb, resp_body);
    }
    free(resp_body);
    return status;
}

/* Output sink: when the full-screen console is active, command output
 * is appended to its scrollback instead of being written to stdout. */
static scrollback *g_output = NULL;

static void out_line(const char *text)
{
    if (g_output) {
        sb_push(g_output, text ? text : "");
    } else {
        puts(text ? text : "");
    }
}

/* One-shot run() used by CLI mode; routes through g_output when set. */
static int run(const char *method, const char *path, const char *body)
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

static void print_usage(void)
{
    static const char *lines[] = {
        "usage: zestyctl [-s socket] | [-h host -p port -u user]",
        "                <command> [args...]",
        "",
        "server:",
        "  status",
        "",
        "admin:",
        "  list databases|groups|users|partitions|keyspaces|settings",
        "  create database <name> <replication_factor>",
        "  delete database <name>",
        "  create group <name>",
        "  delete group <name>",
        "  create user <name> <group_mask>",
        "  set user <name> <group_mask>",
        "  delete user <name>",
        "  create partition <db> <name> <create_mask> <update_mask>"
        " <read_mask> <delete_mask>",
        "  delete partition <db> <name>",
        "  get setting <name> | set setting <name> <json|->",
        "  delete setting <name>",
        "",
        "data (partitions/keyspaces are created automatically on put):",
        "  get <db>/<partition>/<keyspace>/<id>",
        "  put <db>/<partition>/<keyspace>/<id> <json|->"
        " [--filter k=v]... [ttl]",
        "  rm <db>/<partition>/<keyspace>/<id>",
        "  all <db>/<partition>/<keyspace> [--filter k=v]...",
        "  ids <db>/<partition>/<keyspace> [--filter k=v]...",
        "  query <db>/<partition>/<keyspace> [--filter k=v]...",
        "        [--field k=v]...",
        "",
        "cluster (requires zestyd -n <peer_port>):",
        "  join node <addr> <port>     join an existing mesh via seed",
        "  list nodes | cluster        membership, leader and ranges",
        NULL,
    };
    for (int i = 0; lines[i]; i++) {
        out_line(lines[i]);
    }
}

/* Builds "/data/{...}" path prefix from a slash-joined spec. */
static bool parse_data_spec(const char *spec, char *path, size_t cap)
{
    snprintf(path, cap, "/data/%s", spec);
    return 1;
}

static void append_filters(char *path, size_t cap, const char **filters,
                           size_t nfilters, const char **fields,
                           size_t nfields)
{
    size_t len = strlen(path);
    for (size_t i = 0; i < nfilters; i++) {
        len += snprintf(path + len, cap - len, "%sfilter=%s",
                        strchr(path, '?') ? "&" : "?", filters[i]);
    }
    for (size_t i = 0; i < nfields; i++) {
        len += snprintf(path + len, cap - len, "%sfield=%s",
                        strchr(path, '?') ? "&" : "?", fields[i]);
    }
}

/* Collects repeated --filter/--field options starting at argv[*i]. */
static void collect_options(int argc, char **argv, int *i,
                            const char ***filters, size_t *nfilters,
                            const char ***fields, size_t *nfields)
{
    size_t fcap = 4, flcap = 4;
    *filters = malloc(fcap * sizeof(char *));
    *fields = malloc(flcap * sizeof(char *));
    while (*i < argc) {
        if (strcmp(argv[*i], "--filter") == 0 && *i + 1 < argc) {
            if (*nfilters == fcap) {
                fcap *= 2;
                *filters = realloc(*filters, fcap * sizeof(char *));
            }
            (*filters)[(*nfilters)++] = argv[++*i];
        } else if (strcmp(argv[*i], "--field") == 0 && *i + 1 < argc) {
            if (*nfields == flcap) {
                flcap *= 2;
                *fields = realloc(*fields, flcap * sizeof(char *));
            }
            (*fields)[(*nfields)++] = argv[++*i];
        } else {
            break;
        }
        (*i)++;
    }
}

static char *body_from_arg(const char *arg)
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

/* Executes a command from an argv-style array. Output goes to stdout.
 * Returns process-style exit code (0 = success). */
static int execute_command(int argc, char **argv)
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

    /* ---- cluster (stage 4) ---- */
    if (strcmp(cmd, "cluster") == 0) {
        return run("GET", "/admin/cluster", NULL);
    }
    if (sub && strcmp(cmd, "join") == 0 && strcmp(sub, "node") == 0 &&
        argi + 1 < argc) {
        char body[256];
        snprintf(body, sizeof(body), "{\"addr\":\"%s\",\"port\":%s}",
                 argv[argi], argv[argi + 1]);
        return run("POST", "/admin/join", body);
    }

    /* ---- create/delete entities ---- */
    if (sub && strcmp(cmd, "create") == 0) {
        char body[1024];
        if (strcmp(sub, "database") == 0 && argi + 1 <= argc) {
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
            snprintf(body, sizeof(body),
                     "{\"name\":\"%s\",\"groups\":%s}", argv[argi],
                     argv[argi + 1]);
            return run("POST", "/admin/users", body);
        }
        if (strcmp(sub, "partition") == 0 && argi + 5 < argc) {
            snprintf(body, sizeof(body),
                     "{\"database\":\"%s\",\"name\":\"%s\","
                     "\"create_mask\":%s,\"update_mask\":%s,"
                     "\"read_mask\":%s,\"delete_mask\":%s}",
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
        char body[512];
        snprintf(body, sizeof(body), "{\"name\":\"%s\",\"groups\":%s}",
                 argv[argi], argv[argi + 1]);
        return run("POST", "/admin/users", body);
    }

    /* ---- settings shortcuts ---- */
    if (sub && strcmp(cmd, "get") == 0 && strcmp(sub, "setting") == 0 &&
        argi < argc) {
        char path[512];
        snprintf(path, sizeof(path), "/admin/settings/%s", argv[argi]);
        return run("GET", path, NULL);
    }
    if (sub && strcmp(cmd, "set") == 0 && strcmp(sub, "setting") == 0 &&
        argi + 1 <= argc) {
        char path[512];
        snprintf(path, sizeof(path), "/admin/settings/%s", argv[argi]);
        char *body = body_from_arg(argi + 1 <= argc ? argv[argi + 1] : "-");
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
            /* optional --filter k=v pairs may precede the document */
            const char **filters = NULL;
            size_t nfilters = 0, fcap = 4;
            filters = malloc(fcap * sizeof(char *));
            while (argi < argc && strcmp(argv[argi], "--filter") == 0 &&
                   argi + 1 < argc) {
                if (nfilters == fcap) {
                    fcap *= 2;
                    filters = realloc(filters, fcap * sizeof(char *));
                }
                filters[nfilters++] = argv[++argi];
                argi++;
            }

            char *doc = body_from_arg(argi < argc ? argv[argi] : "-");
            if (!doc) {
                out_line("zestyctl: no document supplied");
                free(filters);
                return 1;
            }
            size_t len = strlen(path);
            if (argi + 1 < argc) {
                len += snprintf(path + len, sizeof(path) - len, "?ttl=%s",
                                argv[argi + 1]);
            }
            for (size_t i = 0; i < nfilters; i++) {
                snprintf(path + len, sizeof(path) - len, "%sfilter=%s",
                         strchr(path, '?') ? "&" : "?", filters[i]);
                len = strlen(path);
            }
            int rc = run("PUT", path, doc);
            free(doc);
            free(filters);
            return rc;
        }
        if (strcmp(cmd, "rm") == 0) {
            return run("DELETE", path, NULL);
        }

        /* collection ops */
        const char **filters = NULL, **fields = NULL;
        size_t nfilters = 0, nfields = 0;
        collect_options(argc, argv, &argi, &filters, &nfilters, &fields,
                        &nfields);

        /* append the operation segment: /data/db/partition/keyspace/<op> */
        size_t len = strlen(path);
        snprintf(path + len, sizeof(path) - len, "/%s", cmd);
        append_filters(path, sizeof(path), filters, nfilters, fields,
                       nfields);

        int rc;
        if (strcmp(cmd, "all") == 0) {
            rc = run("GET", path, NULL);
        } else if (strcmp(cmd, "ids") == 0) {
            rc = run("GET", path, NULL);
        } else {
            /* query passes fields in the body too; URL form also works */
            char body[4096] = "";
            if (nfields > 0) {
                strcpy(body, "{\"fields\":{");
                for (size_t i = 0; i < nfields; i++) {
                    char eqcopy[512];
                    snprintf(eqcopy, sizeof(eqcopy), "%s", fields[i]);
                    char *eq = strchr(eqcopy, '=');
                    if (eq) {
                        *eq = '\0';
                        strcat(body, i ? "," : "");
                        strcat(body, "\"");
                        strcat(body, eqcopy);
                        strcat(body, "\":\"");
                        strcat(body, eq + 1);
                        strcat(body, "\"");
                    }
                }
                strcat(body, "}}");
            }
            rc = run("POST", path, nfields > 0 ? body : NULL);
        }
        free(filters);
        free(fields);
        return rc;
    }

    /* ---- cluster (stage 4) ---- */
    if (sub && strcmp(cmd, "join") == 0 && strcmp(sub, "node") == 0 &&
        argi + 1 < argc) {
        char body[256];
        snprintf(body, sizeof(body), "{\"addr\":\"%s\",\"port\":%s}",
                 argv[argi], argv[argi + 1]);
        return run("POST", "/admin/join", body);
    }

    print_usage();
    return 1;
}

/* ------------------------------------------------------------------ */
/* TUI rendering                                                       */

typedef struct {
    char input[1024];
    int cursor;
} input_line;

/* Live server information shown beside the ASCII banner. */
static struct {
    bool reachable;
    char version[24];
    char clustered[8];
    char databases[16];
    char refreshed[16];
} g_info;

static void format_time_now(char *buf, size_t cap)
{
    time_t now = time(NULL);
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(buf, cap, "%H:%M:%S", &tmv);
}

static void target_label(char *buf, size_t cap)
{
    if (g_host) {
        snprintf(buf, cap, "%s:%d", g_host, g_port);
    } else {
        snprintf(buf, cap, "socket:%s", g_sockpath);
    }
}

/* Polls the node for live status; called periodically from the console
 * loop so the header panel stays current. */
static void refresh_server_info(void)
{
    snprintf(g_info.databases, sizeof(g_info.databases), "-");
    snprintf(g_info.version, sizeof(g_info.version), "-");
    snprintf(g_info.clustered, sizeof(g_info.clustered), "-");
    format_time_now(g_info.refreshed, sizeof(g_info.refreshed));

    char *body = NULL;
    int status = http_request("GET", "/status", NULL, &body);
    if (status != 200 || !body) {
        g_info.reachable = false;
        free(body);
        return;
    }
    g_info.reachable = true;

    cJSON *obj = cJSON_Parse(body);
    free(body);
    if (obj) {
        cJSON *jv = cJSON_GetObjectItemCaseSensitive(obj, "version");
        cJSON *jc = cJSON_GetObjectItemCaseSensitive(obj, "clustered");
        if (cJSON_IsString(jv) && jv->valuestring) {
            snprintf(g_info.version, sizeof(g_info.version), "%s",
                     jv->valuestring);
        }
        if (cJSON_IsBool(jc)) {
            snprintf(g_info.clustered, sizeof(g_info.clustered), "%s",
                     cJSON_IsTrue(jc) ? "yes" : "no");
        }
        cJSON_Delete(obj);
    }

    body = NULL;
    status = http_request("GET", "/admin/databases", NULL, &body);
    if (status == 200 && body) {
        cJSON *arr = cJSON_Parse(body);
        if (arr && cJSON_IsArray(arr)) {
            snprintf(g_info.databases, sizeof(g_info.databases), "%d",
                     cJSON_GetArraySize(arr));
        }
        cJSON_Delete(arr);
    }
    free(body);
}

/* Writes one line clipped to `width` display columns. ANSI escape
 * sequences pass through verbatim but do not count against the width,
 * so colourised table rows never wrap or bleed. */
static void emit_clipped(const char *s, int width)
{
    int col = 0;
    const char *p = s;
    while (*p && col < width) {
        if (*p == '\033' && p[1] == '[') {
            const char *q = p + 2;
            while (*q && (*q < '@' || *q > '~')) {
                q++;
            }
            if (*q) {
                q++;
            }
            fwrite(p, 1, (size_t)(q - p), stdout);
            p = q;
            continue;
        }
        putchar(*p++);
        col++;
    }
    printf("\033[0m\033[K");
}

static void render(const scrollback *sb, const input_line *input)
{
    int width = terminal_width();
    int height = terminal_height();
    if (width < 20) width = 20;
    if (height < 10) height = 10;

    /* art + rule + input bar + status bar */
    int body_rows = height - (ZTUI_ART_LINES + 1 + 2);
    if (body_rows < 1) {
        body_rows = 1;
    }

    char target[128];
    target_label(target, sizeof(target));

    const char *labels[ZTUI_ART_LINES] = {
        "server :", "status :", "version:", "cluster:", "dbs    :",
        "updated:", "", "",
    };
    char values[ZTUI_ART_LINES][160];
    snprintf(values[0], sizeof(values[0]), "\033[1m%s\033[0m", target);
    snprintf(values[1], sizeof(values[1]), "%s",
             g_info.reachable ? "\033[32mconnected\033[0m"
                              : "\033[31munreachable\033[0m");
    snprintf(values[2], sizeof(values[2]), "\033[33m%s\033[0m",
             g_info.version);
    snprintf(values[3], sizeof(values[3]), "%s", g_info.clustered);
    snprintf(values[4], sizeof(values[4]), "\033[36m%s\033[0m",
             g_info.databases);
    snprintf(values[5], sizeof(values[5]), "\033[2m%s\033[0m",
             g_info.refreshed);
    snprintf(values[6], sizeof(values[6]), "");
    snprintf(values[7], sizeof(values[7]), "");

    printf("\033[H\033[2J");

    for (int i = 0; i < ZTUI_ART_LINES; i++) {
        printf("\033[1;36m%s\033[0m", g_art[i]);
        int pad = 54 - (int)strlen(g_art[i]);
        if (pad < 1) pad = 1;
        printf("%*s", pad, "");
        printf("\033[2m%s\033[0m %s\033[K\r\n", labels[i], values[i]);
    }

    /* horizontal rule */
    printf("\033[2m");
    for (int i = 0; i < width; i++) {
        putchar('-');
    }
    printf("\033[0m\r\n");

    /* scrollback window: show the last body_rows lines */
    size_t start = sb->count > (size_t)body_rows
                       ? sb->count - (size_t)body_rows
                       : 0;
    int printed = 0;
    for (size_t i = start; i < sb->count && printed < body_rows;
         i++, printed++) {
        emit_clipped(sb->lines[i], width);
        printf("\r\n");
    }
    for (; printed < body_rows; printed++) {
        printf("\033[K\r\n");
    }

    /* input bar: reverse video like nano's prompt */
    char bar[sizeof(input->input) + 4];
    snprintf(bar, sizeof(bar), "> %s", input->input);
    printf("\033[7m%-*.*s\033[0m\033[K\r\n", width, width, bar);

    /* status bar: shortcut hints left, connection right */
    const char *hints =
        "^X Exit  ^L Clear  ^C Cancel  Enter Run  Up/Dn History";
    const char *right = target;
    int used = (int)strlen(hints) + (int)strlen(right);
    int pad = width - used;
    if (pad < 1) pad = 1;
    printf("\033[44;97;1m%s%*s%s\033[0m", hints, pad, "", right);

    /* place the terminal cursor inside the input bar: rows 1..6 are the
     * banner, row 7 the rule, body_rows of output follow (rows 8..), so
     * the input bar sits on row ART_LINES+2+body_rows */
    printf("\033[%d;%dH", ZTUI_ART_LINES + 2 + body_rows,
           input->cursor + 3);
    fflush(stdout);
}

/* Decoded special keys (values above any single byte). */
enum { KEY_UP = 1000, KEY_DOWN, KEY_RIGHT, KEY_LEFT, KEY_HOME, KEY_END,
       KEY_DELETE };

/* Blocking key reader over raw stdin; decodes CSI arrow/home/end/delete
 * escape sequences. Returns a byte value or a KEY_* code, -1 on EOF. */
static int read_key(void)
{
    unsigned char b;
    if (read(STDIN_FILENO, &b, 1) <= 0) {
        return -1;
    }
    if (b != 27) {
        return b;
    }

    struct pollfd pfd = { STDIN_FILENO, POLLIN, 0 };
    if (poll(&pfd, 1, 30) <= 0) {
        return 27;   /* lone ESC */
    }
    if (read(STDIN_FILENO, &b, 1) <= 0) {
        return 27;
    }
    if (b != '[' && b != 'O') {
        return 27;
    }
    if (poll(&pfd, 1, 30) <= 0) {
        return 27;
    }
    if (read(STDIN_FILENO, &b, 1) <= 0) {
        return 27;
    }
    switch (b) {
    case 'A': return KEY_UP;
    case 'B': return KEY_DOWN;
    case 'C': return KEY_RIGHT;
    case 'D': return KEY_LEFT;
    case 'H': return KEY_HOME;
    case 'F': return KEY_END;
    case '1':
    case '7':
        if (poll(&pfd, 1, 30) > 0) {
            if (read(STDIN_FILENO, &b, 1) <= 0) return 27;
        }
        return KEY_HOME;
    case '4':
    case '8':
        if (poll(&pfd, 1, 30) > 0) {
            if (read(STDIN_FILENO, &b, 1) <= 0) return 27;
        }
        return KEY_END;
    case '3':
        if (poll(&pfd, 1, 30) > 0) {
            if (read(STDIN_FILENO, &b, 1) <= 0) return 27;
        }
        return KEY_DELETE;
    default:
        return 27;
    }
}

typedef struct {
    char **items;
    int count;
    int cap;
} cmd_history;

static void history_push(cmd_history *h, const char *cmd)
{
    if (cmd[0] == '\0') {
        return;
    }
    if (h->count > 0 && strcmp(h->items[h->count - 1], cmd) == 0) {
        return;
    }
    if (h->count == h->cap) {
        h->cap = h->cap ? h->cap * 2 : 32;
        char **grown = realloc(h->items, (size_t)h->cap * sizeof(char *));
        if (!grown) {
            return;
        }
        h->items = grown;
    }
    h->items[h->count] = strdup(cmd);
    if (h->items[h->count]) {
        h->count++;
    }
}

static void history_free(cmd_history *h)
{
    for (int i = 0; i < h->count; i++) {
        free(h->items[i]);
    }
    free(h->items);
    h->items = NULL;
    h->count = h->cap = 0;
}

static volatile sig_atomic_t g_winch_flag = 0;

static void on_winch(int sig)
{
    (void)sig;
    g_winch_flag = 1;
}

static void on_terminate(int sig)
{
    (void)sig;
    g_exit_requested = true;
}

static void input_clear(input_line *input)
{
    input->input[0] = '\0';
    input->cursor = 0;
}

static void input_insert(input_line *input, char c)
{
    size_t len = strlen(input->input);
    if ((int)len >= (int)sizeof(input->input) - 1) {
        return;
    }
    memmove(&input->input[input->cursor + 1],
            &input->input[input->cursor], len - (size_t)input->cursor + 1);
    input->input[input->cursor++] = c;
}

static void input_backspace(input_line *input)
{
    if (input->cursor == 0) {
        return;
    }
    memmove(&input->input[input->cursor - 1],
            &input->input[input->cursor],
            strlen(input->input) - (size_t)input->cursor + 1);
    input->cursor--;
}

static void input_delete(input_line *input)
{
    size_t len = strlen(input->input);
    if (input->cursor >= (int)len) {
        return;
    }
    memmove(&input->input[input->cursor],
            &input->input[input->cursor + 1],
            len - (size_t)input->cursor);
}

/* Splits a command line into argv tokens (space separated, no quoting). */
#define MAX_ARGS 64
static int split_command(char *line, char *argv_out[MAX_ARGS])
{
    int argc = 0;
    char *save = NULL;
    char *tok = strtok_r(line, " \t", &save);
    while (tok && argc < MAX_ARGS) {
        argv_out[argc++] = tok;
        tok = strtok_r(NULL, " \t", &save);
    }
    return argc;
}

static void shell_loop(void)
{
    scrollback sb = {0};
    input_line input = {{0}, 0};
    cmd_history hist = {0};
    char draft[sizeof(input.input)] = "";
    int hist_pos = -1;   /* -1 = editing fresh input */

    if (!isatty(STDIN_FILENO) || !enable_raw_mode()) {
        fprintf(stderr, "zestyctl: interactive console requires a "
                        "terminal\n");
        return;
    }

    g_output = &sb;
    g_quiet_stderr = true;

    signal(SIGWINCH, on_winch);
    signal(SIGINT, on_terminate);
    signal(SIGTERM, on_terminate);

    printf("\033[?1049h\033[H\033[2J");   /* alternate screen buffer */

    sb_push(&sb, "\033[1;36mZestyDB console\033[0m - type help for"
                 " commands, CTRL+X exits");
    refresh_server_info();

    while (!g_exit_requested) {
        render(&sb, &input);

        if (g_winch_flag) {
            g_winch_flag = 0;   /* redraw with the new size */
        }

        struct pollfd pfd = { STDIN_FILENO, POLLIN, 0 };
        int pr = poll(&pfd, 1, 5000);
        if (g_exit_requested) {
            break;
        }
        if (pr < 0) {
            if (errno == EINTR) {
                continue;
            }
            break;
        }
        if (pr == 0) {
            refresh_server_info();   /* periodic live update */
            continue;
        }

        int k = read_key();
        if (k < 0) {
            break;   /* EOF */
        }

        switch (k) {
        case 24:   /* CTRL+X: exit */
            g_exit_requested = true;
            break;

        case 12:   /* CTRL+L: clear */
            sb_free(&sb);
            sb_push(&sb, "\033[1mcleared\033[0m");
            refresh_server_info();
            input_clear(&input);
            hist_pos = -1;
            break;

        case 3:   /* CTRL+C: cancel current line */
            input_clear(&input);
            hist_pos = -1;
            break;

        case 1:   /* CTRL+A */
            input.cursor = 0;
            break;

        case 5:   /* CTRL+E */
            input.cursor = (int)strlen(input.input);
            break;

        case KEY_LEFT:
            if (input.cursor > 0) input.cursor--;
            break;

        case KEY_RIGHT:
            if (input.cursor < (int)strlen(input.input)) input.cursor++;
            break;

        case KEY_HOME:
            input.cursor = 0;
            break;

        case KEY_END:
            input.cursor = (int)strlen(input.input);
            break;

        case KEY_DELETE:
            input_delete(&input);
            break;

        case KEY_UP:
            if (hist.count == 0) {
                break;
            }
            if (hist_pos == -1) {
                snprintf(draft, sizeof(draft), "%s", input.input);
                hist_pos = hist.count - 1;
            } else if (hist_pos > 0) {
                hist_pos--;
            }
            snprintf(input.input, sizeof(input.input), "%s",
                     hist.items[hist_pos]);
            input.cursor = (int)strlen(input.input);
            break;

        case KEY_DOWN:
            if (hist_pos == -1) {
                break;
            }
            hist_pos++;
            if (hist_pos >= hist.count) {
                hist_pos = -1;
                snprintf(input.input, sizeof(input.input), "%s", draft);
            } else {
                snprintf(input.input, sizeof(input.input), "%s",
                         hist.items[hist_pos]);
            }
            input.cursor = (int)strlen(input.input);
            break;

        case 127:
        case 8:
            input_backspace(&input);
            break;

        case '\r':
        case '\n':
            if (input.input[0] == '\0') {
                break;
            }
            sb_printf(&sb, "\033[33m> %s\033[0m", input.input);
            history_push(&hist, input.input);

            char work[sizeof(input.input)];
            snprintf(work, sizeof(work), "%s", input.input);
            char *args[MAX_ARGS];
            execute_command(split_command(work, args), args);

            refresh_server_info();   /* keep the panel live */
            input_clear(&input);
            hist_pos = -1;
            draft[0] = '\0';
            break;

        default:
            if (k >= 32 && k < 127) {
                input_insert(&input, (char)k);
            }
            break;
        }
    }

    g_output = NULL;

    disable_raw_mode();
    printf("\033[0m\033[?1049l");   /* leave alternate screen */
    fflush(stdout);

    sb_free(&sb);
    history_free(&hist);
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
            g_host = g_host ? g_host : "127.0.0.1";   /* -p implies TCP */
        } else if ((strcmp(argv[argi], "-u") == 0 ||
                    strcmp(argv[argi], "--user") == 0) &&
                   argi + 1 < argc) {
            g_user = argv[++argi];
        } else {
            print_usage();
            return 1;
        }
        argi++;
    }

    if (argi >= argc) {
        shell_loop();      /* no command: full-screen console */
        return 0;
    }

    return execute_command(argc - argi, argv + argi);
}
