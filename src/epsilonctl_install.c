/* epsilonctl_install.c - interactive service install/setup routines
 * (launchd agent writer + questionnaire). Part of the epsilonctl module;
 * see epsilonctl_internal.h.
 */

#include <errno.h>
#include <fcntl.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <time.h>
#include <unistd.h>

#include "../vendor/cjson/cJSON.h"
#include "epsilonctl_internal.h"
static bool ask_yes_no(const char *prompt, bool default_yes)
{
    char line[64];
    printf("%s [%s]: ", prompt, default_yes ? "Y/n" : "y/N");
    fflush(stdout);
    if (!fgets(line, sizeof(line), stdin)) {
        return default_yes;
    }
    char c = 0;
    for (size_t i = 0; line[i] && i < sizeof(line); i++) {
        if (line[i] != ' ' && line[i] != '\t' && line[i] != '\n' &&
            line[i] != '\r') {
            c = line[i];
            break;
        }
    }
    if (c == 0) {
        return default_yes;
    }
    if (c == 'y' || c == 'Y') {
        return true;
    }
    if (c == 'n' || c == 'N') {
        return false;
    }
    return default_yes;
}

static int ask_int(const char *prompt, int deflt)
{
    char line[64];
    for (;;) {
        printf("%s [%d]: ", prompt, deflt);
        fflush(stdout);
        if (!fgets(line, sizeof(line), stdin)) {
            return deflt;
        }
        /* empty line: keep the default */
        if (line[0] == '\n' || line[0] == '\r' || line[0] == '\0') {
            return deflt;
        }
        char *end = NULL;
        long v = strtol(line, &end, 10);
        if (end != line && (v >= 1 && v <= 65535)) {
            return (int)v;
        }
        if (v == 0 && end != line && strchr(line, '0')) {
            return (int)v;   /* 0 = disabled (peer port) */
        }
        printf("  please enter a number between 1 and 65535\n");
    }
}

static void ask_addr(const char *prompt, char *out, size_t cap,
                     const char *deflt)
{
    char line[256];
    printf("%s [%s]: ", prompt, deflt);
    fflush(stdout);
    if (!fgets(line, sizeof(line), stdin)) {
        snprintf(out, cap, "%s", deflt);
        return;
    }
    size_t len = strlen(line);
    while (len > 0 && (line[len - 1] == '\n' || line[len - 1] == '\r')) {
        line[--len] = '\0';
    }
    if (len == 0) {
        snprintf(out, cap, "%s", deflt);
    } else {
        snprintf(out, cap, "%s", line);
    }
}

static int http_status(const char *method, const char *path,
                       const char *body)
{
    char *resp_body = NULL;
    int status = http_request(method, path, body, &resp_body);
    if (status < 0) {
        return status;
    }
    free(resp_body);
    return status;
}

/* POST/PUT a named setting (JSON body). Returns HTTP status. */
static int put_setting(const char *name, const char *json_body)
{
    char path[512];
    snprintf(path, sizeof(path), "/admin/settings/%s", name);
    return http_status("POST", path, json_body);
}

/* Opens the browser to the admin console on the current server. */
static void launch_browser(const char *host, int port)
{
    char url[512];
    snprintf(url, sizeof(url), "http://%s:%d/admin", host, port);
    printf("  Opening %s\n", url);
    fflush(stdout);
    pid_t pid = fork();
    if (pid == 0) {
        const char *cmd = access("/usr/bin/open", X_OK) == 0
                              ? "/usr/bin/open"
                              : "xdg-open";
        const char *args[] = { cmd, url, NULL };
        if (execvp(cmd, (char *const *)args) != 0) {
            _exit(127);
        }
    }
}

/* Writes every byte of buf to fd, looping on partial writes. */
static bool write_all(int fd, const void *buf, size_t len)
{
    const char *p = buf;
    while (len > 0) {
        ssize_t n = write(fd, p, len);
        if (n < 0) {
            if (errno == EINTR) {
                continue;
            }
            return false;
        }
        p += n;
        len -= (size_t)n;
    }
    return true;
}

/* Copies one file over another, replacing the destination (used to
 * install/upgrade the binaries; the destination is made executable). */
static bool copy_file_over(const char *src, const char *dst)
{
    int in = open(src, O_RDONLY);
    if (in < 0) {
        return false;
    }
    int out = open(dst, O_WRONLY | O_CREAT | O_TRUNC, 0755);
    if (out < 0) {
        close(in);
        return false;
    }
    char buf[65536];
    ssize_t n;
    bool ok = true;
    while ((n = read(in, buf, sizeof(buf))) > 0) {
        if (!write_all(out, buf, (size_t)n)) {
            ok = false;
            break;
        }
    }
    if (n < 0) {
        ok = false;
    }
    fchmod(out, 0755);
    close(out);
    close(in);
    return ok;
}

/* Locates the directory holding the built binaries, relative to the
 * epsilonctl executable or the current directory. Fills `out`. */
static void source_bin_dir(const char *argv0, char *out, size_t cap)
{
    char dir[1024] = ".";
    if (argv0 && argv0[0] && strchr(argv0, '/')) {
        snprintf(dir, sizeof(dir), "%s", argv0);
        char *slash = strrchr(dir, '/');
        if (slash == dir) {
            snprintf(dir, sizeof(dir), "/");
        } else if (slash) {
            *slash = '\0';
        }
    }
    static const char *const rels[] = { "bin", "../bin" };
    for (size_t i = 0; i < sizeof(rels) / sizeof(rels[0]); i++) {
        char cand[1100];
        snprintf(cand, sizeof(cand), "%s/%s/epsilond", dir, rels[i]);
        if (access(cand, R_OK) == 0) {
            snprintf(out, cap, "%s/%s", dir, rels[i]);
            return;
        }
    }
    if (access("bin/epsilond", R_OK) == 0) {
        snprintf(out, cap, "bin");
        return;
    }
    snprintf(out, cap, "%s/bin", dir);
}

/* Installs (or upgrades) the built binaries to /usr/bin, falling back to
 * /usr/local/bin when /usr/bin is not writable. Fills `prefix` with the
 * chosen directory. Returns the number of binaries installed. */
static int install_binaries(const char *src_dir, char *prefix, size_t cap)
{
    static const char *const names[] = { "epsilond", "epsilonctl",
                                         "epsilonbkup", "epsilonbench" };
    const char *chosen = NULL;
    if (access("/usr/bin", W_OK) == 0) {
        chosen = "/usr/bin";
    } else if (access("/usr/local/bin", W_OK) == 0) {
        chosen = "/usr/local/bin";
    } else {
        printf("  (neither /usr/bin nor /usr/local/bin is writable; "
               "binaries stay in %s)\n", src_dir);
        return 0;
    }
    snprintf(prefix, cap, "%s", chosen);
    int installed = 0;
    for (size_t i = 0; i < sizeof(names) / sizeof(names[0]); i++) {
        char src[1100], dst[1100];
        snprintf(src, sizeof(src), "%s/%s", src_dir, names[i]);
        snprintf(dst, sizeof(dst), "%s/%s", chosen, names[i]);
        if (access(src, R_OK) != 0) {
            continue;
        }
        if (copy_file_over(src, dst)) {
            installed++;
        } else {
            printf("  could not install %s to %s\n", names[i], dst);
        }
    }
    return installed;
}

/* The data directory the service should use. On Linux the system-wide
 * location is /var/lib/epsilon; elsewhere it defaults to <cwd>/data (the
 * server's own -d default creates ./data in the working directory). */
static void default_data_dir(char *out, size_t cap, const char *here)
{
#ifdef __APPLE__
    if (here && *here) {
        snprintf(out, cap, "%s/data", here);
    } else {
        snprintf(out, cap, "./data");
    }
#else
    (void)here;
    snprintf(out, cap, "/var/lib/epsilon");
#endif
}

/* Parses flag/value pairs (["-p","8123","-b","0.0.0.0",...]) into the
 * install parameters. Values that are not present are left untouched. */
static void parse_args(char **argv, int argc, char *bind, int *port,
                       int *peer_port, char *advertise, char *data_dir,
                       char *log_path)
{
    for (int i = 0; i + 1 < argc; i++) {
        if (strcmp(argv[i], "-p") == 0) {
            *port = atoi(argv[i + 1]);
        } else if (strcmp(argv[i], "-b") == 0) {
            snprintf(bind, 64, "%s", argv[i + 1]);
        } else if (strcmp(argv[i], "-n") == 0) {
            *peer_port = atoi(argv[i + 1]);
        } else if (strcmp(argv[i], "-A") == 0) {
            snprintf(advertise, 64, "%s", argv[i + 1]);
        } else if (strcmp(argv[i], "-d") == 0) {
            snprintf(data_dir, 1024, "%s", argv[i + 1]);
        } else if (strcmp(argv[i], "-l") == 0) {
            snprintf(log_path, 1024, "%s", argv[i + 1]);
        }
    }
}

/* Reads the currently installed service configuration so a re-run of
 * install/setup can keep it instead of resetting to factory defaults.
 * Fills the parameters found; returns true when a service exists. */
static bool read_existing_config(char *bind, int *port, int *peer_port,
                                 char *advertise, char *data_dir,
                                 char *log_path)
{
#ifdef __APPLE__
    char path[1024];
    snprintf(path, sizeof(path),
             "%s/Library/LaunchAgents/com.epsilondb.server.plist",
             getenv("HOME") ? getenv("HOME") : ".");
    FILE *fp = fopen(path, "r");
    if (!fp) {
        return false;
    }
    char buf[8192];
    size_t total = 0;
    size_t n;
    while (total < sizeof(buf) - 1 &&
           (n = fread(buf + total, 1, sizeof(buf) - total - 1, fp)) > 0) {
        total += n;
    }
    fclose(fp);
    buf[total] = '\0';
    char *in = strstr(buf, "<key>ProgramArguments</key>");
    char *arr = in ? strstr(in, "<array>") : NULL;
    char *end = in ? strstr(in, "</array>") : NULL;
    if (!arr || !end || end < arr) {
        return false;
    }
    char *tokens[64];
    int ntok = 0;
    char *p = arr;
    while (p < end && ntok < 64) {
        char *open = strstr(p, "<string>");
        if (!open || open >= end) {
            break;
        }
        char *close = strstr(open, "</string>");
        if (!close || close >= end) {
            break;
        }
        char *val = open + 8;
        *close = '\0';
        tokens[ntok++] = val;
        p = close + 9;
    }
    /* tokens[0] is the binary path; the rest are flag/value pairs */
    if (ntok < 2) {
        return false;
    }
    parse_args(tokens + 1, ntok - 1, bind, port, peer_port, advertise,
               data_dir, log_path);
    return true;
#else
    const char *path = "/etc/systemd/system/epsilon.service";
    FILE *fp = fopen(path, "r");
    if (!fp) {
        return false;
    }
    char buf[8192];
    size_t total = 0;
    size_t n;
    while (total < sizeof(buf) - 1 &&
           (n = fread(buf + total, 1, sizeof(buf) - total - 1, fp)) > 0) {
        total += n;
    }
    fclose(fp);
    buf[total] = '\0';
    char *start = strstr(buf, "ExecStart=");
    char *end = strstr(buf, "\n[Install]");
    if (!start) {
        return false;
    }
    char *stop = end ? end : buf + total;
    char *tokens[64];
    int ntok = 0;
    char *p = start + 10;   /* after "ExecStart=" */
    char *save = NULL;
    char *tok = strtok_r(p, " \t\n", &save);
    while (tok && ntok < 64 && tok < stop) {
        if (tok >= stop) {
            break;
        }
        tokens[ntok++] = tok;
        tok = strtok_r(NULL, " \t\n", &save);
    }
    if (ntok < 2) {
        return false;
    }
    parse_args(tokens + 1, ntok - 1, bind, port, peer_port, advertise,
               data_dir, log_path);
    return true;
#endif
}


#ifdef __APPLE__
/* Creates (or recreates) the local launchd service unit. `here` is the
 * working directory for the service and `binpath` the absolute path of
 * the epsilond binary to run (usually the installed /usr/bin copy, so
 * re-running install upgrades the service binary). Returns true on
 * success. */
static bool install_launchd(const char *here, const char *binpath,
                            const char *bind, int port, int peer_port,
                            const char *advertise, const char *data_dir,
                            const char *log_path)
{
    char data_elems[512];
    snprintf(data_elems, sizeof(data_elems),
             "    <string>-d</string><string>%s</string>\n", data_dir);
    char peer_elems[1024] = "";
    if (peer_port > 0) {
        char n_el[256], a_el[256];
        snprintf(n_el, sizeof(n_el),
                 "    <string>-n</string><string>%d</string>\n", peer_port);
        snprintf(a_el, sizeof(a_el),
                 "    <string>-A</string><string>%s</string>\n", advertise);
        snprintf(peer_elems, sizeof(peer_elems), "%s%s", n_el, a_el);
    }
    char plist[4096];
    snprintf(plist, sizeof(plist),
             "<?xml version=\"1.0\" encoding=\"UTF-8\"?>\n"
             "<!DOCTYPE plist PUBLIC \"-//Apple//DTD PLIST 1.0//EN\" "
             "\"http://www.apple.com/DTDs/PropertyList-1.0.dtd\">\n"
             "<plist version=\"1.0\">\n"
             "<dict>\n"
             "  <key>Label</key>\n"
             "  <string>com.epsilondb.server</string>\n"
             "  <key>ProgramArguments</key>\n"
             "  <array>\n"
             "    <string>%s</string>\n"
             "    <string>-p</string><string>%d</string>\n"
             "    <string>-b</string><string>%s</string>\n"
             "%s"
             "%s"
             "    <string>-l</string><string>%s</string>\n"
             "  </array>\n"
             "  <key>RunAtLoad</key><true/>\n"
             "  <key>KeepAlive</key><true/>\n"
             "  <key>StandardOutPath</key><string>%s</string>\n"
             "  <key>StandardErrorPath</key><string>%s</string>\n"
             "  <key>WorkingDirectory</key><string>%s</string>\n"
             "</dict>\n"
             "</plist>\n",
             binpath, port, bind, data_elems, peer_elems, log_path, log_path,
             log_path, here);
    char path[1024];
    snprintf(path, sizeof(path),
             "%s/Library/LaunchAgents/com.epsilondb.server.plist",
             getenv("HOME") ? getenv("HOME") : ".");
    FILE *fp = fopen(path, "w");
    if (!fp) {
        printf("  could not write %s\n", path);
        return false;
    }
    fputs(plist, fp);
    fclose(fp);
    printf("  wrote %s\n", path);
    return true;
}

#else
/* Creates a directory if it does not exist yet. */
static void ensure_dir(const char *path)
{
    if (mkdir(path, 0750) != 0 && errno != EEXIST) {
        printf("  (could not create %s: %s)\n", path, strerror(errno));
    }
}

/* Builds the systemd unit file text for the server. */
static void systemd_unit_text(char *out, size_t cap, const char *binpath,
                              const char *bind, int port, int peer_port,
                              const char *advertise, const char *data_dir,
                              const char *log_path)
{
    char peer_extra[256] = "";
    if (peer_port > 0) {
        snprintf(peer_extra, sizeof(peer_extra),
                 "  -n %d -A %s\n", peer_port, advertise);
    }
    snprintf(out, cap,
             "[Unit]\n"
             "Description=EpsilonDB distributed key/value database server\n"
             "After=network.target\n"
             "\n"
             "[Service]\n"
             "Type=simple\n"
             "ExecStart=%s -p %d -b %s -d %s -l %s\n"
             "%s"
             "Restart=always\n"
             "RestartSec=3\n"
             "\n"
             "[Install]\n"
             "WantedBy=multi-user.target\n",
             binpath, port, bind, data_dir, log_path, peer_extra);
}

/* Writes and activates a systemd unit for the server. The unit passes
 * the data path and all ports through to epsilond, so re-running install
 * after a rebuild upgrades the running service. Returns true when the
 * unit was written (activation may need privileges). */
static bool install_systemd(const char *binpath, const char *bind, int port,
                            int peer_port, const char *advertise,
                            const char *data_dir, const char *log_path)
{
    ensure_dir(data_dir);
    char logdir[1024];
    snprintf(logdir, sizeof(logdir), "%s", log_path);
    char *slash = strrchr(logdir, '/');
    if (slash) {
        *slash = '\0';
        ensure_dir(logdir);
    }

    char unit[4096];
    systemd_unit_text(unit, sizeof(unit), binpath, bind, port, peer_port,
                      advertise, data_dir, log_path);

    const char *path = "/etc/systemd/system/epsilon.service";
    FILE *fp = fopen(path, "w");
    if (!fp) {
        printf("  could not write %s (run as root?); unit file:\n%s\n",
               path, unit);
        printf("  activate with:\n");
        printf("    sudo tee %s > /dev/null <<'EOF'\n%sEOF\n", path, unit);
        printf("    sudo systemctl daemon-reload && "
               "sudo systemctl enable --now epsilon\n");
        return false;
    }
    fputs(unit, fp);
    fclose(fp);
    printf("  wrote %s\n", path);

    if (system("systemctl daemon-reload >/dev/null 2>&1") != 0) {
        printf("  (systemctl not available; enable manually with "
               "`systemctl enable --now epsilon`)\n");
        return true;
    }
    if (system("systemctl enable --now epsilon >/dev/null 2>&1") != 0) {
        printf("  (could not enable/start the service; check "
               "`systemctl status epsilon`)\n");
        return true;
    }
    printf("  service enabled and started (systemctl status epsilon)\n");
    return true;
}

#endif
/* Platform service installer: launchd on macOS, systemd on Linux. */
static bool install_service(const char *here, const char *binpath,
                            const char *bind, int port, int peer_port,
                            const char *advertise, const char *data_dir,
                            const char *log_path)
{
#ifdef __APPLE__
    return install_launchd(here, binpath, bind, port, peer_port, advertise,
                           data_dir, log_path);
#else
    (void)here;
    return install_systemd(binpath, bind, port, peer_port, advertise,
                           data_dir, log_path);
#endif
}

static const char *peer_arg_text(int peer_port, const char *advertise)
{
    static char buf[160];
    if (peer_port > 0) {
        snprintf(buf, sizeof(buf), "-n %d -A %s ", peer_port, advertise);
    } else {
        buf[0] = '\0';
    }
    return buf;
}

/* One-shot install: ask for the server parameters, copy the binaries to
 * /usr/bin, write the launchd service and report how to open ports /
 * connect. Re-running it (or setup) after a rebuild upgrades the
 * installed binaries. */
int cmd_install(int argc, char **argv)
{
    char bind[64] = "127.0.0.1";
    int port = 8123;
    int peer_port = 0;
    char advertise[64] = "127.0.0.1";
    char data_dir[1024];
    char log_path[1024];
    char here[1024];

    if (getcwd(here, sizeof(here))) {
        default_data_dir(data_dir, sizeof(data_dir), here);
    } else {
        default_data_dir(data_dir, sizeof(data_dir), ".");
    }
    snprintf(log_path, sizeof(log_path), "/var/log/epsilondb/epsilondb.log");

    /* an existing service means this is an upgrade: keep its settings as
     * the defaults so pressing Enter preserves the current configuration */
    bool existing = read_existing_config(bind, &port, &peer_port, advertise,
                                         data_dir, log_path);

    printf("\nEpsilonDB setup\n");
    printf("---------------\n");
    printf("This configures a local service. Answer the questions below;\n");
    printf("defaults are shown in [brackets].\n");
    if (existing) {
        printf("An existing service was found: its settings are the defaults,\n");
        printf("so pressing Enter keeps the current configuration.\n");
    }
    printf("\n");

    ask_addr("HTTP bind address", bind, sizeof(bind), bind);
    port = ask_int("HTTP port", port);
    peer_port = ask_int("Cluster peer port (0 = single node)", peer_port);
    if (peer_port > 0) {
        ask_addr("Advertised address (reachable by other nodes)",
                 advertise, sizeof(advertise), advertise);
    } else {
        snprintf(advertise, sizeof(advertise), "%s", bind);
    }
    if (!ask_yes_no("Use the data directory", true)) {
        ask_addr("Data directory", data_dir, sizeof(data_dir), data_dir);
    }
    if (!ask_yes_no("Use the default log path", true)) {
        ask_addr("Log file path", log_path, sizeof(log_path), log_path);
    }

    printf("\nInstalling the binaries...\n");
    char src_dir[1024], prefix[64];
    source_bin_dir(argc > 0 ? argv[0] : NULL, src_dir, sizeof(src_dir));
    int installed = install_binaries(src_dir, prefix, sizeof(prefix));
    if (installed > 0) {
        printf("  installed %d binaries in %s\n", installed, prefix);
    }

    printf("\nInstalling the service...\n");
    char binpath[1024];
    if (installed > 0) {
        snprintf(binpath, sizeof(binpath), "%s/epsilond", prefix);
    } else {
        snprintf(binpath, sizeof(binpath), "%s/epsilond", src_dir);
    }
    if (!install_service(here, binpath, bind, port, peer_port, advertise,
                         data_dir, log_path)) {
        return 1;
    }
#ifdef __APPLE__
    printf("  run it with:\n");
    printf("    launchctl load -w %s/Library/LaunchAgents/com.epsilondb.server.plist\n",
           getenv("HOME") ? getenv("HOME") : ".");
#else
    printf("  manage the service with:\n");
    printf("    systemctl status|restart|stop epsilon\n");
#endif
    printf("  or start it in the foreground with:\n");
    printf("    %s -p %d -b %s %s-d %s -l %s\n",
           binpath, port, bind, peer_arg_text(peer_port, advertise),
           data_dir, log_path);
    if (installed > 0) {
        printf("  note: rebuild and re-run `epsilonctl install` to upgrade "
               "the installed binaries\n");
    }

    printf("\nPorts / firewall:\n");
    printf("  - open TCP %d (HTTP API + admin console) to anyone who needs it\n",
           port);
    if (peer_port > 0) {
        printf("  - open TCP %d between cluster nodes (node-to-node mesh)\n",
               peer_port);
    }
    printf("  - put a TLS-terminating reverse proxy (nginx/caddy) on %d for\n"
           "    encrypted remote access; the server itself has no TLS\n",
           port);
    printf("  - the Unix admin socket stays local-only\n");

    printf("\nConnect:\n");
    printf("  Browser:  http://%s:%d/admin   (first visit creates the admin user)\n",
           bind, port);
    printf("  CLI:      epsilonctl -h %s -p %d\n", bind, port);
    printf("  REST:     http://%s:%d\n", bind, port);
    printf("\nOpen the admin console now?\n");
    if (ask_yes_no("Open browser", true)) {
        launch_browser(bind, port);
    }
    return 0;
}

/* Interactive re-run: ask again and update the service + persisted
 * settings. */
int cmd_setup(int argc, char **argv)
{
    printf("\nEpsilonDB reconfiguration\n");
    printf("--------------------------\n");
    printf("Re-ask the server parameters, upgrade the installed binaries,\n");
    printf("update the launchd service and push the new values to the\n");
    printf("running server's settings.\n\n");

    char bind[64] = "127.0.0.1";
    int port = 8123;
    int peer_port = 0;
    char advertise[64] = "127.0.0.1";
    char data_dir[1024];
    char log_path[1024];
    char here[1024];

    if (getcwd(here, sizeof(here))) {
        default_data_dir(data_dir, sizeof(data_dir), here);
    } else {
        default_data_dir(data_dir, sizeof(data_dir), ".");
    }
    snprintf(log_path, sizeof(log_path), "/var/log/epsilondb/epsilondb.log");

    /* re-run: keep the existing service's settings as the defaults */
    read_existing_config(bind, &port, &peer_port, advertise, data_dir,
                         log_path);

    ask_addr("HTTP bind address", bind, sizeof(bind), bind);
    port = ask_int("HTTP port", port);
    peer_port = ask_int("Cluster peer port (0 = single node)", peer_port);
    if (peer_port > 0) {
        ask_addr("Advertised address (reachable by other nodes)",
                 advertise, sizeof(advertise), advertise);
    } else {
        snprintf(advertise, sizeof(advertise), "%s", bind);
    }
    if (!ask_yes_no("Use the data directory", true)) {
        ask_addr("Data directory", data_dir, sizeof(data_dir), data_dir);
    }
    if (!ask_yes_no("Use the default log path", true)) {
        ask_addr("Log file path", log_path, sizeof(log_path), log_path);
    }

    printf("\nUpgrading the installed binaries...\n");
    char src_dir[1024], prefix[64];
    source_bin_dir(argc > 0 ? argv[0] : NULL, src_dir, sizeof(src_dir));
    int installed = install_binaries(src_dir, prefix, sizeof(prefix));
    if (installed > 0) {
        printf("  upgraded %d binaries in %s\n", installed, prefix);
    }

    printf("\nUpdating the service...\n");
    char binpath[1024];
    if (installed > 0) {
        snprintf(binpath, sizeof(binpath), "%s/epsilond", prefix);
    } else {
        snprintf(binpath, sizeof(binpath), "%s/epsilond", src_dir);
    }
    if (!install_service(here, binpath, bind, port, peer_port, advertise,
                         data_dir, log_path)) {
        return 1;
    }
#ifdef __APPLE__
    printf("  restart it with:\n");
    printf("    launchctl unload %s/Library/LaunchAgents/com.epsilondb.server.plist\n"
           "    launchctl load -w %s/Library/LaunchAgents/com.epsilondb.server.plist\n",
           getenv("HOME") ? getenv("HOME") : ".",
           getenv("HOME") ? getenv("HOME") : ".");
#else
    printf("  restart it with:\n");
    printf("    sudo systemctl restart epsilon\n");
#endif

    printf("\nUpdating the running server...\n");
    char body[512];
    int failures = 0;
    snprintf(body, sizeof(body), "{\"bind\":\"%s\"}", bind);
    if (put_setting("server.bind", body) >= 300) {
        failures++;
    }
    snprintf(body, sizeof(body), "%d", port);
    if (put_setting("server.http_port", body) >= 300) {
        failures++;
    }
    snprintf(body, sizeof(body), "%d", peer_port);
    if (put_setting("server.peer_port", body) >= 300) {
        failures++;
    }
    snprintf(body, sizeof(body), "{\"addr\":\"%s\"}", advertise);
    if (put_setting("server.advertise", body) >= 300) {
        failures++;
    }
    snprintf(body, sizeof(body), "{\"path\":\"%s\"}", data_dir);
    if (put_setting("server.data_dir", body) >= 300) {
        failures++;
    }
    snprintf(body, sizeof(body), "{\"path\":\"%s\"}", log_path);
    if (put_setting("server.log_path", body) >= 300) {
        failures++;
    }
    if (failures > 0) {
        printf("  (could not reach the server; %d setting(s) were not updated)\n",
               failures);
    } else {
        printf("  pushed bind, http_port, peer_port, advertise, data_dir, "
               "log_path\n");
        printf("  (note: restart the service for these to take effect)\n");
    }
    return 0;
}

/* ------------------------------------------------------------------ */