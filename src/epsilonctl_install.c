/* epsilonctl_install.c - interactive service install/setup routines
 * (launchd agent writer + questionnaire). Part of the epsilonctl module;
 * see epsilonctl_internal.h.
 */

#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
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

/* Creates (or recreates) the local launchd service unit. `here` is the
 * directory containing the epsilond binary; the data dir defaults to
 * <here>/data when NULL. Returns true on success. */
static bool install_launchd(const char *here, const char *bind, int port,
                            int peer_port, const char *advertise,
                            const char *data_dir, const char *log_path)
{
    char binpath[1024];
    snprintf(binpath, sizeof(binpath), "%s/epsilond", here);
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

/* One-shot install: ask for the server parameters, write the launchd
 * service and report how to open ports / connect. */
int cmd_install(int argc, char **argv)
{
    (void)argc;
    (void)argv;
    char bind[64] = "127.0.0.1";
    int port = 8123;
    int peer_port = 0;
    char advertise[64] = "127.0.0.1";
    char data_dir[1024];
    char log_path[1024];
    char here[1024];

    printf("\nEpsilonDB setup\n");
    printf("---------------\n");
    printf("This configures a local service. Answer the questions below;\n");
    printf("defaults are shown in [brackets].\n\n");

    ask_addr("HTTP bind address", bind, sizeof(bind), "127.0.0.1");
    port = ask_int("HTTP port", 8123);
    peer_port = ask_int("Cluster peer port (0 = single node)", 0);
    if (peer_port > 0) {
        ask_addr("Advertised address (reachable by other nodes)",
                 advertise, sizeof(advertise), bind);
    } else {
        snprintf(advertise, sizeof(advertise), "%s", bind);
    }
    if (getcwd(here, sizeof(here))) {
        snprintf(data_dir, sizeof(data_dir), "%s/data", here);
    } else {
        snprintf(data_dir, sizeof(data_dir), "./data");
    }
    if (!ask_yes_no("Use the data directory", true)) {
        ask_addr("Data directory", data_dir, sizeof(data_dir), data_dir);
    }
    snprintf(log_path, sizeof(log_path), "/var/log/epsilondb/epsilondb.log");
    if (!ask_yes_no("Use the default log path", true)) {
        ask_addr("Log file path", log_path, sizeof(log_path), log_path);
    }

    printf("\nInstalling the service...\n");
    if (!install_launchd(here, bind, port, peer_port, advertise,
                         data_dir, log_path)) {
        return 1;
    }
    printf("  run it with:\n");
    printf("    launchctl load -w %s/Library/LaunchAgents/com.epsilondb.server.plist\n",
           getenv("HOME") ? getenv("HOME") : ".");
    printf("  or start it in the foreground with:\n");
    printf("    %s/epsilond -p %d -b %s %s-d %s -l %s\n",
           here, port, bind, peer_arg_text(peer_port, advertise),
           data_dir, log_path);

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
    (void)argc;
    (void)argv;
    printf("\nEpsilonDB reconfiguration\n");
    printf("--------------------------\n");
    printf("Re-ask the server parameters, update the launchd service and\n");
    printf("push the new values to the running server's settings.\n\n");

    char bind[64] = "127.0.0.1";
    int port = 8123;
    int peer_port = 0;
    char advertise[64] = "127.0.0.1";
    char data_dir[1024];
    char log_path[1024];
    char here[1024];

    if (getcwd(here, sizeof(here))) {
        snprintf(data_dir, sizeof(data_dir), "%s/data", here);
    } else {
        snprintf(data_dir, sizeof(data_dir), "./data");
    }
    snprintf(log_path, sizeof(log_path), "/var/log/epsilondb/epsilondb.log");

    ask_addr("HTTP bind address", bind, sizeof(bind), "127.0.0.1");
    port = ask_int("HTTP port", 8123);
    peer_port = ask_int("Cluster peer port (0 = single node)", 0);
    if (peer_port > 0) {
        ask_addr("Advertised address (reachable by other nodes)",
                 advertise, sizeof(advertise), bind);
    } else {
        snprintf(advertise, sizeof(advertise), "%s", bind);
    }
    if (!ask_yes_no("Use the data directory", true)) {
        ask_addr("Data directory", data_dir, sizeof(data_dir), data_dir);
    }
    if (!ask_yes_no("Use the default log path", true)) {
        ask_addr("Log file path", log_path, sizeof(log_path), log_path);
    }

    printf("\nUpdating the service...\n");
    if (!install_launchd(here, bind, port, peer_port, advertise,
                         data_dir, log_path)) {
        return 1;
    }
    printf("  restart it with:\n");
    printf("    launchctl unload %s/Library/LaunchAgents/com.epsilondb.server.plist\n"
           "    launchctl load -w %s/Library/LaunchAgents/com.epsilondb.server.plist\n",
           getenv("HOME") ? getenv("HOME") : ".",
           getenv("HOME") ? getenv("HOME") : ".");

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