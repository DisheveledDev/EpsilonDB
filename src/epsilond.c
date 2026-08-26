/* epsilond - EpsilonDB server entrypoint.
 *
 * Usage: epsilond [-p port] [-b bind_addr] [-d data_dir] [-a admin_dir]
 *               [-s admin_socket] [-n peer_port] [-A advertise_addr]
 *
 * Defaults: port 8123, bind 0.0.0.0, data dir ./data, admin ./admin,
 *           admin socket ./epsilon-admin.sock, clustering disabled.
 *
 * The HTTP port serves client REST traffic (authenticated). The Unix
 * domain admin socket serves local management traffic (epsilonctl): no
 * authentication, full admin rights. With -n the node also serves its
 * raw peer socket and joins/forms a cluster mesh (stage 4).
 */

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

#include "admin/admin_console.h"
#include "api/epsilon_api.h"
#include "engine/epsilon_config.h"
#include "epsilon_banner.h"
#include "httpd/epsilon_http.h"
#include "socket/epsilon_cluster.h"
#include "epsilon_log.h"

#define DEFAULT_ADMIN_SOCK "epsilon-admin.sock"
#define DEFAULT_LOG_PATH   "/var/log/epsilondb/epsilondb.log"

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
}

/* ------------------------------------------------------------------ */
/* banner                                                              */

#define C_RESET  "\033[0m"
#define C_BOLD   "\033[1m"
#define C_GREEN  "\033[32m"
#define C_YELLOW "\033[33m"

static bool use_colour(void)
{
    return isatty(STDOUT_FILENO);
}

static void print_banner(void)
{
    const char *g = use_colour() ? C_GREEN : "";
    const char *b = use_colour() ? C_BOLD : "";
    const char *y = use_colour() ? C_YELLOW : "";
    const char *r = use_colour() ? C_RESET : "";

    printf("%s%s", b, g);
    for (int i = 0; i < EDB_BANNER_LINES; i++) {
        printf("%s\n", edb_banner[i]);
    }
    printf("%s%s", y, r);
    printf("        distributed key/value database server\n");
    printf("%s", r);
}

/* Prints a short how-to-connect notice after the server is up. The peer
 * port (clustering) may be 0 = disabled. */
static void print_connect_info(const char *bind, int port, int peer_port,
                               const char *data_dir)
{
    const char *g = use_colour() ? C_GREEN : "";
    const char *y = use_colour() ? C_YELLOW : "";
    const char *r = use_colour() ? C_RESET : "";

    printf("\n%sEpsilonDB is running.%s\n", g, r);
    printf("  Admin console:   %shttp://%s:%d/admin%s (browser UI; first visit\n",
           g, bind, port, r);
    printf("                   creates the admin user, then sign in)\n");
    printf("  REST API:        %shttp://%s:%d%s\n", g, bind, port, r);
    printf("  Local admin CLI: %sepsilonctl%s (or: epsilonctl -h %s -p %d)\n",
           g, r, bind, port);
    printf("  Data directory:  %s\n", data_dir);
    printf("\n%sFirewall / remote access:%s\n", y, r);
    printf("  - %s%s:%d%s must be reachable by anyone using the API or console.\n",
           g, bind, port, r);
    printf("  - TLS is terminated by a reverse proxy in front of the HTTP port\n");
    printf("    (no TLS in the server); put nginx/caddy on %d if you need it.\n",
           port);
    printf("  - The Unix admin socket (epsilonctl default) is local-only and is\n");
    printf("    never exposed over the network.\n");
    if (peer_port > 0) {
        printf("  - Cluster peer port %d must be open between nodes\n",
               peer_port);
    }
    printf("%s", r);
    fflush(stdout);
}

static void print_usage(const char *prog)
{
    printf("EpsilonDB - distributed key/value database server\n");
    printf("\n");
    printf("Usage: %s [options]\n", prog);
    printf("\n");
    printf("Network (HTTP client-facing port):\n");
    printf("  -p, --port <port>       HTTP REST API port (default: 8123)\n");
    printf("  -b, --bind <addr>       address to bind the HTTP port to\n");
    printf("                          (default: 0.0.0.0, all interfaces)\n");
    printf("  -s, --socket <path>     Unix admin socket for epsilonctl (trusted,\n");
    printf("                          no auth; default: ./epsilon-admin.sock)\n");
    printf("\n");
    printf("Clustering (node-to-node mesh):\n");
    printf("  -n, --peer-port <port>  enable clustering and bind this raw peer\n");
    printf("                          port (default: clustering disabled); the\n");
    printf("                          peer listener always binds 0.0.0.0\n");
    printf("  -A, --advertise <addr>  address advertised to other nodes\n");
    printf("                          (default: 127.0.0.1)\n");
    printf("\n");
    printf("Storage:\n");
    printf("  -d, --data <dir>        data directory holding one SQLite file per\n");
    printf("                          shard (default: ./data)\n");
    printf("  -a, --admin <dir>       admin console directory (default: ./admin)\n");
    printf("\n");
    printf("Logging:\n");
    printf("  -l, --log <file>        log file path\n");
    printf("                          (default: /var/log/epsilondb/epsilondb.log;\n");
    printf("                          falls back to console if not writable)\n");
    printf("\n");
    printf("Misc:\n");
    printf("  -h, --help              show this help and exit\n");
    printf("\n");
    printf("TLS is terminated by a reverse proxy in front of the HTTP port.\n");
    printf("Before any user exists, unauthenticated HTTP requests run with full\n");
    printf("rights so the first admin can be created (or use the admin socket).\n");
}

int main(int argc, char **argv)
{
    int port = 8123;
    const char *bind_addr = NULL;
    const char *data_dir = "data";
    const char *admin_dir = "admin";
    const char *admin_sock = DEFAULT_ADMIN_SOCK;
    int peer_port = 0;              /* 0 = clustering disabled */
    const char *advertise_addr = "127.0.0.1";
    const char *log_path = DEFAULT_LOG_PATH;

    for (int i = 1; i < argc; i++) {
        if ((strcmp(argv[i], "-p") == 0 || strcmp(argv[i], "--port") == 0) &&
            i + 1 < argc) {
            port = atoi(argv[++i]);
        } else if ((strcmp(argv[i], "-b") == 0 ||
                    strcmp(argv[i], "--bind") == 0) && i + 1 < argc) {
            bind_addr = argv[++i];
        } else if ((strcmp(argv[i], "-d") == 0 ||
                    strcmp(argv[i], "--data") == 0) && i + 1 < argc) {
            data_dir = argv[++i];
        } else if ((strcmp(argv[i], "-a") == 0 ||
                    strcmp(argv[i], "--admin") == 0) && i + 1 < argc) {
            admin_dir = argv[++i];
        } else if ((strcmp(argv[i], "-s") == 0 ||
                    strcmp(argv[i], "--socket") == 0) && i + 1 < argc) {
            admin_sock = argv[++i];
        } else if ((strcmp(argv[i], "-n") == 0 ||
                    strcmp(argv[i], "--peer-port") == 0) && i + 1 < argc) {
            peer_port = atoi(argv[++i]);
        } else if ((strcmp(argv[i], "-A") == 0 ||
                    strcmp(argv[i], "--advertise") == 0) && i + 1 < argc) {
            advertise_addr = argv[++i];
        } else if ((strcmp(argv[i], "-l") == 0 ||
                    strcmp(argv[i], "--log") == 0) && i + 1 < argc) {
            log_path = argv[++i];
        } else if (strcmp(argv[i], "-h") == 0 ||
                   strcmp(argv[i], "--help") == 0) {
            print_usage(argv[0]);
            return 0;
        }
    }

    edb_log_open(log_path);
    print_banner();

    edb_engine *engine = edb_engine_open(data_dir);
    if (!engine) {
        edb_log("ERROR", "failed to open data directory '%s'", data_dir);
        edb_log_close();
        return 1;
    }
    edb_config *config = edb_config_open(engine);
    if (!config) {
        edb_log("ERROR", "failed to open config store");
        edb_engine_close(engine);
        edb_log_close();
        return 1;
    }

    /* validate ports before anything binds */
    if (port < 1 || port > 65535) {
        edb_log("ERROR", "invalid port %d", port);
        edb_config_close(config);
        edb_engine_close(engine);
        edb_log_close();
        return 1;
    }
    if (peer_port < 0 || peer_port > 65535) {
        edb_log("ERROR", "invalid peer port %d", peer_port);
        edb_config_close(config);
        edb_engine_close(engine);
        edb_log_close();
        return 1;
    }

    edb_cluster *cluster = NULL;
    edb_repl *repl = NULL;
    char node_id[EDB_NODE_ID_MAX] = "";
    if (peer_port > 0) {
        cluster = edb_cluster_start(config, advertise_addr, peer_port,
                                    node_id);
        if (!cluster) {
            edb_log("ERROR", "failed to start cluster service on peer port %d",
                 peer_port);
            edb_config_close(config);
            edb_engine_close(engine);
            edb_log_close();
            return 1;
        }
        edb_cluster_set_http_port(cluster, port);
        edb_log("INFO", "cluster node %s advertising %s:%d (peer port %d)",
             node_id, advertise_addr, peer_port, peer_port);

        /* re-enable mesh encryption from the persisted key (restart) */
        {
            uint8_t enc_key[32];
            uint8_t mac_key[32];
            if (edb_cluster_load_keys(data_dir, enc_key, mac_key)) {
                estp_set_mesh_key(enc_key, mac_key);
            }
        }

        /* stage 5 replication on top of the mesh */
        repl = edb_repl_start(cluster, config, data_dir);
        if (!repl) {
            edb_log("ERROR", "failed to start replication service");
            edb_cluster_stop(cluster);
            edb_config_close(config);
            edb_engine_close(engine);
            edb_log_close();
            return 1;
        }
    }
    edb_api_set_cluster(cluster);
    edb_api_set_repl(repl);

    edb_http_server *srv = edb_http_start(bind_addr, port);
    if (!srv) {
        edb_log("ERROR", "failed to bind HTTP port %d", port);
        edb_repl_stop(repl);
        edb_cluster_stop(cluster);
        edb_config_close(config);
        edb_engine_close(engine);
        edb_log_close();
        return 1;
    }

    if (!edb_admin_console_register(srv)) {
        edb_log("WARN", "failed to register admin console");
    }
    (void)admin_dir;

    print_connect_info(bind_addr ? bind_addr : "0.0.0.0", port, peer_port,
                       data_dir);

    if (!edb_api_register(srv, engine, config)) {
        edb_log("ERROR", "failed to register API routes");
        edb_http_stop(srv);
        edb_repl_stop(repl);
        edb_cluster_stop(cluster);
        edb_config_close(config);
        edb_engine_close(engine);
        edb_log_close();
        return 1;
    }

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    /* local admin socket: epsilonctl connects here, trusted, no auth */
    if (admin_sock && !edb_http_start_admin(srv, admin_sock)) {
        edb_log("WARN", "admin socket '%s' unavailable",
             admin_sock);
    }

    /* workload/performance analytics recorder */
    edb_api_analytics_start(config, node_id);

    edb_log("INFO", "listening on port %d, data in '%s', admin socket '%s'",
         port, data_dir, admin_sock ? admin_sock : "disabled");

    /* The accept loop runs inside its own thread started by
     * edb_http_start's design; wait here for shutdown. */
    int tick = 0;
    while (!g_stop) {
        if (++tick % 10 == 0) {
            edb_log_rotate_if_needed();
        }
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    edb_log("INFO", "shutting down");
    edb_http_stop(srv);
    edb_api_analytics_stop();
    edb_api_set_cluster(NULL);
    edb_api_set_repl(NULL);
    edb_repl_stop(repl);
    edb_cluster_stop(cluster);
    edb_config_close(config);
    edb_engine_close(engine);
    edb_log_close();
    return 0;
}
