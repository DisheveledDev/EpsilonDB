/* zestyd - ZestyDB server entrypoint.
 *
 * Usage: zestyd [-p port] [-b bind_addr] [-d data_dir] [-a admin_dir]
 *               [-s admin_socket] [-n peer_port] [-A advertise_addr]
 *
 * Defaults: port 8123, bind 0.0.0.0, data dir ./data, admin ./admin,
 *           admin socket ./zesty-admin.sock, clustering disabled.
 *
 * The HTTP port serves client REST traffic (authenticated). The Unix
 * domain admin socket serves local management traffic (zestyctl): no
 * authentication, full admin rights. With -n the node also serves its
 * raw peer socket and joins/forms a cluster mesh (stage 4).
 */

#include <signal.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "api/zesty_api.h"
#include "engine/zesty_config.h"
#include "httpd/zesty_http.h"
#include "socket/zesty_cluster.h"

#define DEFAULT_ADMIN_SOCK "zesty-admin.sock"

static volatile sig_atomic_t g_stop = 0;

static void on_signal(int sig)
{
    (void)sig;
    g_stop = 1;
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
        } else if (strcmp(argv[i], "-h") == 0 ||
                   strcmp(argv[i], "--help") == 0) {
            printf("usage: %s [-p port] [-b addr] [-d data_dir]"
                   " [-a admin_dir] [-s admin_socket] [-n peer_port]"
                   " [-A advertise_addr]\n",
                   argv[0]);
            return 0;
        }
    }

    zdb_engine *engine = zdb_engine_open(data_dir);
    if (!engine) {
        fprintf(stderr, "zestyd: failed to open data directory '%s'\n",
                data_dir);
        return 1;
    }
    zdb_config *config = zdb_config_open(engine);
    if (!config) {
        fprintf(stderr, "zestyd: failed to open config store\n");
        zdb_engine_close(engine);
        return 1;
    }

    zdb_http_server *srv = zdb_http_start(bind_addr, port);
    if (!srv) {
        zdb_config_close(config);
        zdb_engine_close(engine);
        return 1;
    }

    /* static UI is optional; absence is not fatal */
    zdb_http_serve_static(srv, "/admin", admin_dir);

    if (!zdb_api_register(srv, engine, config)) {
        fprintf(stderr, "zestyd: failed to register API routes\n");
        zdb_http_stop(srv);
        zdb_config_close(config);
        zdb_engine_close(engine);
        return 1;
    }

    zdb_cluster *cluster = NULL;
    zdb_repl *repl = NULL;
    if (peer_port > 0) {
        char node_id[ZDB_NODE_ID_MAX];
        cluster = zdb_cluster_start(config, advertise_addr, peer_port,
                                    node_id);
        if (!cluster) {
            fprintf(stderr, "zestyd: failed to start cluster service on"
                            " peer port %d\n", peer_port);
            zdb_http_stop(srv);
            zdb_config_close(config);
            zdb_engine_close(engine);
            return 1;
        }
        printf("zestyd cluster node %s advertising %s:%d"
               " (peer port %d)\n",
               node_id, advertise_addr, peer_port, peer_port);

        /* stage 5 replication on top of the mesh */
        repl = zdb_repl_start(cluster, config, data_dir);
        if (!repl) {
            fprintf(stderr, "zestyd: failed to start replication service\n");
            zdb_cluster_stop(cluster);
            zdb_http_stop(srv);
            zdb_config_close(config);
            zdb_engine_close(engine);
            return 1;
        }
    }
    zdb_api_set_cluster(cluster);
    zdb_api_set_repl(repl);

    signal(SIGINT, on_signal);
    signal(SIGTERM, on_signal);

    /* local admin socket: zestyctl connects here, trusted, no auth */
    if (admin_sock && !zdb_http_start_admin(srv, admin_sock)) {
        fprintf(stderr, "zestyd: warning: admin socket '%s' unavailable\n",
                admin_sock);
    }

    printf("zestyd listening on port %d, data in '%s', admin socket '%s'\n",
           port, data_dir, admin_sock ? admin_sock : "disabled");

    /* The accept loop runs inside its own thread started by
     * zdb_http_start's design; wait here for shutdown. */
    while (!g_stop) {
        struct timespec ts = { .tv_sec = 0, .tv_nsec = 100 * 1000 * 1000 };
        nanosleep(&ts, NULL);
    }

    printf("zestyd shutting down\n");
    zdb_api_set_cluster(NULL);
    zdb_api_set_repl(NULL);
    zdb_repl_stop(repl);
    zdb_cluster_stop(cluster);
    zdb_http_stop(srv);
    zdb_config_close(config);
    zdb_engine_close(engine);
    return 0;
}
