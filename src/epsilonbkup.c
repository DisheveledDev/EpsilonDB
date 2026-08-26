/* epsilonbkup - backup/restore CLI for EpsilonDB.
 *
 * Backup gathers every shard in the cluster and packs them into a zip;
 * restore locks the cluster, wipes the shards and pushes the backup back.
 * See epsilonbkup_internal.h and the README for the full protocol.
 */

#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#include "../vendor/cjson/cJSON.h"
#include "engine/md5.h"
#include "socket/epsilon_cluster.h"
#include "socket/epsilon_snap.h"
#include "epsilon_banner.h"
#include "api/version.h"
#include "epsilonbkup_internal.h"

#define DEFAULT_ADMIN_SOCK "epsilon-admin.sock"
#define CHUNK_BYTES (8 * 1024 * 1024)
#define MAX_SHARDS 8192
#define MAX_NODES 64

/* ------------------------------------------------------------------ */
/* process + filesystem helpers                                        */
/*                                                                     */
/* These deliberately avoid system()/popen(): the paths below come from */
/* argv and from a restore manifest, and interpolating them into a      */
/* shell string lets a single quote in a filename run arbitrary         */
/* commands as whatever user runs the tool (often root, since the       */
/* installer registers a system service).                               */

/* Runs argv directly (no shell). When `cwd` is non-NULL the child
 * chdir()s there first. Returns the child's exit status, or -1 if it
 * could not be run or was killed by a signal. */
static int run_argv(const char *cwd, char *const argv[])
{
    pid_t pid = fork();
    if (pid < 0) {
        return -1;
    }
    if (pid == 0) {
        if (cwd && chdir(cwd) != 0) {
            _exit(127);
        }
        int devnull = open("/dev/null", O_WRONLY);
        if (devnull >= 0) {
            dup2(devnull, STDOUT_FILENO);
            dup2(devnull, STDERR_FILENO);
            if (devnull > STDERR_FILENO) {
                close(devnull);
            }
        }
        execvp(argv[0], argv);
        _exit(127);
    }
    int status = 0;
    while (waitpid(pid, &status, 0) < 0) {
        if (errno != EINTR) {
            return -1;
        }
    }
    return WIFEXITED(status) ? WEXITSTATUS(status) : -1;
}

/* Recursively deletes `path`. Returns 0 on success (including when the
 * path does not exist). Replaces the previous `rm -rf` shell-out. */
static int remove_tree(const char *path)
{
    struct stat st;
    if (lstat(path, &st) != 0) {
        return (errno == ENOENT) ? 0 : -1;
    }
    if (!S_ISDIR(st.st_mode)) {
        return unlink(path) == 0 ? 0 : -1;
    }
    DIR *d = opendir(path);
    if (!d) {
        return -1;
    }
    int rc = 0;
    struct dirent *ent;
    while ((ent = readdir(d)) != NULL) {
        if (strcmp(ent->d_name, ".") == 0 || strcmp(ent->d_name, "..") == 0) {
            continue;
        }
        char child[PATH_MAX];
        if (snprintf(child, sizeof(child), "%s/%s", path, ent->d_name) >=
            (int)sizeof(child)) {
            rc = -1;
            continue;
        }
        if (remove_tree(child) != 0) {
            rc = -1;
        }
    }
    closedir(d);
    if (rmdir(path) != 0) {
        rc = -1;
    }
    return rc;
}

/* Resolves `path` to an absolute path without requiring it to exist
 * (realpath() fails on a file that has not been created yet). Returns 0
 * on success. */
static int absolute_path(const char *path, char *out, size_t outsz)
{
    if (!path || !*path) {
        return -1;
    }
    if (path[0] == '/') {
        return (snprintf(out, outsz, "%s", path) < (int)outsz) ? 0 : -1;
    }
    char cwd[PATH_MAX];
    if (!getcwd(cwd, sizeof(cwd))) {
        return -1;
    }
    return (snprintf(out, outsz, "%s/%s", cwd, path) < (int)outsz) ? 0 : -1;
}

static void shard_key_for(const char *partition, const char *keyspace,
                          char out[33])
{
    char ph[33], kh[33], framed[66];
    edb_md5_hex(partition, strlen(partition), ph);
    edb_md5_hex(keyspace, strlen(keyspace), kh);
    snprintf(framed, sizeof(framed), "%s:%s", ph, kh);
    edb_md5_hex(framed, strlen(framed), out);
}

/* ------------------------------------------------------------------ */
/* backup                                                              */

typedef struct {
    char key[33];
    long long size;
    bool system;
    char database[128];
    char partition[256];
    char keyspace[128];
    bool have_names;
    int nholders;          /* nodes that reported holding this shard */
    int holders[MAX_NODES];
} shard_entry;

typedef struct {
    char id[EDB_NODE_ID_MAX];
    char addr[EDB_ADDR_MAX];
    int mesh_port;
    int http_port;
    bool online;
    /* Reachable over the process-wide transport already configured (the
     * local admin socket), rather than by dialling addr:http_port. */
    bool local;
} cluster_node;

static int node_cmp(const void *a, const void *b)
{
    return strcmp(((const cluster_node *)a)->id, ((const cluster_node *)b)->id);
}

static int shard_cmp(const void *a, const void *b)
{
    return strcmp(((const shard_entry *)a)->key, ((const shard_entry *)b)->key);
}

static char *staging_dir_make(const char *out_zip)
{
    char dir[PATH_MAX];
    if (snprintf(dir, sizeof(dir), "%s.staging", out_zip) >=
        (int)sizeof(dir)) {
        fprintf(stderr, "epsilonbkup: output path too long\n");
        return NULL;
    }
    if (remove_tree(dir) != 0 || (mkdir(dir, 0700) != 0 && errno != EEXIST)) {
        fprintf(stderr, "epsilonbkup: cannot create staging dir '%s': %s\n",
                dir, strerror(errno));
        return NULL;
    }
    return strdup(dir);
}

static int cmd_backup(int argc, char **argv)
{
    const char *out_zip = NULL;
    bool keep_staging = false;
    int argi = 0;
    while (argi < argc) {
        if (strcmp(argv[argi], "-o") == 0 && argi + 1 < argc) {
            out_zip = argv[argi + 1];
            argi += 2;
        } else if (strcmp(argv[argi], "-k") == 0) {
            keep_staging = true;
            argi++;
        } else {
            fprintf(stderr, "epsilonbkup: unknown backup option '%s'\n",
                    argv[argi]);
            return 2;
        }
    }
    if (!out_zip) {
        char stamp[64];
        time_t now = time(NULL);
        struct tm tmv;
        localtime_r(&now, &tmv);
        strftime(stamp, sizeof(stamp), "epsilon-backup-%Y%m%d-%H%M%S.zip",
                 &tmv);
        out_zip = strdup(stamp);
    }

    /* --- cluster topology + keyspace names --------------------------- */
    int status = 0;
    cJSON *cluster = http_json("GET", "/admin/cluster", NULL, &status);
    cJSON *keyspaces = http_json("GET", "/admin/keyspaces", NULL, NULL);
    bool single_node = false;
    if (!cluster || status >= 400) {
        const char *err = cluster ? json_error(cluster) : NULL;
        if (err && strstr(err, "clustering disabled")) {
            /* single node without a peer port: no mesh, use HTTP */
            single_node = true;
            printf("single node (clustering disabled): downloading over HTTP\n");
        } else {
            fprintf(stderr, "epsilonbkup: cannot read cluster state (%s)\n",
                    err ? err : "HTTP error");
            cJSON_Delete(cluster);
            cJSON_Delete(keyspaces);
            return 1;
        }
    }
    if (!keyspaces) {
        fprintf(stderr, "epsilonbkup: cannot read keyspace registry\n");
        cJSON_Delete(cluster);
        cJSON_Delete(keyspaces);
        return 1;
    }

    /* per-node connectivity: HTTP port (from gossip) + mesh peer port */
    cluster_node nodes[MAX_NODES] = {0};
    size_t nnodes = 0;
    if (!single_node) {
        const cJSON *jnodes =
            cJSON_GetObjectItemCaseSensitive(cluster, "nodes");
        if (cJSON_IsArray(jnodes)) {
            const cJSON *n = NULL;
            cJSON_ArrayForEach(n, jnodes) {
                if (nnodes >= MAX_NODES) {
                    break;
                }
                const cJSON *jid = cJSON_GetObjectItemCaseSensitive(n, "id");
                const cJSON *jaddr =
                    cJSON_GetObjectItemCaseSensitive(n, "addr");
                const cJSON *jport =
                    cJSON_GetObjectItemCaseSensitive(n, "port");
                const cJSON *jhttp =
                    cJSON_GetObjectItemCaseSensitive(n, "http_port");
                const cJSON *jonline =
                    cJSON_GetObjectItemCaseSensitive(n, "online");
                const cJSON *jremoved =
                    cJSON_GetObjectItemCaseSensitive(n, "removed");
                if (!cJSON_IsString(jid) || !jid->valuestring ||
                    cJSON_IsTrue(jremoved)) {
                    continue;
                }
                memset(&nodes[nnodes], 0, sizeof(nodes[nnodes]));
                snprintf(nodes[nnodes].id, sizeof(nodes[nnodes].id), "%s",
                         jid->valuestring);
                if (cJSON_IsString(jaddr) && jaddr->valuestring) {
                    snprintf(nodes[nnodes].addr, sizeof(nodes[nnodes].addr),
                             "%s", jaddr->valuestring);
                }
                if (cJSON_IsNumber(jport)) {
                    nodes[nnodes].mesh_port = jport->valueint;
                }
                if (cJSON_IsNumber(jhttp)) {
                    nodes[nnodes].http_port = jhttp->valueint;
                }
                nodes[nnodes].online = cJSON_IsTrue(jonline);
                nnodes++;
            }
        }
    } else {
        /* single node: reuse whatever transport the tool is already using.
         * With no -h that is the local admin socket, for which there is no
         * TCP port; the node must still be treated as reachable. */
        snprintf(nodes[0].id, sizeof(nodes[0].id), "%s",
                 g_host ? g_host : "local");
        nodes[0].http_port = g_host ? g_port : 0;
        nodes[0].local = true;
        nodes[0].online = true;
        nnodes = 1;
    }
    qsort(nodes, nnodes, sizeof(nodes[0]), node_cmp);
    const cJSON *jnode_id = single_node
                                ? NULL
                                : cJSON_GetObjectItemCaseSensitive(cluster,
                                                                  "node_id");
    const char *self_id = cJSON_IsString(jnode_id) ? jnode_id->valuestring
                                                   : NULL;

    char *staging = staging_dir_make(out_zip);
    if (!staging) {
        cJSON_Delete(cluster);
        cJSON_Delete(keyspaces);
        return 1;
    }

    /* map shard key -> partition/keyspace names */
    shard_entry *names = NULL;
    size_t nnames = 0, names_cap = 0;
    if (cJSON_IsArray(keyspaces)) {
        const cJSON *k = NULL;
        cJSON_ArrayForEach(k, keyspaces) {
            const cJSON *jpart = cJSON_GetObjectItemCaseSensitive(k, "partition");
            const cJSON *jks = cJSON_GetObjectItemCaseSensitive(k, "name");
            const cJSON *jdb = cJSON_GetObjectItemCaseSensitive(k, "database");
            if (!cJSON_IsString(jpart) || !cJSON_IsString(jks) ||
                !jpart->valuestring || !jks->valuestring) {
                continue;
            }
            if (nnames == names_cap) {
                names_cap = names_cap ? names_cap * 2 : 64;
                shard_entry *grown =
                    realloc(names, names_cap * sizeof(*names));
                if (!grown) {
                    break;
                }
                names = grown;
            }
            memset(&names[nnames], 0, sizeof(names[nnames]));
            shard_key_for(jpart->valuestring, jks->valuestring,
                          names[nnames].key);
            snprintf(names[nnames].partition, sizeof(names[nnames].partition),
                     "%s", jpart->valuestring);
            snprintf(names[nnames].keyspace, sizeof(names[nnames].keyspace),
                     "%s", jks->valuestring);
            if (cJSON_IsString(jdb) && jdb->valuestring) {
                snprintf(names[nnames].database, sizeof(names[nnames].database),
                         "%s", jdb->valuestring);
            }
            names[nnames].have_names = true;
            nnames++;
        }
    }

    /* --- shard manifest: union every online node's list -------------- */
    shard_entry *shards = malloc(MAX_SHARDS * sizeof(*shards));
    size_t nshards = 0;
    if (!shards) {
        fprintf(stderr, "epsilonbkup: out of memory\n");
        cJSON_Delete(cluster);
        cJSON_Delete(keyspaces);
        free(names);
        free(staging);
        return 1;
    }
    for (size_t n = 0; n < nnodes; n++) {
        if (!nodes[n].online ||
            (!nodes[n].local && nodes[n].http_port <= 0)) {
            continue;
        }
        int mstatus = 0;
        cJSON *manifest = NULL;
        if (single_node) {
            manifest = http_json("GET", "/admin/backup/manifest", NULL,
                                 &mstatus);
        } else {
            const char *saved_host = g_host;
            int saved_port = g_port;
            g_host = nodes[n].addr;
            g_port = nodes[n].http_port;
            manifest = http_json("GET", "/admin/backup/manifest", NULL,
                                 &mstatus);
            g_host = saved_host;
            g_port = saved_port;
        }
        if (!manifest || mstatus >= 400) {
            fprintf(stderr, "epsilonbkup: cannot read shard manifest from "
                            "%s (status %d)\n", nodes[n].id, mstatus);
            cJSON_Delete(manifest);
            continue;
        }
        const cJSON *jshards =
            cJSON_GetObjectItemCaseSensitive(manifest, "shards");
        if (cJSON_IsArray(jshards)) {
            const cJSON *s = NULL;
            cJSON_ArrayForEach(s, jshards) {
                if (nshards >= MAX_SHARDS) {
                    break;
                }
                const cJSON *jkey =
                    cJSON_GetObjectItemCaseSensitive(s, "key");
                const cJSON *jsize =
                    cJSON_GetObjectItemCaseSensitive(s, "size");
                const cJSON *jsys =
                    cJSON_GetObjectItemCaseSensitive(s, "system");
                if (!cJSON_IsString(jkey) || !jkey->valuestring) {
                    continue;
                }
                /* merge into the union (dedupe by key) */
                shard_entry *entry = NULL;
                for (size_t i = 0; i < nshards; i++) {
                    if (strcmp(shards[i].key, jkey->valuestring) == 0) {
                        entry = &shards[i];
                        break;
                    }
                }
                if (!entry) {
                    memset(&shards[nshards], 0, sizeof(shards[nshards]));
                    snprintf(shards[nshards].key, sizeof(shards[nshards].key),
                             "%s", jkey->valuestring);
                    entry = &shards[nshards++];
                }
                if (cJSON_IsNumber(jsize) && !entry->size) {
                    entry->size = (long long)jsize->valuedouble;
                }
                if (cJSON_IsTrue(jsys)) {
                    entry->system = true;
                }
                if (entry->nholders < MAX_NODES) {
                    entry->holders[entry->nholders++] = (int)n;
                }
                for (size_t i = 0; i < nnames; i++) {
                    if (strcmp(names[i].key, entry->key) == 0) {
                        entry->have_names = true;
                        snprintf(entry->database, sizeof(entry->database),
                                 "%s", names[i].database);
                        snprintf(entry->partition, sizeof(entry->partition),
                                 "%s", names[i].partition);
                        snprintf(entry->keyspace, sizeof(entry->keyspace),
                                 "%s", names[i].keyspace);
                        break;
                    }
                }
            }
        }
        cJSON_Delete(manifest);
    }
    qsort(shards, nshards, sizeof(shards[0]), shard_cmp);

    /* skip config shards */
    size_t nbackup = 0;
    for (size_t i = 0; i < nshards; i++) {
        if (!shards[i].system) {
            shards[nbackup++] = shards[i];
        }
    }
    nshards = nbackup;

    if (nshards == 0) {
        printf("no data shards to back up (cluster is empty)\n");
        remove_tree(staging);
        cJSON_Delete(cluster);
        cJSON_Delete(keyspaces);
        free(names);
        free(staging);
        free(shards);
        return 0;
    }

    /* --- fetch each shard from a node that holds it ------------------ */
    size_t ok = 0, failed = 0;
    for (size_t i = 0; i < nshards; i++) {
        char path[1100];
        snprintf(path, sizeof(path), "%s/%s.sqlite", staging,
                 shards[i].key);
        bool got = false;
        for (int h = 0; h < shards[i].nholders && !got; h++) {
            int n = shards[i].holders[h];
            if (n < 0 || (size_t)n >= nnodes || !nodes[n].online) {
                continue;
            }
            if (nodes[n].mesh_port > 0 && nodes[n].addr[0]) {
                if (edb_snap_fetch(nodes[n].addr, nodes[n].mesh_port,
                                   shards[i].key, staging) == 0) {
                    got = true;
                    break;
                } else if (!g_quiet) {
                    fprintf(stderr,
                            "epsilonbkup: mesh fetch of %s from %s failed, "
                            "trying next holder\n", shards[i].key,
                            nodes[n].id);
                }
            }
            if (!got && (nodes[n].local || nodes[n].http_port > 0)) {
                const char *saved_host = g_host;
                int saved_port = g_port;
                if (!single_node) {
                    g_host = nodes[n].addr;
                    g_port = nodes[n].http_port;
                }
                char url[1100];
                snprintf(url, sizeof(url), "/admin/backup/shard/%s",
                         shards[i].key);
                int st = 0;
                size_t blen = 0;
                char *body =
                    http_request_raw("GET", url, NULL, 0, &blen, &st);
                if (!single_node) {
                    g_host = saved_host;
                    g_port = saved_port;
                }
                if (body && st == 200) {
                    FILE *fp = fopen(path, "wb");
                    if (fp) {
                        fwrite(body, 1, blen, fp);
                        fclose(fp);
                        got = true;
                    }
                }
                free(body);
            }
        }
        if (got) {
            struct stat st;
            if (stat(path, &st) == 0) {
                shards[i].size = (long long)st.st_size;
            }
            ok++;
            printf("  backed up %s (%lld bytes)\n", shards[i].key,
                   shards[i].size);
        } else {
            failed++;
            fprintf(stderr, "epsilonbkup: FAILED to back up %s\n",
                    shards[i].key);
        }
    }

    /* --- manifest.json + zip ----------------------------------------- */
    cJSON *m = cJSON_CreateObject();
    time_t now = time(NULL);
    char stamp[64];
    struct tm tmv;
    localtime_r(&now, &tmv);
    strftime(stamp, sizeof(stamp), "%Y-%m-%dT%H:%M:%S%z", &tmv);
    cJSON_AddStringToObject(m, "created", stamp);
    cJSON_AddStringToObject(m, "tool", "epsilonbkup");
    cJSON_AddStringToObject(m, "source_node",
                            self_id ? self_id : "single");
    cJSON *sn = cJSON_AddArrayToObject(m, "shards");
    for (size_t i = 0; sn && i < nshards; i++) {
        cJSON *e = cJSON_CreateObject();
        cJSON_AddStringToObject(e, "key", shards[i].key);
        cJSON_AddNumberToObject(e, "size", (double)shards[i].size);
        if (shards[i].have_names) {
            cJSON_AddStringToObject(e, "database", shards[i].database);
            cJSON_AddStringToObject(e, "partition", shards[i].partition);
            cJSON_AddStringToObject(e, "keyspace", shards[i].keyspace);
        }
        cJSON_AddItemToArray(sn, e);
    }
    char *mjson = cJSON_PrintUnformatted(m);
    cJSON_Delete(m);
    if (mjson) {
        char mp[1100];
        snprintf(mp, sizeof(mp), "%s/manifest.json", staging);
        FILE *fp = fopen(mp, "w");
        if (fp) {
            fputs(mjson, fp);
            fclose(fp);
        }
        free(mjson);
    }

    /* zip the sqlite files + manifest at the top level.
     *
     * The archive path must be made absolute first: zip runs with the
     * staging directory as its working directory, so a relative output
     * name would be created *inside* the staging tree and then destroyed
     * by the cleanup below, leaving no backup behind while still
     * reporting success. */
    char zip_abs[PATH_MAX];
    if (absolute_path(out_zip, zip_abs, sizeof(zip_abs)) != 0) {
        fprintf(stderr, "epsilonbkup: cannot resolve output path '%s'\n",
                out_zip);
        cJSON_Delete(cluster);
        cJSON_Delete(keyspaces);
        free(names);
        free(staging);
        free(shards);
        return 1;
    }
    char *zip_argv[] = { (char *)"zip", (char *)"-q", (char *)"-r",
                         zip_abs, (char *)".", NULL };
    int zrc = run_argv(staging, zip_argv);
    if (zrc != 0) {
        fprintf(stderr,
                "epsilonbkup: zip failed (is 'zip' installed?); files are "
                "in '%s'\n", staging);
        cJSON_Delete(cluster);
        cJSON_Delete(keyspaces);
        free(names);
        free(staging);
        free(shards);
        return 1;
    }
    printf("backup written to %s (%zu shards, %zu failed)\n", zip_abs,
           ok, failed);
    if (!keep_staging) {
        remove_tree(staging);
    } else {
        printf("staging files kept in %s\n", staging);
    }
    cJSON_Delete(cluster);
    cJSON_Delete(keyspaces);
    free(names);
    free(staging);
    free(shards);
    return failed > 0 ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* restore                                                             */

static int cmd_restore(int argc, char **argv)
{
    if (argc < 1) {
        fprintf(stderr, "epsilonbkup: restore needs a backup zip\n");
        return 2;
    }
    const char *zip_path = argv[0];

    /* --- unpack ------------------------------------------------------- */
    char dir[PATH_MAX];
    if (snprintf(dir, sizeof(dir), "%s.unpacked", zip_path) >=
        (int)sizeof(dir)) {
        fprintf(stderr, "epsilonbkup: backup path too long\n");
        return 1;
    }
    if (remove_tree(dir) != 0 || (mkdir(dir, 0700) != 0 && errno != EEXIST)) {
        fprintf(stderr, "epsilonbkup: cannot create '%s': %s\n", dir,
                strerror(errno));
        return 1;
    }
    char *unzip_argv[] = { (char *)"unzip", (char *)"-q", (char *)"-o",
                           (char *)zip_path, (char *)"-d", dir, NULL };
    if (run_argv(NULL, unzip_argv) != 0) {
        fprintf(stderr, "epsilonbkup: cannot unpack '%s' (is 'unzip' "
                        "installed?)\n", zip_path);
        return 1;
    }
    char mpath[PATH_MAX];
    if (snprintf(mpath, sizeof(mpath), "%s/manifest.json", dir) >=
        (int)sizeof(mpath)) {
        fprintf(stderr, "epsilonbkup: backup path too long\n");
        return 1;
    }
    cJSON *manifest = NULL;
    {
        char *mtext = NULL;
        FILE *fp = fopen(mpath, "rb");
        if (fp) {
            fseek(fp, 0, SEEK_END);
            long size = ftell(fp);
            fseek(fp, 0, SEEK_SET);
            if (size > 0 && size < 64 * 1024 * 1024) {
                mtext = malloc((size_t)size + 1);
                if (mtext && fread(mtext, 1, (size_t)size, fp) ==
                                 (size_t)size) {
                    mtext[size] = '\0';
                    manifest = cJSON_Parse(mtext);
                }
                free(mtext);
            }
            fclose(fp);
        }
    }
    const cJSON *jshards = manifest ? cJSON_GetObjectItemCaseSensitive(
                                          manifest, "shards")
                                    : NULL;
    shard_entry *shards = malloc(MAX_SHARDS * sizeof(*shards));
    size_t nshards = 0;
    if (!shards) {
        fprintf(stderr, "epsilonbkup: out of memory\n");
        cJSON_Delete(manifest);
        return 1;
    }
    if (cJSON_IsArray(jshards)) {
        const cJSON *s = NULL;
        cJSON_ArrayForEach(s, jshards) {
            if (nshards >= MAX_SHARDS) {
                break;
            }
            const cJSON *jkey = cJSON_GetObjectItemCaseSensitive(s, "key");
            const cJSON *jsize = cJSON_GetObjectItemCaseSensitive(s, "size");
            if (!cJSON_IsString(jkey) || !jkey->valuestring) {
                continue;
            }
            memset(&shards[nshards], 0, sizeof(shards[nshards]));
            snprintf(shards[nshards].key, sizeof(shards[nshards].key), "%s",
                     jkey->valuestring);
            shards[nshards].size = cJSON_IsNumber(jsize)
                                       ? (long long)jsize->valuedouble
                                       : 0;
            nshards++;
        }
    }
    qsort(shards, nshards, sizeof(shards[0]), shard_cmp);
    if (nshards == 0) {
        fprintf(stderr, "epsilonbkup: no shards found in '%s'\n", zip_path);
        cJSON_Delete(manifest);
        return 1;
    }

    /* --- cluster topology --------------------------------------------- */
    int status = 0;
    cJSON *cluster = http_json("GET", "/admin/cluster", NULL, &status);
    cluster_node nodes[MAX_NODES] = {0};
    size_t nnodes = 0;
    if (cluster && status < 400) {
        const cJSON *jnodes = cJSON_GetObjectItemCaseSensitive(cluster, "nodes");
        if (cJSON_IsArray(jnodes)) {
            const cJSON *n = NULL;
            cJSON_ArrayForEach(n, jnodes) {
                if (nnodes >= MAX_NODES) {
                    break;
                }
                const cJSON *jid = cJSON_GetObjectItemCaseSensitive(n, "id");
                const cJSON *jaddr = cJSON_GetObjectItemCaseSensitive(n, "addr");
                const cJSON *jport = cJSON_GetObjectItemCaseSensitive(n, "port");
                const cJSON *jhttp = cJSON_GetObjectItemCaseSensitive(n,
                                                                     "http_port");
                const cJSON *jonline = cJSON_GetObjectItemCaseSensitive(n,
                                                                       "online");
                const cJSON *jremoved = cJSON_GetObjectItemCaseSensitive(n,
                                                                        "removed");
                if (!cJSON_IsString(jid) || !jid->valuestring ||
                    cJSON_IsTrue(jremoved)) {
                    continue;
                }
                memset(&nodes[nnodes], 0, sizeof(nodes[nnodes]));
                snprintf(nodes[nnodes].id, sizeof(nodes[nnodes].id), "%s",
                         jid->valuestring);
                if (cJSON_IsString(jaddr) && jaddr->valuestring) {
                    snprintf(nodes[nnodes].addr, sizeof(nodes[nnodes].addr),
                             "%s", jaddr->valuestring);
                }
                if (cJSON_IsNumber(jport)) {
                    nodes[nnodes].mesh_port = jport->valueint;
                }
                if (cJSON_IsNumber(jhttp)) {
                    nodes[nnodes].http_port = jhttp->valueint;
                }
                nodes[nnodes].online = cJSON_IsTrue(jonline);
                nnodes++;
            }
        }
    }
    cJSON_Delete(cluster);

    /* Single-node (no cluster): the seed itself is the only target. */
    if (nnodes == 0) {
        snprintf(nodes[0].id, sizeof(nodes[0].id), "%s",
                 g_host ? g_host : "local");
        snprintf(nodes[0].addr, sizeof(nodes[0].addr), "%s",
                 g_host ? g_host : "local");
        nodes[0].http_port = g_host ? g_port : 0;
        nodes[0].online = true;
        nnodes = 1;
    }
    qsort(nodes, nnodes, sizeof(nodes[0]), node_cmp);

    /* nodes without a known HTTP port fall back to the seed's own HTTP
     * endpoint (the local admin socket or the -h/-p connection) */
    const char *seed_addr = g_host ? g_host : "local";
    int seed_port = g_host ? g_port : 0;

    for (size_t i = 0; i < nnodes; i++) {
        if (!nodes[i].online) {
            printf("  node %s is offline - skipped\n", nodes[i].id);
        }
    }

    /* --- lock all nodes ------------------------------------------------ */
    int locked = 0;
    for (size_t i = 0; i < nnodes; i++) {
        if (!nodes[i].online) {
            continue;
        }
        int st = 0;
        if (nodes[i].http_port > 0) {
            /* drive the node directly */
            int saved_port = g_port;
            const char *saved_host = g_host;
            const char *saved_user = g_user;
            g_host = nodes[i].addr;
            g_port = nodes[i].http_port;
            g_user = g_user ? g_user : "";
            cJSON *r = http_json("POST", "/admin/restore/lock", NULL, &st);
            g_host = saved_host;
            g_port = saved_port;
            g_user = saved_user;
            if (r) {
                cJSON_Delete(r);
            }
            if (st == 200) {
                locked++;
                printf("  locked %s (%s:%d)\n", nodes[i].id, nodes[i].addr,
                       nodes[i].http_port);
                continue;
            }
            printf("  WARNING: could not lock %s (%s:%d) - status %d\n",
                   nodes[i].id, nodes[i].addr, nodes[i].http_port, st);
        } else if (strcmp(nodes[i].addr, seed_addr) == 0 &&
                   (seed_port == 0 || nodes[i].mesh_port == 0)) {
            /* the seed itself: use our own HTTP connection */
            int st2 = 0;
            cJSON *r = http_json("POST", "/admin/restore/lock", NULL, &st2);
            cJSON_Delete(r);
            if (st2 == 200) {
                locked++;
                printf("  locked %s (seed)\n", nodes[i].id);
            }
        }
    }
    if (locked == 0) {
        fprintf(stderr, "epsilonbkup: no node could be locked - aborting\n");
        cJSON_Delete(manifest);
        return 1;
    }

    /* --- wipe all nodes ------------------------------------------------ */
    int wiped_nodes = 0;
    size_t wiped_total = 0;
    for (size_t i = 0; i < nnodes; i++) {
        if (!nodes[i].online) {
            continue;
        }
        int st = 0;
        cJSON *r = NULL;
        if (nodes[i].http_port > 0) {
            int saved_port = g_port;
            const char *saved_host = g_host;
            g_host = nodes[i].addr;
            g_port = nodes[i].http_port;
            r = http_json("POST", "/admin/restore/wipe", NULL, &st);
            g_host = saved_host;
            g_port = saved_port;
        } else if (strcmp(nodes[i].addr, seed_addr) == 0) {
            r = http_json("POST", "/admin/restore/wipe", NULL, &st);
        }
        if (r && st == 200) {
            const cJSON *jw = cJSON_GetObjectItemCaseSensitive(r, "wiped");
            size_t w = cJSON_IsNumber(jw) ? (size_t)jw->valuedouble : 0;
            wiped_total += w;
            wiped_nodes++;
            printf("  wiped %zu shards on %s\n", w, nodes[i].id);
        } else {
            fprintf(stderr, "epsilonbkup: wipe failed on %s (status %d)\n",
                    nodes[i].id, st);
        }
        cJSON_Delete(r);
    }
    printf("wiped %zu shards across %d nodes\n", wiped_total, wiped_nodes);

    /* --- push each shard to every online node ------------------------- */
    size_t pushed = 0, push_failed = 0;
    for (size_t i = 0; i < nshards; i++) {
        char filepath[PATH_MAX];
        if (snprintf(filepath, sizeof(filepath), "%s/%s.sqlite", dir,
                     shards[i].key) >= (int)sizeof(filepath)) {
            fprintf(stderr, "epsilonbkup: %s path too long\n",
                    shards[i].key);
            push_failed++;
            continue;
        }
        struct stat st;
        if (stat(filepath, &st) != 0) {
            fprintf(stderr, "epsilonbkup: %s missing from backup\n",
                    shards[i].key);
            push_failed++;
            continue;
        }
        FILE *fp = fopen(filepath, "rb");
        if (!fp) {
            fprintf(stderr, "epsilonbkup: cannot read %s\n", filepath);
            push_failed++;
            continue;
        }
        for (size_t n = 0; n < nnodes; n++) {
            if (!nodes[n].online || nodes[n].http_port <= 0) {
                continue;
            }
            int saved_port = g_port;
            const char *saved_host = g_host;
            g_host = nodes[n].addr;
            g_port = nodes[n].http_port;
            bool ok = true;
            long long offset = 0;
            long long filesize = (long long)st.st_size;
            char *chunk = malloc(CHUNK_BYTES);
            if (!chunk) {
                fclose(fp);
                ok = false;
            }
            rewind(fp);
            while (ok && offset < filesize) {
                size_t want = (size_t)(filesize - offset);
                if (want > sizeof(chunk)) {
                    want = sizeof(chunk);
                }
                size_t got = fread(chunk, 1, want, fp);
                if (got == 0) {
                    ok = false;
                    break;
                }
                char url[1200];
                bool last = (offset + (long long)got) >= filesize;
                snprintf(url, sizeof(url),
                         "/admin/restore/shard/%s?offset=%lld%s",
                         shards[i].key, offset, last ? "&final=1" : "");
                int st2 = 0;
                /* raw binary upload */
                size_t blen = 0;
                char *resp = http_request_raw("PUT", url, chunk, got, &blen,
                                              &st2);
                if (st2 != 200) {
                    ok = false;
                    fprintf(stderr,
                            "epsilonbkup: upload of %s to %s failed "
                            "(status %d, offset %lld)\n",
                            shards[i].key, nodes[n].id, st2, offset);
                    free(resp);
                    break;
                }
                free(resp);
                offset += (long long)got;
            }
            free(chunk);
            g_host = saved_host;
            g_port = saved_port;
            if (!ok) {
                push_failed++;
            } else {
                pushed++;
                printf("  restored %s -> %s\n", shards[i].key, nodes[n].id);
            }
        }
        fclose(fp);
    }

    /* --- unlock -------------------------------------------------------- */
    int unlocked = 0;
    for (size_t i = 0; i < nnodes; i++) {
        if (!nodes[i].online) {
            continue;
        }
        int st = 0;
        cJSON *r = NULL;
        if (nodes[i].http_port > 0) {
            int saved_port = g_port;
            const char *saved_host = g_host;
            g_host = nodes[i].addr;
            g_port = nodes[i].http_port;
            r = http_json("POST", "/admin/restore/unlock", NULL, &st);
            g_host = saved_host;
            g_port = saved_port;
        } else if (strcmp(nodes[i].addr, seed_addr) == 0) {
            r = http_json("POST", "/admin/restore/unlock", NULL, &st);
        }
        if (r && st == 200) {
            unlocked++;
        }
        cJSON_Delete(r);
    }

    remove_tree(dir);
    cJSON_Delete(manifest);
    free(shards);
    printf("restore complete: %zu shards pushed, %zu failed, "
           "%d/%zu nodes unlocked\n",
           pushed, push_failed, unlocked, nnodes);
    return push_failed > 0 ? 1 : 0;
}

/* ------------------------------------------------------------------ */
/* main                                                                */

static void print_usage(void)
{
    printf("usage: epsilonbkup [-h host] [-p port] [-u user [-P password]] "
           "<backup|restore> [args]\n");
    printf("  backup [-o out.zip] [-k]      download all shards into a zip\n");
    printf("  restore <backup.zip>          lock the cluster, wipe shards,\n");
    printf("                                push the backup back, unlock\n");
}

int main(int argc, char **argv)
{
    /* shared EpsilonDB banner */
    for (int i = 0; i < EDB_BANNER_LINES; i++) {
        printf("%s\n", edb_banner[i]);
    }
    printf("        backup and restore tool, version %s\n\n", sw_version);
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
        } else if ((strcmp(argv[argi], "-P") == 0 ||
                    strcmp(argv[argi], "--password") == 0) &&
                   argi + 1 < argc) {
            g_password = argv[argi + 1];
            argi += 2;
        } else if (strcmp(argv[argi], "-q") == 0) {
            g_quiet = true;
            argi++;
        } else {
            print_usage();
            return 2;
        }
    }
    if (argi >= argc) {
        print_usage();
        return 2;
    }
    /* remote user auth requires a password (the server rejects
     * passwordless users over HTTP) */
    if (g_host && g_user && *g_user && g_password[0] == '\0') {
        fprintf(stderr,
                "epsilonbkup: user '%s' needs a password over HTTP "
                "(pass -P <password>)\n",
                g_user);
        return 1;
    }
    const char *cmd = argv[argi++];

    if (strcmp(cmd, "backup") == 0) {
        return cmd_backup(argc - argi, argv + argi);
    }
    if (strcmp(cmd, "restore") == 0) {
        return cmd_restore(argc - argi, argv + argi);
    }
    print_usage();
    return 2;
}