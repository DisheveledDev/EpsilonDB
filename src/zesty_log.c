/* zesty_log.c - shared logging facility. See zesty_log.h. */

#include "zesty_log.h"

#include <fcntl.h>
#include <pthread.h>
#include <stdarg.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>
#include <sys/wait.h>
#include <time.h>
#include <unistd.h>

#define C_RESET  "\033[0m"
#define C_GREEN  "\033[32m"
#define C_YELLOW "\033[33m"
#define C_RED    "\033[31m"

static FILE *g_log = NULL;
static char *g_log_path = NULL;
static char g_log_date[9] = "";   /* "YYYYMMDD" of the open log file */
static pthread_mutex_t g_log_mutex = PTHREAD_MUTEX_INITIALIZER;

static bool use_colour(void)
{
    return isatty(STDOUT_FILENO);
}

/* Creates path and every missing parent directory (best effort). */
static void mkdir_p(char *path)
{
    if (!path || !*path) {
        return;
    }
    for (char *p = path + 1; *p; p++) {
        if (*p == '/') {
            *p = '\0';
            mkdir(path, 0755);
            *p = '/';
        }
    }
    mkdir(path, 0755);
}

void zdb_log_open(const char *path)
{
    time_t now = time(NULL);
    struct tm tmv;
    char dir[1024];
    char *owned_path = NULL;

    if (path && *path) {
        owned_path = strdup(path);
        if (!owned_path) {
            return;
        }
    }

    if (owned_path) {
        /* ensure the parent directory exists (best effort: /var/log/zestydb) */
        snprintf(dir, sizeof(dir), "%s", owned_path);
        char *slash = strrchr(dir, '/');
        if (slash) {
            *slash = '\0';
            mkdir_p(dir);
        }
    }

    pthread_mutex_lock(&g_log_mutex);
    FILE *new_log = owned_path ? fopen(owned_path, "a") : NULL;
    if (g_log) {
        fclose(g_log);
    }
    free(g_log_path);
    g_log = new_log;
    g_log_path = owned_path;
    localtime_r(&now, &tmv);
    strftime(g_log_date, sizeof(g_log_date), "%Y%m%d", &tmv);
    pthread_mutex_unlock(&g_log_mutex);
}

void zdb_log_close(void)
{
    pthread_mutex_lock(&g_log_mutex);
    if (g_log) {
        fclose(g_log);
        g_log = NULL;
    }
    free(g_log_path);
    g_log_path = NULL;
    g_log_date[0] = '\0';
    pthread_mutex_unlock(&g_log_mutex);
}

/* Compresses a rotated log file in place (gzip is a standard system
 * utility). Blocks until done; rotation happens once per day. */
static void gzip_file(const char *path)
{
    pid_t pid = fork();
    if (pid < 0) {
        return;
    }
    if (pid == 0) {
        int null_fd = open("/dev/null", O_RDWR);
        if (null_fd >= 0) {
            dup2(null_fd, STDOUT_FILENO);
            dup2(null_fd, STDERR_FILENO);
            if (null_fd > STDERR_FILENO) {
                close(null_fd);
            }
        }
        execlp("gzip", "gzip", "-f", path, (char *)NULL);
        _exit(127);
    }
    waitpid(pid, NULL, 0);
}

/* Rotates the log once per day, nginx-style: rename the active log to a
 * date-stamped file, compress it, and start a fresh log. */
void zdb_log_rotate_if_needed(void)
{
    time_t now = time(NULL);
    struct tm tmv;
    char today[9];

    localtime_r(&now, &tmv);
    strftime(today, sizeof(today), "%Y%m%d", &tmv);

    pthread_mutex_lock(&g_log_mutex);
    if (!g_log_path) {
        pthread_mutex_unlock(&g_log_mutex);
        return;
    }
    if (strcmp(today, g_log_date) == 0) {
        pthread_mutex_unlock(&g_log_mutex);
        return;
    }

    if (g_log) {
        fclose(g_log);
        g_log = NULL;
    }
    if (g_log_date[0]) {
        char dated[2048];
        snprintf(dated, sizeof(dated), "%s-%s", g_log_path, g_log_date);
        rename(g_log_path, dated);
        gzip_file(dated);
    }
    g_log = fopen(g_log_path, "a");
    snprintf(g_log_date, sizeof(g_log_date), "%s", today);
    pthread_mutex_unlock(&g_log_mutex);
}

/* Writes a timestamped line to the log file and to the console. */
void zdb_log(const char *level, const char *fmt, ...)
{
    char msg[2048];
    char ts[32];
    va_list ap;
    time_t now = time(NULL);
    struct tm tmv;

    va_start(ap, fmt);
    vsnprintf(msg, sizeof(msg), fmt, ap);
    va_end(ap);

    localtime_r(&now, &tmv);
    strftime(ts, sizeof(ts), "%Y-%m-%d %H:%M:%S", &tmv);

    const char *colour = C_RESET;
    if (strcmp(level, "ERROR") == 0) {
        colour = C_RED;
    } else if (strcmp(level, "WARN") == 0) {
        colour = C_YELLOW;
    } else if (strcmp(level, "INFO") == 0) {
        colour = C_GREEN;
    }
    bool console_err = strcmp(level, "ERROR") == 0 ||
                       strcmp(level, "WARN") == 0;
    FILE *console = console_err ? stderr : stdout;

    pthread_mutex_lock(&g_log_mutex);
    if (use_colour()) {
        fprintf(console, "%s[%s] %s%s\n", colour, ts, msg, C_RESET);
    } else {
        fprintf(console, "[%s] %s\n", ts, msg);
    }

    if (g_log) {
        fprintf(g_log, "[%s] %-5s %s\n", ts, level, msg);
        fflush(g_log);
    }
    pthread_mutex_unlock(&g_log_mutex);
}
