/* epsilonctl_internal.h - shared state and declarations across the
 * epsilonctl translation units (CLI main, HTTP plumbing, full-screen
 * TUI, interactive install/setup). Not installed.
 */

#ifndef EPSILONCTL_INTERNAL_H
#define EPSILONCTL_INTERNAL_H

#include <stdbool.h>
#include <stddef.h>
#include <signal.h>

/* --- connection target ---------------------------------------------- */
extern const char *g_host;      /* set => TCP HTTP mode */
extern int g_port;
extern const char *g_user;
extern const char *g_sockpath;
extern volatile sig_atomic_t g_exit_requested;
extern bool g_quiet_stderr;     /* suppress chatter in console mode */

/* --- output plumbing ------------------------------------------------ */
typedef struct {
    char **lines;      /* scrollback content */
    size_t count;
    size_t cap;
} scrollback;

extern scrollback *g_output;    /* non-NULL while the console is active */

void sb_push(scrollback *sb, const char *text);
void sb_printf(scrollback *sb, const char *fmt, ...);
void sb_free(scrollback *sb);

/* --- HTTP plumbing (epsilonctl.c) ----------------------------------- */
char *read_stdin(size_t *len_out);
int http_request(const char *method, const char *path, const char *body,
                 char **body_out);
void render_json_table(scrollback *sb, const char *json);
int run_sb(scrollback *sb, const char *method, const char *path,
           const char *body);
void out_line(const char *text);
int run(const char *method, const char *path, const char *body);

/* --- commands (epsilonctl.c) ---------------------------------------- */
void print_usage(void);
char *body_from_arg(const char *arg);
int execute_command(int argc, char **argv);

/* --- interactive install/setup (epsilonctl_install.c) --------------- */
int cmd_install(int argc, char **argv);
int cmd_setup(int argc, char **argv);

/* --- TUI (epsilonctl_tui.c) ----------------------------------------- */
void shell_loop(void);

#endif
