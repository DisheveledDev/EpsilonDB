/* epsilon_log.h - shared logging facility.
 *
 * All server-side logging funnels through edb_log(), which writes a
 * timestamped, level-tagged line to the console and, once opened via
 * edb_log_open(), to a file (default /var/log/epsilondb/epsilondb.log). The
 * file is rotated once per day, nginx-style (rename + gzip). When no file
 * has been opened, or the file cannot be opened, logging degrades to the
 * console only.
 */

#ifndef EPSILON_LOG_H
#define EPSILON_LOG_H

#ifdef __GNUC__
#define EDB_LOG_PRINTF(fmt_idx, arg_idx) \
    __attribute__((format(printf, fmt_idx, arg_idx)))
#else
#define EDB_LOG_PRINTF(fmt_idx, arg_idx)
#endif

/* Open path for appending, creating parent directories (best effort). Call
 * once at startup. NULL/empty path, or failure to open, => console-only. */
void edb_log_open(const char *path);

/* Flush and close the log file (idempotent). */
void edb_log_close(void);

/* Rotate the log once per day. Safe to call periodically; a cheap no-op
 * while the date has not changed. */
void edb_log_rotate_if_needed(void);

/* Write a timestamped line to the log file and console. level is one of
 * "ERROR", "WARN", "INFO" (anything else is treated as INFO). Thread-safe. */
void edb_log(const char *level, const char *fmt, ...) EDB_LOG_PRINTF(2, 3);

#endif
