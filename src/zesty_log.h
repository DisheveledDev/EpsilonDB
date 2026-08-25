/* zesty_log.h - shared logging facility.
 *
 * All server-side logging funnels through zdb_log(), which writes a
 * timestamped, level-tagged line to the console and, once opened via
 * zdb_log_open(), to a file (default /var/log/zestydb/zestydb.log). The
 * file is rotated once per day, nginx-style (rename + gzip). When no file
 * has been opened, or the file cannot be opened, logging degrades to the
 * console only.
 */

#ifndef ZESTY_LOG_H
#define ZESTY_LOG_H

#ifdef __GNUC__
#define ZDB_LOG_PRINTF(fmt_idx, arg_idx) \
    __attribute__((format(printf, fmt_idx, arg_idx)))
#else
#define ZDB_LOG_PRINTF(fmt_idx, arg_idx)
#endif

/* Open path for appending, creating parent directories (best effort). Call
 * once at startup. NULL/empty path, or failure to open, => console-only. */
void zdb_log_open(const char *path);

/* Flush and close the log file (idempotent). */
void zdb_log_close(void);

/* Rotate the log once per day. Safe to call periodically; a cheap no-op
 * while the date has not changed. */
void zdb_log_rotate_if_needed(void);

/* Write a timestamped line to the log file and console. level is one of
 * "ERROR", "WARN", "INFO" (anything else is treated as INFO). Thread-safe. */
void zdb_log(const char *level, const char *fmt, ...) ZDB_LOG_PRINTF(2, 3);

#endif
