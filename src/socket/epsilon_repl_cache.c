/* epsilon_repl_cache.c - the persisted change cache: a sqlite log of
 * changes that could not be acknowledged by their target peers, drained
 * in order when a peer returns. Part of the replication module; see
 * epsilon_repl_internal.h.
 */

#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>

#include "../../vendor/cjson/cJSON.h"
#include "../engine/epsilon_engine.h"
#include "../epsilon_log.h"
#include "epsilon_repl_internal.h"
/* change cache (persisted sqlite log of unacknowledged changes)       */

bool cache_open(change_cache *cc, const char *data_dir)
{
    size_t len = strlen(data_dir) + sizeof("/changes.sqlite");
    char *path = malloc(len);
    if (!path) {
        return false;
    }
    snprintf(path, len, "%s/changes.sqlite", data_dir);
    int rc = sqlite3_open_v2(path, &cc->db,
                             SQLITE_OPEN_READWRITE | SQLITE_OPEN_CREATE |
                                 SQLITE_OPEN_FULLMUTEX,
                             NULL);
    free(path);
    if (rc != SQLITE_OK) {
        if (cc->db) {
            sqlite3_close(cc->db);
            cc->db = NULL;
        }
        return false;
    }
    sqlite3_busy_timeout(cc->db, 5000);
    char *err = NULL;
    if (sqlite3_exec(cc->db,
                     "PRAGMA journal_mode=DELETE;"
                     "PRAGMA synchronous=FULL;"
                     "CREATE TABLE IF NOT EXISTS PendingChanges ("
                     " seq INTEGER PRIMARY KEY AUTOINCREMENT,"
                     " target TEXT NOT NULL,"
                     " cid TEXT NOT NULL,"
                     " payload TEXT NOT NULL,"
                     " created INT NOT NULL,"
                     " UNIQUE(target, cid)"
                     ");"
                     "DROP TABLE IF EXISTS PendingChangesV2;"
                     "CREATE TABLE PendingChangesV2 ("
                     " seq INTEGER PRIMARY KEY AUTOINCREMENT,"
                     " target TEXT NOT NULL,"
                     " cid TEXT NOT NULL,"
                     " payload TEXT NOT NULL,"
                     " created INT NOT NULL,"
                     " UNIQUE(target, cid)"
                     ");"
                     "INSERT OR IGNORE INTO PendingChangesV2"
                     " (seq,target,cid,payload,created)"
                     " SELECT seq,target,cid,payload,created"
                     " FROM PendingChanges;"
                     "DROP TABLE PendingChanges;"
                     "ALTER TABLE PendingChangesV2 RENAME TO PendingChanges;",
                     NULL, NULL, &err) != SQLITE_OK) {
        edb_log("ERROR", "change cache init failed: %s",
                err ? err : "?");
        sqlite3_free(err);
        sqlite3_close(cc->db);
        cc->db = NULL;
        return false;
    }
    pthread_mutex_init(&cc->lock, NULL);
    return true;
}

void cache_close(change_cache *cc)
{
    if (!cc->db) {
        return;
    }
    pthread_mutex_lock(&cc->lock);
    sqlite3_close(cc->db);
    cc->db = NULL;
    pthread_mutex_unlock(&cc->lock);
    pthread_mutex_destroy(&cc->lock);
}

/* Takes ownership of payload. */
void cache_append(change_cache *cc, const char *target,
                         const char *cid, const char *payload)
{
    if (!cc->db) {
        free((void *)payload);
        return;
    }
    pthread_mutex_lock(&cc->lock);
    sqlite3_stmt *stmt = NULL;
    bool ok =
        sqlite3_prepare_v2(cc->db,
                           "INSERT OR REPLACE INTO PendingChanges"
                           " (target, cid, payload, created)"
                           " VALUES (?, ?, ?, ?)",
                           -1, &stmt, NULL) == SQLITE_OK;
    if (ok) {
        sqlite3_bind_text(stmt, 1, target, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, cid, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 3, payload, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 4, repl_epoch_now());
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            edb_log("ERROR", "change cache insert failed: %s",
                    sqlite3_errmsg(cc->db));
        }
    }
    if (stmt) {
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&cc->lock);
    free((void *)payload);
}

void cache_remove(change_cache *cc, const char *target,
                         const char *cid)
{
    if (!cc->db) {
        return;
    }
    pthread_mutex_lock(&cc->lock);
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cc->db,
                           "DELETE FROM PendingChanges"
                           " WHERE target = ? AND cid = ?",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, target, -1, SQLITE_TRANSIENT);
        sqlite3_bind_text(stmt, 2, cid, -1, SQLITE_TRANSIENT);
        sqlite3_step(stmt);
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&cc->lock);
}

/* ------------------------------------------------------------------ */
/* replication service state                                           */


/* Loads at most `limit` pending rows for target, oldest first.
 * Returns a malloc'd array; caller frees payloads + array. */
pending_row *cache_load(change_cache *cc, const char *target,
                               size_t limit, size_t *count_out)
{
    *count_out = 0;
    if (!cc->db) {
        return NULL;
    }
    pending_row *rows = malloc(limit * sizeof(*rows));
    if (!rows) {
        return NULL;
    }
    pthread_mutex_lock(&cc->lock);
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(cc->db,
                           "SELECT target, cid, payload FROM"
                           " PendingChanges WHERE target = ?"
                           " ORDER BY seq LIMIT ?",
                           -1, &stmt, NULL) == SQLITE_OK) {
        sqlite3_bind_text(stmt, 1, target, -1, SQLITE_TRANSIENT);
        sqlite3_bind_int64(stmt, 2, (sqlite3_int64)limit);
        while (*count_out < limit &&
               sqlite3_step(stmt) == SQLITE_ROW) {
            pending_row *r = &rows[*count_out];
            memset(r, 0, sizeof(*r));
            const char *tgt = (const char *)sqlite3_column_text(stmt, 0);
            const char *cid = (const char *)sqlite3_column_text(stmt, 1);
            const char *pay = (const char *)sqlite3_column_text(stmt, 2);
            if (tgt) {
                snprintf(r->target, sizeof(r->target), "%s", tgt);
            }
            if (cid) {
                snprintf(r->cid, sizeof(r->cid), "%s", cid);
            }
            r->payload = pay ? strdup(pay) : NULL;
            (*count_out)++;
        }
        sqlite3_finalize(stmt);
    }
    pthread_mutex_unlock(&cc->lock);
    return rows;
}

void free_rows(pending_row *rows, size_t count)
{
    for (size_t i = 0; i < count; i++) {
        free(rows[i].payload);
    }
    free(rows);
}