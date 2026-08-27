/* epsilon_eql.c - EQL execution engine (stage 8, milestone eql-a).
 *
 * See epsilon_eql.h for the overview. This file implements the SELECT
 * path: FROM-reference scanning, shard materialization, SQL rewriting,
 * and JSON result serialization. DML replication arrives with eql-c.
 */

#include "epsilon_eql.h"

#include <ctype.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include "../../vendor/sqlite/sqlite3.h"

#define EQL_MAX_TABLES 16
#define EQL_MAX_COLS 256
#define EQL_MAX_NAME 512

typedef struct {
    char db[128];
    char part[256];
    char ks[128];
    char dotted[EQL_MAX_NAME];   /* canonical db.part.ks */
    char alias[16];              /* eql_t0 .. */
    char cols[EQL_MAX_COLS][128];
    size_t ncols;
} eql_table;

typedef struct {
    const char *sql;
    size_t pos;
    size_t len;
} eql_scan;

static bool is_ident_char(char c)
{
    return isalnum((unsigned char)c) != 0 || c == '_';
}

/* Reads one identifier at pos: bare word, "quoted" (" escapes),
 * `backticked`, or [bracketed]. On success the unescaped text is written
 * to out and pos advances past it. Returns false leaving pos unchanged
 * when no identifier starts here. */
static bool read_ident(eql_scan *sc, char *out, size_t cap)
{
    size_t p = sc->pos;
    size_t o = 0;
    if (p >= sc->len) {
        return false;
    }
    char c = sc->sql[p];
    if (c == '"' || c == '`') {
        char quote = c;
        p++;
        while (p < sc->len) {
            char ch = sc->sql[p];
            if (ch == quote) {
                if (quote == '"' && p + 1 < sc->len &&
                    sc->sql[p + 1] == '"') {
                    if (o + 1 < cap) {
                        out[o++] = '"';
                    }
                    p += 2;
                    continue;
                }
                p++;
                goto done;
            }
            if (o + 1 < cap) {
                out[o++] = ch;
            }
            p++;
        }
        return false;   /* unterminated quote */
    }
    if (c == '[') {
        p++;
        while (p < sc->len && sc->sql[p] != ']') {
            if (o + 1 < cap) {
                out[o++] = sc->sql[p];
            }
            p++;
        }
        if (p >= sc->len) {
            return false;
        }
        p++;
        goto done;
    }
    if (!is_ident_char(c)) {
        return false;
    }
    while (p < sc->len && is_ident_char(sc->sql[p])) {
        if (o + 1 < cap) {
            out[o++] = sc->sql[p];
        }
        p++;
    }
done:
    out[o] = '\0';
    sc->pos = p;
    return true;
}

static void skip_spaces_comments(eql_scan *sc)
{
    for (;;) {
        while (sc->pos < sc->len && isspace((unsigned char)sc->sql[sc->pos])) {
            sc->pos++;
        }
        if (sc->pos + 1 < sc->len && sc->sql[sc->pos] == '-' &&
            sc->sql[sc->pos + 1] == '-') {
            while (sc->pos < sc->len && sc->sql[sc->pos] != '\n') {
                sc->pos++;
            }
            continue;
        }
        if (sc->pos + 1 < sc->len && sc->sql[sc->pos] == '/' &&
            sc->sql[sc->pos + 1] == '*') {
            const char *end = strstr(sc->sql + sc->pos + 2, "*/");
            sc->pos = end ? (size_t)(end - sc->sql) + 2 : sc->len;
            continue;
        }
        return;
    }
}

/* Skips a complete quoted literal ('string' / "ident") in raw text. */
static void skip_quoted_raw(eql_scan *sc)
{
    char quote = sc->sql[sc->pos];
    sc->pos++;
    while (sc->pos < sc->len) {
        char c = sc->sql[sc->pos];
        if (c == quote) {
            if (sc->pos + 1 < sc->len && sc->sql[sc->pos + 1] == quote) {
                sc->pos += 2;
                continue;
            }
            sc->pos++;
            return;
        }
        if (quote == '\'' && c == '\\' && sc->pos + 1 < sc->len) {
            sc->pos++;
        }
        sc->pos++;
    }
}

static bool same_shard(const eql_table *t, const char *db, const char *part,
                       const char *ks)
{
    return strcmp(t->db, db) == 0 && strcmp(t->part, part) == 0 &&
           strcmp(t->ks, ks) == 0;
}

/* Collects Database.Partition.Keyspace references following FROM/JOIN
 * keywords. Malformed positions are ignored (SQLite reports them later). */
static int collect_refs(const char *sql, eql_table *tables, int ntables)
{
    eql_scan sc = { sql, 0, strlen(sql) };
    int found = 0;
    while (sc.pos < sc.len) {
        skip_spaces_comments(&sc);
        if (sc.pos >= sc.len) {
            break;
        }
        char c = sc.sql[sc.pos];
        if (c == '\'' || c == '"' || c == '`' || c == '[') {
            skip_quoted_raw(&sc);
            continue;
        }
        size_t start = sc.pos;
        while (sc.pos < sc.len && is_ident_char(sc.sql[sc.pos])) {
            sc.pos++;
        }
        if (sc.pos == start) {
            sc.pos++;
            continue;
        }
        size_t wlen = sc.pos - start;
        if (wlen != 4 && wlen != 5) {
            continue;
        }
        char word[8];
        memcpy(word, sc.sql + start, wlen);
        word[wlen] = '\0';
        for (size_t i = 0; i < wlen; i++) {
            word[i] = (char)tolower((unsigned char)word[i]);
        }
        if (strcmp(word, "from") != 0 && strcmp(word, "join") != 0) {
            continue;
        }

        char parts[3][256];
        bool ok = true;
        for (int seg = 0; seg < 3 && ok; seg++) {
            skip_spaces_comments(&sc);
            ok = read_ident(&sc, parts[seg], sizeof(parts[seg]));
            if (ok && seg < 2) {
                skip_spaces_comments(&sc);
                ok = sc.pos < sc.len && sc.sql[sc.pos] == '.';
                if (ok) {
                    sc.pos++;
                }
            }
        }
        if (!ok) {
            ok = true;
        }
        if (!ok || !parts[0][0] || !parts[1][0] || !parts[2][0]) {
            /* subquery or unsupported reference shape: let SQLite complain */
            continue;
        }
        bool dup = false;
        for (int i = 0; i < ntables + found; i++) {
            if (same_shard(&tables[i], parts[0], parts[1], parts[2])) {
                dup = true;
                break;
            }
        }
        if (!dup) {
            eql_table *t = &tables[ntables + found];
            memset(t, 0, sizeof(*t));
            snprintf(t->db, sizeof(t->db), "%s", parts[0]);
            snprintf(t->part, sizeof(t->part), "%s", parts[1]);
            snprintf(t->ks, sizeof(t->ks), "%s", parts[2]);
            snprintf(t->dotted, sizeof(t->dotted), "%s.%s.%s", t->db,
                     t->part, t->ks);
            snprintf(t->alias, sizeof(t->alias), "eql_t%d",
                     ntables + found);
            found++;
        }
    }
    return found;
}

/* Rewrites every occurrence of each table's dotted reference to its
 * alias. Quoted literals and comments are copied verbatim so a match can
 * never fire inside a string. Returns malloc'd text or NULL. */
static char *rewrite_sql(const char *sql, const eql_table *tables,
                         size_t ntables)
{
    size_t len = strlen(sql);
    char *out = malloc(len * 2 + 16);
    if (!out) {
        return NULL;
    }
    size_t cap = len * 2 + 15;
    size_t o = 0;
    eql_scan sc = { sql, 0, len };
#define EMIT(ch)                                                         \
    do {                                                                 \
        if (o < cap) {                                                   \
            out[o++] = (ch);                                             \
        }                                                                \
    } while (0)
    while (sc.pos < sc.len) {
        char c = sc.sql[sc.pos];
        bool maybe_ref = ntables > 0 && (c == '"' || c == '`' || c == '[' || is_ident_char(c));
        if (!maybe_ref && c == '\'') {
            size_t start = sc.pos;
            skip_quoted_raw(&sc);
            for (size_t i = start; i < sc.pos && o < cap; i++) {
                out[o++] = sc.sql[i];
            }
            continue;
        }
        if (c == '-' && sc.pos + 1 < sc.len && sc.sql[sc.pos + 1] == '-') {
            while (sc.pos < sc.len && sc.sql[sc.pos] != '\n') {
                EMIT(sc.sql[sc.pos++]);
            }
            continue;
        }
        if (c == '/' && sc.pos + 1 < sc.len && sc.sql[sc.pos + 1] == '*') {
            const char *end = strstr(sc.sql + sc.pos + 2, "*/");
            size_t stop = end ? (size_t)(end - sc.sql) + 2 : sc.len;
            for (; sc.pos < stop && o < cap; sc.pos++) {
                out[o++] = sc.sql[sc.pos];
            }
            continue;
        }
        if (c != '"' && c != '`' && c != '[' && !is_ident_char(c)) {
            EMIT(c);
            sc.pos++;
            continue;
        }

        bool matched = false;
        eql_scan probe = { sc.sql, sc.pos, sc.len };
        char segs[3][256];
        bool parse_ok = true;
        for (int seg = 0; seg < 3; seg++) {
            if (seg > 0) {
                skip_spaces_comments(&probe);
                if (probe.pos >= probe.len || probe.sql[probe.pos] != '.') {
                    parse_ok = false;
                    break;
                }
                probe.pos++;
                skip_spaces_comments(&probe);
            }
            if (!read_ident(&probe, segs[seg], sizeof(segs[seg]))) {
                parse_ok = false;
                break;
            }
        }
        if (parse_ok) {
            char cand[EQL_MAX_NAME];
            snprintf(cand, sizeof(cand), "%s.%s.%s", segs[0], segs[1],
                     segs[2]);
            for (size_t ti = 0; ti < ntables; ti++) {
                if (strcmp(cand, tables[ti].dotted) == 0) {
                    const char *alias = tables[ti].alias;
                    while (*alias && o < cap) {
                        out[o++] = *alias++;
                    }
                    sc.pos = probe.pos;
                    matched = true;
                    break;
                }
            }
        }
        if (!matched) {
            if (c == '"' || c == '`' || c == '[') {
                size_t start = sc.pos;
                skip_quoted_raw(&sc);
                for (size_t i = start; i < sc.pos && o < cap; i++) {
                    out[o++] = sc.sql[i];
                }
            } else {
                /* bare identifier: emit the whole word */
                while (sc.pos < sc.len && is_ident_char(sc.sql[sc.pos])) {
                    EMIT(sc.sql[sc.pos++]);
                }
            }
        }
    }
#undef EMIT
    out[o] = '\0';
    return out;
}

/* --- shard materialization ------------------------------------------- */

static bool perm_denied(const edb_eql_ctx *ctx, const eql_table *t,
                        uint64_t groups, bool trusted)
{
    if (trusted) {
        return false;
    }
    edb_partition_info part;
    if (!edb_partition_get(ctx->config, t->db, t->part, &part)) {
        return false;   /* implicit partition: allow-all masks */
    }
    return !edb_check_perm(part.read_mask, groups, EDB_PERM_READ);
}

/* Fetches the live documents of one shard as parallel arrays (ids +
 * values), mirroring REST read semantics: quorum merge when replication
 * applies, local otherwise. Returns array of value objects; ids_out
 * carries the record keys. */
static cJSON *fetch_docs(const edb_eql_ctx *ctx, const eql_table *t,
                         char ***ids_out, size_t *nids_out)
{
    *ids_out = NULL;
    *nids_out = 0;
    cJSON *meta = ctx->repl
                      ? edb_repl_read_query_meta(ctx->repl, t->db, t->part,
                                                 t->ks, NULL)
                      : edb_query_ts(ctx->engine, t->part, t->ks, NULL);
    if (!meta) {
        return cJSON_CreateArray();
    }
    size_t n = 0;
    size_t cap = 16;
    char **ids = malloc(cap * sizeof(*ids));
    if (!ids) {
        cJSON_Delete(meta);
        return cJSON_CreateArray();
    }
    cJSON *docs = cJSON_CreateArray();
    if (!docs) {
        free(ids);
        cJSON_Delete(meta);
        return NULL;
    }
    const cJSON *row = NULL;
    cJSON_ArrayForEach(row, meta) {
        const cJSON *jid = cJSON_GetObjectItemCaseSensitive(row, "id");
        cJSON *jval = cJSON_GetObjectItemCaseSensitive(row, "value");
        if (!cJSON_IsObject(jval)) {
            continue;
        }
        if (n == cap) {
            size_t grown = cap * 2;
            char **grown_ids = realloc(ids, grown * sizeof(*grown_ids));
            if (!grown_ids) {
                continue;
            }
            ids = grown_ids;
            cap = grown;
        }
        ids[n] = strdup(cJSON_IsString(jid) && jid->valuestring
                            ? jid->valuestring : "");
        cJSON *doc = cJSON_Duplicate(jval, 1);
        if (!doc || !ids[n]) {
            free(ids[n]);
            cJSON_Delete(doc);
            continue;
        }
        /* keep documents ordered by last-modified like the REST reads */
        cJSON_AddItemToArray(docs, doc);
        n++;
    }
    cJSON_Delete(meta);
    *ids_out = ids;
    *nids_out = n;
    return docs;
}

/* Adds every object member name of doc to the column union in encounter
 * order; the record key stays at position 0 ("id"). */
static void union_columns(eql_table *t, const cJSON *doc)
{
    const cJSON *item = NULL;
    cJSON_ArrayForEach(item, doc) {
        const char *key = item->string;
        if (!key) {
            continue;
        }
        bool seen = false;
        for (size_t i = 0; i < t->ncols; i++) {
            if (strcmp(t->cols[i], key) == 0) {
                seen = true;
                break;
            }
        }
        if (!seen && t->ncols < EQL_MAX_COLS) {
            snprintf(t->cols[t->ncols++], sizeof(t->cols[0]), "%s", key);
        }
    }
}

static void bind_json(sqlite3_stmt *stmt, int index, const cJSON *v)
{
    if (!v || cJSON_IsNull(v)) {
        sqlite3_bind_null(stmt, index);
        return;
    }
    if (cJSON_IsBool(v)) {
        sqlite3_bind_int(stmt, index, cJSON_IsTrue(v) ? 1 : 0);
        return;
    }
    if (cJSON_IsNumber(v)) {
        double d = v->valuedouble;
        if (d >= -9.2e18 && d <= 9.2e18 &&
            d == (double)(long long)d) {
            sqlite3_bind_int64(stmt, index, (long long)d);
        } else {
            sqlite3_bind_double(stmt, index, d);
        }
        return;
    }
    if (cJSON_IsString(v)) {
        sqlite3_bind_text(stmt, index,
                          v->valuestring ? v->valuestring : "", -1,
                          SQLITE_STATIC);
        return;
    }
    char *text = cJSON_PrintUnformatted(v);
    if (text) {
        sqlite3_bind_text(stmt, index, text, -1, SQLITE_TRANSIENT);
    } else {
        sqlite3_bind_null(stmt, index);
    }
    free(text);
}

/* Creates the shadow table for a shard and inserts its documents.
 * Returns 0 on success, an HTTP-style code otherwise. Takes ownership of
 * docs (frees it); ids/nids are borrowed. */
static int materialize(sqlite3 *db, const eql_table *t, cJSON *docs,
                       char **ids, size_t nids)
{
    char sql[4096];
    size_t used = 0;
    used += (size_t)snprintf(sql + used, sizeof(sql) - used,
                             "CREATE TABLE \"%s\" (\"id\" TEXT PRIMARY KEY",
                             t->alias);
    for (size_t i = 1; i < t->ncols; i++) {
        int wrote = snprintf(sql + used, sizeof(sql) - used, ", \"%s\"",
                             t->cols[i]);
        if (wrote < 0 || (size_t)wrote >= sizeof(sql) - used) {
            cJSON_Delete(docs);
            return 500;
        }
        used += (size_t)wrote;
    }
    if (!t->ncols) {
        cJSON_Delete(docs);
        return 500;
    }
    strncat(sql, ")", sizeof(sql) - strlen(sql) - 1);
    char *errmsg = NULL;
    if (sqlite3_exec(db, sql, NULL, NULL, &errmsg) != SQLITE_OK) {
        fprintf(stderr, "eql: create %s failed: %s\n", t->alias,
                errmsg ? errmsg : "?");
        sqlite3_free(errmsg);
        cJSON_Delete(docs);
        return 500;
    }

    size_t insert_cap = strlen(sql) + 16;
    char *insert_sql = malloc(insert_cap);
    if (!insert_sql) {
        cJSON_Delete(docs);
        return 500;
    }
    size_t ins_used = 0;
    ins_used += (size_t)snprintf(insert_sql, insert_cap - ins_used,
                                 "INSERT OR REPLACE INTO \"%s\" VALUES (?",
                                 t->alias);
    for (size_t i = 1; i < t->ncols; i++) {
        int wrote = snprintf(insert_sql + ins_used, insert_cap - ins_used,
                             ",?");
        if (wrote < 0 || (size_t)wrote >= insert_cap - ins_used) {
            free(insert_sql);
            cJSON_Delete(docs);
            return 500;
        }
        ins_used += (size_t)wrote;
    }
    strncat(insert_sql, ")", insert_cap - strlen(insert_sql) - 1);

    sqlite3_stmt *stmt = NULL;
    if (sqlite3_prepare_v2(db, insert_sql, -1, &stmt, NULL) != SQLITE_OK) {
        fprintf(stderr, "eql: prepare insert failed: %s\n",
                sqlite3_errmsg(db));
        free(insert_sql);
        cJSON_Delete(docs);
        return 500;
    }
    free(insert_sql);

    int status = 0;
    for (size_t r = 0; r < nids && status == 0; r++) {
        sqlite3_reset(stmt);
        sqlite3_clear_bindings(stmt);
        sqlite3_bind_text(stmt, 1, ids[r], -1, SQLITE_STATIC);
        const cJSON *doc = cJSON_GetArrayItem(docs, (int)r);
        for (size_t ci = 1; ci < t->ncols; ci++) {
            bind_json(stmt, (int)(ci + 1),
                      cJSON_GetObjectItemCaseSensitive(doc, t->cols[ci]));
        }
        if (sqlite3_step(stmt) != SQLITE_DONE) {
            fprintf(stderr, "eql: insert failed: %s\n", sqlite3_errmsg(db));
            status = 500;
        }
    }
    sqlite3_finalize(stmt);
    cJSON_Delete(docs);
    return status;
}

/* Hardens the connection against filesystem/network escape hatches
 * before user SQL is ever prepared. */
static void harden_connection(sqlite3 *db)
{
    int off = 0;
    sqlite3_db_config(db, SQLITE_DBCONFIG_ENABLE_LOAD_EXTENSION, 0, &off);
    sqlite3_limit(db, SQLITE_LIMIT_ATTACHED, 0);
    sqlite3_limit(db, SQLITE_LIMIT_SQL_LENGTH, 1 << 20);
    sqlite3_limit(db, SQLITE_LIMIT_COMPOUND_SELECT, 512);
}

static int deny_authorizer(void *ud, int action, const char *a1,
                           const char *a2, const char *db_name,
                           const char *trigger)
{
    (void)a1;
    (void)a2;
    (void)db_name;
    (void)trigger;
    switch (action) {
    case SQLITE_DETACH:
    case SQLITE_ATTACH:
        return SQLITE_DENY;
    default:
        (void)ud;
        return SQLITE_OK;
    }
}

/* --- statement execution --------------------------------------------- */

static char *error_response(const char *message)
{
    cJSON *obj = cJSON_CreateObject();
    if (!obj) {
        return NULL;
    }
    cJSON_AddStringToObject(obj, "status", "error");
    cJSON_AddStringToObject(obj, "message", message);
    char *text = cJSON_PrintUnformatted(obj);
    cJSON_Delete(obj);
    return text;
}

int edb_eql_execute(const edb_eql_ctx *ctx, const char *sql,
                    uint64_t user_groups, bool trusted, char **json_out)
{
    if (json_out) {
        *json_out = NULL;
    }
    if (!ctx || !ctx->engine || !ctx->config || !json_out ||
        !sql || !*sql) {
        return 400;
    }

    int status = 200;
    eql_table tables[EQL_MAX_TABLES];
    memset(tables, 0, sizeof(tables));
    int ntables = collect_refs(sql, tables, 0);
    if (ntables <= 0) {
        *json_out = error_response(
            "no Database.Partition.Keyspace reference found in FROM/JOIN");
        return 400;
    }

    for (int i = 0; i < ntables && status == 200; i++) {
        if (perm_denied(ctx, &tables[i], user_groups, trusted)) {
            status = 403;
        }
    }
    if (status != 200) {
        *json_out = error_response("permission denied");
        return status;
    }

    for (int i = 0; i < ntables; i++) {
        snprintf(tables[i].cols[tables[i].ncols++],
                 sizeof(tables[i].cols[0]), "id");
    }

    char *rewritten = rewrite_sql(sql, tables, (size_t)ntables);
    if (!rewritten) {
        return 500;
    }

    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        free(rewritten);
        return 500;
    }
    harden_connection(db);

    for (int i = 0; i < ntables && status == 200; i++) {
        size_t nids = 0;
        char **ids = NULL;
        cJSON *docs = fetch_docs(ctx, &tables[i], &ids, &nids);
        if (!docs) {
            status = 500;
            break;
        }
        const cJSON *doc = NULL;
        cJSON_ArrayForEach(doc, docs) {
            union_columns(&tables[i], doc);
        }
        status = materialize(db, &tables[i], docs, ids, nids);
        if (status == 0) {
            status = 200;
        }

        for (size_t k = 0; k < nids; k++) {
            free(ids[k]);
        }
        free(ids);
    }
    if (status != 200) {
        free(rewritten);
        sqlite3_close(db);
        return status > 0 ? status : 500;
    }

    sqlite3_set_authorizer(db, deny_authorizer, NULL);

    const char *tail = NULL;
    if (sqlite3_prepare_v2(db, rewritten, -1, &stmt, &tail) != SQLITE_OK) {
        const char *msg = sqlite3_errmsg(db);
        *json_out = error_response(msg ? msg : "prepare failed");
        free(rewritten);
        sqlite3_close(db);
        return 400;
    }

    while (tail && (*tail == ';' || *tail == ' ' || *tail == '\n' ||
                    *tail == '\r' || *tail == '\t')) {
        tail++;
    }
    if (tail && *tail) {
        *json_out = error_response("one statement per request");
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        free(rewritten);
        return 400;
    }
    free(rewritten);
    if (!stmt || !sqlite3_stmt_readonly(stmt)) {
        *json_out =
            error_response("only SELECT statements are supported");
        if (stmt) {
            sqlite3_finalize(stmt);
        }
        sqlite3_close(db);
        return 400;
    }

    int ncols = sqlite3_column_count(stmt);
    cJSON *result = cJSON_CreateObject();
    cJSON *cols_json = result ? cJSON_AddArrayToObject(result, "columns")
                              : NULL;
    cJSON *rows_json = result ? cJSON_AddArrayToObject(result, "rows")
                              : NULL;
    if (!result || !cols_json || !rows_json) {
        cJSON_Delete(result);
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return 500;
    }
    cJSON_AddStringToObject(result, "status", "ok");
    for (int c = 0; c < ncols; c++) {
        const char *name = sqlite3_column_name(stmt, c);
        cJSON_AddItemToArray(cols_json, cJSON_CreateString(name ? name : ""));
    }

    int step;
    while ((step = sqlite3_step(stmt)) == SQLITE_ROW) {
        cJSON *row = cJSON_CreateArray();
        if (!row) {
            step = SQLITE_ERROR;
            break;
        }
        for (int c = 0; c < ncols; c++) {
            cJSON *val = NULL;
            switch (sqlite3_column_type(stmt, c)) {
            case SQLITE_INTEGER:
                val = cJSON_CreateNumber(
                    (double)sqlite3_column_int64(stmt, c));
                break;
            case SQLITE_FLOAT:
                val = cJSON_CreateNumber(sqlite3_column_double(stmt, c));
                break;
            case SQLITE_TEXT:
                val = cJSON_CreateString(
                    (const char *)sqlite3_column_text(stmt, c));
                break;
            case SQLITE_BLOB: {
                const unsigned char *blob =
                    (const unsigned char *)sqlite3_column_blob(stmt, c);
                size_t blen = (size_t)sqlite3_column_bytes(stmt, c);
                char *hex = malloc(blen * 2 + 1);
                if (!hex) {
                    val = cJSON_CreateNull();
                    break;
                }
                static const char digits[] = "0123456789abcdef";
                for (size_t b = 0; b < blen; b++) {
                    hex[b * 2] = digits[blob[b] >> 4];
                    hex[b * 2 + 1] = digits[blob[b] & 0xF];
                }
                hex[blen * 2] = '\0';
                val = cJSON_CreateString(hex);
                free(hex);
                break;
            }
            default:
                val = cJSON_CreateNull();
                break;
            }
            if (!val) {
                val = cJSON_CreateNull();
            }
            cJSON_AddItemToArray(row, val);
        }
        cJSON_AddItemToArray(rows_json, row);
    }
    if (step != SQLITE_DONE) {
        const char *msg = sqlite3_errmsg(db);
        cJSON_Delete(result);
        *json_out = error_response(msg ? msg : "statement execution failed");
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return 400;
    }
    sqlite3_finalize(stmt);
    sqlite3_close(db);

    *json_out = cJSON_PrintUnformatted(result);
    cJSON_Delete(result);
    if (!*json_out) {
        *json_out = error_response("encode failed");
        return 500;
    }
    return 200;
}
