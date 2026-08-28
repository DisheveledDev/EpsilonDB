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
#include <time.h>

#include "../epsilon_log.h"
#include "../lua/epsilon_lua.h"
#include "../../vendor/sqlite/sqlite3.h"

#define EQL_MAX_TABLES 16
#define EQL_MAX_COLS 256
#define EQL_MAX_NAME 512

typedef struct {
    char db[128];
    char part[256];
    char ks[128];                /* empty for partition-wide groups */
    char dotted[EQL_MAX_NAME];   /* canonical db.part.ks (db.part for pw) */
    char alias[16];              /* eql_t0 .. */
    char cols[EQL_MAX_COLS][128];
    uint8_t kinds[EQL_MAX_COLS]; /* nonzero: column carried objects/arrays */
    size_t ncols;
    bool pw;                     /* partition-wide: merged keyspace table */
    char keyspaces[64][128];     /* pw: materialized keyspaces */
    size_t nkeys;
} eql_table;


typedef struct {
    const char *sql;
    size_t pos;
    size_t len;
} eql_scan;

/* Buffers the row events sqlite fires while a user statement runs. They are
 * only replicated once the statement reaches SQLITE_DONE, so a constraint
 * failure rolling the statement back discards every buffered event. */
#define EQL_MAX_EVENTS (1u << 20)
#define EQL_ID_CAP 512

typedef struct {
    const eql_table *t;
    int op;                  /* SQLITE_INSERT / SQLITE_UPDATE / SQLITE_DELETE */
    sqlite3_int64 rowid;
} eql_dml_ev;

typedef struct {
    eql_dml_ev *ev;
    size_t n;
    size_t cap;
    bool overflow;           /* event cap hit: refuse to replicate */
} eql_trapbuf;

typedef struct {
    const eql_table *t;
    sqlite3_int64 rowid;
    char id[EQL_ID_CAP];
    char ks[128];                /* owning keyspace (pw tables) */
} eql_rowrec;

typedef struct {
    eql_rowrec *r;
    size_t n;
    size_t cap;
} eql_rowmap;

/* Per-execution authorizer state: DML is allowed on shard-backed tables
 * for exactly one action class; everything else keeps the eql-b policy. */
typedef struct {
    const eql_table *tables;
    size_t ntables;
    int allow_op;            /* 0: read-only mode */
} eql_acl;


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


typedef struct {
    const eql_table *tables;
    size_t ntables;
    eql_trapbuf *buf;
} eql_trap_bundle;

static void traps_push(eql_trapbuf *b, const eql_table *t, int op,
                       sqlite3_int64 rowid)
{
    if (b->overflow) {
        return;
    }
    if (b->n == b->cap) {
        size_t grown = b->cap ? b->cap * 2 : 256;
        if (grown > EQL_MAX_EVENTS) {
            b->overflow = true;
            return;
        }
        eql_dml_ev *grown_ev = realloc(b->ev, grown * sizeof(*grown_ev));
        if (!grown_ev) {
            b->overflow = true;
            return;
        }
        b->ev = grown_ev;
        b->cap = grown;
    }
    b->ev[b->n].t = t;
    b->ev[b->n].op = op;
    b->ev[b->n].rowid = rowid;
    b->n++;
}

static void trap_row_update(void *ud, int op, const char *db,
                            const char *table, sqlite3_int64 rowid)
{
    (void)db;
    eql_trap_bundle *bd = ud;
    for (size_t i = 0; i < bd->ntables; i++) {
        if (strcmp(bd->tables[i].alias, table) == 0) {
            traps_push(bd->buf, &bd->tables[i], op, rowid);
            return;
        }
    }
}

static void map_add_ks(eql_rowmap *m, const eql_table *t,
                       sqlite3_int64 rowid, const char *id, const char *ks)
{
    if (m->n == m->cap) {
        size_t grown = m->cap ? m->cap * 2 : 128;
        eql_rowrec *g = realloc(m->r, grown * sizeof(*g));
        if (!g) {
            return;   /* lookup degrades to unknown-id for that record */
        }
        m->r = g;
        m->cap = grown;
    }
    snprintf(m->r[m->n].id, EQL_ID_CAP, "%s", id ? id : "");
    snprintf(m->r[m->n].ks, sizeof(m->r[m->n].ks), "%s", ks ? ks : "");
    m->r[m->n].t = t;
    m->r[m->n].rowid = rowid;
    m->n++;
}

static bool map_take(eql_rowmap *m, const eql_table *t,
                     sqlite3_int64 rowid, char id_out[EQL_ID_CAP],
                     char ks_out[128])
{
    for (size_t i = m->n; i > 0; i--) {
        eql_rowrec *e = &m->r[i - 1];
        if (e->t == t && e->rowid == rowid) {
            memcpy(id_out, e->id, EQL_ID_CAP);
            snprintf(ks_out, 128, "%s", e->ks);
            memmove(&m->r[i - 1], &m->r[i], (m->n - i) * sizeof(*m->r));
            m->n--;
            return true;
        }
    }
    return false;
}

static void map_free(eql_rowmap *m)
{
    free(m->r);
    m->r = NULL;
    m->n = m->cap = 0;
}


/* Parses db.part.ks at the cursor; on success advances past it. A
 * trailing db.part (no third segment) is also accepted: parts[2] is left
 * empty and the reference is partition-wide. */
static bool parse_ref2(eql_scan *sc, char parts[3][256])
{
    bool ok = true;
    for (int seg = 0; seg < 3 && ok; seg++) {
        skip_spaces_comments(sc);
        ok = read_ident(sc, parts[seg], 256);
        if (!ok) {
            break;
        }
        if (seg < 2) {
            skip_spaces_comments(sc);
            if (sc->pos < sc->len && sc->sql[sc->pos] == '.') {
                sc->pos++;
            } else {
                if (seg == 1) {
                    parts[2][0] = '\0';   /* partition-wide reference */
                    return true;
                }
                ok = false;   /* db. alone is not a usable reference */
            }
        }
    }
    return ok;
}

/* Collects Database.Partition.Keyspace references following FROM/JOIN
 * keywords and comma-separated source lists. Malformed positions are
 * ignored (SQLite reports them later). */
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
        if (wlen != 4 && wlen != 5 && wlen != 6) {
            continue;
        }
        char word[8];
        memcpy(word, sc.sql + start, wlen);
        word[wlen] = '\0';
        for (size_t i = 0; i < wlen; i++) {
            word[i] = (char)tolower((unsigned char)word[i]);
        }
        if (strcmp(word, "from") != 0 && strcmp(word, "join") != 0 &&
            strcmp(word, "into") != 0 && strcmp(word, "update") != 0) {
            continue;
        }

        char parts[3][256];
        if (!parse_ref2(&sc, parts)) {
            /* subquery or unsupported reference shape: let SQLite complain */
            continue;
        }
        bool pw = parts[2][0] == '\0';
        bool dup = false;
        for (int i = 0; i < ntables + found; i++) {
            if (same_shard(&tables[i], parts[0], parts[1], parts[2]) &&
                tables[i].pw == pw) {
                dup = true;
                break;
            }
        }
        if (!dup && ntables + found < EQL_MAX_TABLES) {
            eql_table *t = &tables[ntables + found];
            memset(t, 0, sizeof(*t));
            snprintf(t->db, sizeof(t->db), "%s", parts[0]);
            snprintf(t->part, sizeof(t->part), "%s", parts[1]);
            snprintf(t->ks, sizeof(t->ks), "%s", parts[2]);
            if (pw) {
                snprintf(t->dotted, sizeof(t->dotted), "%s.%s", t->db,
                         t->part);
            } else {
                snprintf(t->dotted, sizeof(t->dotted), "%s.%s.%s", t->db,
                         t->part, t->ks);
            }
            t->pw = pw;
            snprintf(t->alias, sizeof(t->alias), "eql_t%d",
                     ntables + found);
            found++;
        }

        /* optional alias: AS ident | bare-ident (skipped; not a keyword we
         * scan for) */
        {
            size_t save = sc.pos;
            skip_spaces_comments(&sc);
            size_t wstart = sc.pos;
            while (sc.pos < sc.len && is_ident_char(sc.sql[sc.pos])) {
                sc.pos++;
            }
            bool consumed = false;
            if (sc.pos > wstart) {
                char low[8] = {0};
                size_t wl = sc.pos - wstart;
                if (wl < sizeof(low)) {
                    memcpy(low, sc.sql + wstart, wl);
                    low[wl] = '\0';
                    for (size_t k = 0; k < wl; k++) {
                        low[k] = (char)tolower((unsigned char)low[k]);
                    }
                    if (strcmp(low, "as") == 0) {
                        skip_spaces_comments(&sc);
                        wstart = sc.pos;
                        while (sc.pos < sc.len &&
                               is_ident_char(sc.sql[sc.pos])) {
                            sc.pos++;
                        }
                        consumed = sc.pos > wstart;
                    } else if (wl != 4 && wl != 5) {
                        consumed = true;
                    }
                }
            }
            if (!consumed) {
                sc.pos = save;
            }
        }
        /* comma-separated additional sources: FROM t1 alias , t2 alias */
        for (;;) {
            size_t save2 = sc.pos;
            skip_spaces_comments(&sc);
            if (sc.pos >= sc.len || sc.sql[sc.pos] != ',') {
                sc.pos = save2;
                break;
            }
            char more[3][256];
            sc.pos++;
            if (!parse_ref2(&sc, more)) {
                /* not another source list element (e.g. subquery or AS):
                 * rewind so SQLite sees the original text */
                sc.pos = save2;
                break;
            }
            bool more_pw = more[2][0] == '\0';
            bool dup2 = false;
            for (int i = 0; i < ntables + found; i++) {
                if (same_shard(&tables[i], more[0], more[1], more[2]) &&
                    tables[i].pw == more_pw) {
                    dup2 = true;
                    break;
                }
            }
            if (!dup2 && ntables + found < EQL_MAX_TABLES) {
                eql_table *t = &tables[ntables + found];
                memset(t, 0, sizeof(*t));
                snprintf(t->db, sizeof(t->db), "%s", more[0]);
                snprintf(t->part, sizeof(t->part), "%s", more[1]);
                snprintf(t->ks, sizeof(t->ks), "%s", more[2]);
                if (more_pw) {
                    snprintf(t->dotted, sizeof(t->dotted), "%s.%s", t->db,
                             t->part);
                } else {
                    snprintf(t->dotted, sizeof(t->dotted), "%s.%s.%s",
                             t->db, t->part, t->ks);
                }
                t->pw = more_pw;
                snprintf(t->alias, sizeof(t->alias), "eql_t%d",
                         ntables + found);
                found++;
            }
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
            /* two-segment (partition-wide) candidate: only when the text
             * really stops after the second segment, so a 3-part dotted
             * reference is never swallowed by a pw table that shares its
             * db/part prefix (the pw alias must win only for db.part) */
            eql_scan probe2 = { sc.sql, sc.pos, sc.len };
            char p2segs[2][256];
            bool pw_ok = read_ident(&probe2, p2segs[0], sizeof(p2segs[0]));
            if (pw_ok) {
                skip_spaces_comments(&probe2);
                pw_ok = probe2.pos < probe2.len &&
                        probe2.sql[probe2.pos] == '.';
                if (pw_ok) {
                    probe2.pos++;
                    skip_spaces_comments(&probe2);
                    pw_ok = read_ident(&probe2, p2segs[1],
                                       sizeof(p2segs[1]));
                }
            }
            if (pw_ok) {
                char cand2[EQL_MAX_NAME];
                snprintf(cand2, sizeof(cand2), "%s.%s", p2segs[0],
                         p2segs[1]);
                for (size_t ti = 0; ti < ntables; ti++) {
                    if (tables[ti].pw &&
                        strcmp(cand2, tables[ti].dotted) == 0) {
                        const char *alias = tables[ti].alias;
                        while (*alias && o < cap) {
                            out[o++] = *alias++;
                        }
                        sc.pos = probe2.pos;
                        matched = true;
                        break;
                    }
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
                        uint64_t groups, bool trusted,
                        edb_permission perm)
{
    if (trusted) {
        return false;
    }
    edb_partition_info part;
    if (!edb_partition_get(ctx->config, t->db, t->part, &part)) {
        return false;   /* implicit partition: allow-all masks */
    }
    uint64_t mask;
    switch (perm) {
    case EDB_PERM_CREATE: mask = part.create_mask; break;
    case EDB_PERM_UPDATE: mask = part.update_mask; break;
    case EDB_PERM_DELETE: mask = part.delete_mask; break;
    default:              mask = part.read_mask;  break;
    }
    return !edb_check_perm(mask, groups, perm);
}

/* Classifies the statement head keyword. */
static eql_kind classify_statement(const char *sql)
{
    eql_scan sc = { sql, 0, strlen(sql) };
    skip_spaces_comments(&sc);
    char word[16];
    size_t wl = 0;
    while (sc.pos < sc.len && is_ident_char(sc.sql[sc.pos]) &&
           wl + 1 < sizeof(word)) {
        word[wl++] = (char)tolower((unsigned char)sc.sql[sc.pos]);
        sc.pos++;
    }
    word[wl] = '\0';
    if (strcmp(word, "delete") == 0) {
        return EQL_KIND_DELETE;
    }
    if (strcmp(word, "update") == 0) {
        return EQL_KIND_UPDATE;
    }
    if (strcmp(word, "insert") == 0 || strcmp(word, "replace") == 0) {
        return EQL_KIND_INSERT;
    }
    if (strcmp(word, "select") == 0 || strcmp(word, "values") == 0 ||
        strcmp(word, "explain") == 0) {
        return EQL_KIND_SELECT;
    }
    return EQL_KIND_OTHER;
}

int edb_eql_classify(const char *sql)
{
    return sql && *sql ? (int)classify_statement(sql) : (int)EQL_KIND_OTHER;
}

size_t edb_eql_references(const char *sql, char out[][512], size_t cap)
{
    if (!sql || !*sql || cap == 0) {
        return 0;
    }
    eql_table *tables = calloc(EQL_MAX_TABLES, sizeof(*tables));
    int n = collect_refs(sql, tables, 0);
    if (n > (int)cap) {
        n = (int)cap;
    }
    for (int i = 0; i < n; i++) {
        snprintf(out[i], 512, "%s", tables[i].dotted);
    }
    free(tables);
    return (size_t)n;
}



/* Adds a column to a shadow table's schema unless already present.
 * Shared by the INSERT/UPDATE column scanners. */
static void table_add_column(eql_table *t, const char *name)
{
    for (size_t i = 0; i < t->ncols; i++) {
        if (strcmp(t->cols[i], name) == 0) {
            return;
        }
    }
    if (t->ncols < EQL_MAX_COLS) {
        snprintf(t->cols[t->ncols++], sizeof(t->cols[0]), "%s", name);
    }
}

/* For INSERT statements, picks up an explicit column list following the
 * INTO reference so writes into previously-empty shards create usable
 * shadow tables. */
static void scan_insert_columns(const char *sql, eql_table *tables,
                                size_t ntables)
{
    eql_scan sc = { sql, 0, strlen(sql) };
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
        if (wlen != 4) {
            continue;
        }
        char word[8];
        memcpy(word, sc.sql + start, wlen);
        word[wlen] = '\0';;
        for (size_t i = 0; i < wlen; i++) {
            word[i] = (char)tolower((unsigned char)word[i]);
        }
        if (strcmp(word, "into") != 0) {
            continue;
        }

        char parts[3][256];
        if (!parse_ref2(&sc, parts)) {
            continue;
        }
        eql_table *t = NULL;
        for (size_t i = 0; i < ntables; i++) {
            if (same_shard(&tables[i], parts[0], parts[1], parts[2])) {
                t = &tables[i];
                break;
            }
        }
        if (!t) {
            continue;
        }
        skip_spaces_comments(&sc);
        if (sc.pos >= sc.len || sc.sql[sc.pos] != '(') {
            continue;   /* INSERT ... VALUES without a column list */
        }
        sc.pos++;
        for (;;) {
            char col[128];
            skip_spaces_comments(&sc);
            if (!read_ident(&sc, col, sizeof(col))) {
                break;
            }
            table_add_column(t, col);
            skip_spaces_comments(&sc);
            if (sc.pos < sc.len && sc.sql[sc.pos] == ',') {
                sc.pos++;
                continue;
            }
            break;
        }
    }
}

/* For UPDATE statements, picks up the assignment targets in the SET
 * clause so a write can introduce JSON keys no fetched document has
 * (schema-assigning DML): the shadow table is created with those columns
 * up front, otherwise prepare would fail with "no such column". Only the
 * assignment LHS is collected; the value expression is skipped with
 * quote/paren awareness until a top-level clause keyword or the next
 * top-level comma starts another assignment. */
static void scan_update_columns(const char *sql, eql_table *tables,
                                size_t ntables)
{
    eql_scan sc = { sql, 0, strlen(sql) };
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
        if (wlen != 6) {
            continue;
        }
        char word[8];
        memcpy(word, sc.sql + start, wlen);
        word[wlen] = '\0';
        for (size_t i = 0; i < wlen; i++) {
            word[i] = (char)tolower((unsigned char)word[i]);
        }
        if (strcmp(word, "update") != 0) {
            continue;
        }

        char parts[3][256];
        if (!parse_ref2(&sc, parts)) {
            continue;
        }
        bool pw = parts[2][0] == '\0';
        eql_table *t = NULL;
        for (size_t i = 0; i < ntables; i++) {
            if (same_shard(&tables[i], parts[0], parts[1], parts[2]) &&
                tables[i].pw == pw) {
                t = &tables[i];
                break;
            }
        }
        if (!t) {
            continue;
        }

        /* optional table alias, then the SET keyword */
        bool found_set = false;
        for (;;) {
            skip_spaces_comments(&sc);
            if (sc.pos >= sc.len) {
                break;
            }
            char alias[128];
            if (!read_ident(&sc, alias, sizeof(alias))) {
                sc.pos++;
                continue;
            }
            for (size_t i = 0; alias[i]; i++) {
                alias[i] = (char)tolower((unsigned char)alias[i]);
            }
            if (strcmp(alias, "as") == 0) {
                skip_spaces_comments(&sc);
                read_ident(&sc, alias, sizeof(alias));
                continue;
            }
            if (strcmp(alias, "set") == 0) {
                found_set = true;
                break;
            }
        }
        if (!found_set) {
            continue;
        }

        int depth = 0;
        for (;;) {
            skip_spaces_comments(&sc);
            if (sc.pos >= sc.len) {
                break;
            }
            char c2 = sc.sql[sc.pos];
            if (c2 == '\'' || c2 == '"' || c2 == '`' || c2 == '[') {
                skip_quoted_raw(&sc);
                continue;
            }
            if (c2 == '(') {
                depth++;
                sc.pos++;
                continue;
            }
            if (c2 == ')') {
                if (depth > 0) {
                    depth--;
                }
                sc.pos++;
                continue;
            }
            if (c2 == ',' && depth == 0) {
                sc.pos++;
                continue;
            }
            if (!is_ident_char(c2)) {
                sc.pos++;
                continue;
            }
            char col[128];
            bool quoted = c2 == '"' || c2 == '`' || c2 == '[';
            if (!read_ident(&sc, col, sizeof(col))) {
                sc.pos++;
                continue;
            }
            /* clause keywords (bare, unquoted) end the SET clause */
            char low[128];
            snprintf(low, sizeof(low), "%s", col);
            for (size_t i = 0; low[i]; i++) {
                low[i] = (char)tolower((unsigned char)low[i]);
            }
            if (!quoted && depth == 0 &&
                (strcmp(low, "where") == 0 || strcmp(low, "returning") == 0 ||
                 strcmp(low, "limit") == 0 || strcmp(low, "order") == 0 ||
                 strcmp(low, "offset") == 0 || strcmp(low, "from") == 0 ||
                 strcmp(low, "select") == 0 || strcmp(low, "values") == 0)) {
                break;
            }
            size_t after = sc.pos;
            skip_spaces_comments(&sc);
            if (depth == 0 && sc.pos < sc.len && sc.sql[sc.pos] == '=') {
                table_add_column(t, col);
                sc.pos++;
                continue;
            }
            sc.pos = after;
        }
    }
}


/* Fetches the live documents of one shard as parallel arrays (ids +
 * values), mirroring REST read semantics: quorum merge when replication
 * applies, local otherwise. Returns array of value objects; ids_out
 * carries the record keys. */
static cJSON *fetch_shard_docs(const edb_eql_ctx *ctx, const char *db,
                               const char *part, const char *ks,
                               char ***ids_out, size_t *nids_out)
{
    *ids_out = NULL;
    *nids_out = 0;
    cJSON *meta = ctx->repl
                      ? edb_repl_read_query_meta(ctx->repl, db, part,
                                                 ks, NULL)
                      : edb_query_ts(ctx->engine, part, ks, NULL);
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

/* Partition-wide fetch: merges every registered keyspace of one
 * partition into a single document/id list. ks_out receives one owning
 * keyspace name per row (index-aligned with the ids array) so write-back
 * can route each record to its real shard. Returns NULL docs only on
 * unrecoverable allocation failure; an unregistered partition yields an
 * empty result like the REST reads. */
static cJSON *fetch_docs(const edb_eql_ctx *ctx, eql_table *t,
                         char ***ids_out, size_t *nids_out,
                         char ***ks_out, size_t *nks_out)
{
    *ks_out = NULL;
    *nks_out = 0;
    if (!t->pw) {
        cJSON *docs = fetch_shard_docs(ctx, t->db, t->part, t->ks,
                                       ids_out, nids_out);
        return docs;
    }

    size_t total = 0;
    edb_keyspace_info *list = edb_keyspace_list(ctx->config, &total);
    if (!list) {
        return cJSON_CreateArray();
    }
    size_t nkeys = 0;
    for (size_t i = 0; i < total && nkeys < 64; i++) {
        if (strcmp(list[i].database, t->db) == 0 &&
            strcmp(list[i].partition, t->part) == 0) {
            snprintf(t->keyspaces[nkeys], sizeof(t->keyspaces[0]), "%s",
                     list[i].name);
            nkeys++;
        }
    }
    free(list);
    t->nkeys = nkeys;
    if (!nkeys) {
        return cJSON_CreateArray();
    }

    size_t cap = 16, n = 0;
    char **ids = malloc(cap * sizeof(*ids));
    char **kss = malloc(cap * sizeof(*kss));
    cJSON *docs = cJSON_CreateArray();
    if (!ids || !kss || !docs) {
        free(ids);
        free(kss);
        cJSON_Delete(docs);
        return NULL;
    }
    for (size_t k = 0; k < nkeys; k++) {
        char **shard_ids = NULL;
        size_t shard_n = 0;
        cJSON *shard_docs =
            fetch_shard_docs(ctx, t->db, t->part, t->keyspaces[k],
                             &shard_ids, &shard_n);
        if (!shard_docs) {
            for (size_t i = 0; i < n; i++) {
                free(ids[i]);
                free(kss[i]);
            }
            free(ids);
            free(kss);
            cJSON_Delete(docs);
            return NULL;
        }
        for (size_t r = 0; r < shard_n; r++) {
            if (n == cap) {
                size_t grown = cap * 2;
                char **gi = realloc(ids, grown * sizeof(*gi));
                char **gk = realloc(kss, grown * sizeof(*gk));
                if (!gi || !gk) {
                    free(gi ? gi : ids);
                    free(gk ? gk : kss);
                    /* on partial growth the stale pointer must not leak:
                     * whichever realloc succeeded is covered by the other
                     * branch's free of the original block only when the
                     * original is still valid; the safe path is to abort
                     * with what we own */
                    cJSON_Delete(shard_docs);
                    for (size_t i = 0; i < n; i++) {
                        free(ids[i]);
                        free(kss[i]);
                    }
                    if (gi) {
                        ids = gi;
                    }
                    if (gk) {
                        kss = gk;
                    }
                    for (size_t i = 0; i < n; i++) {
                        free(ids[i]);
                        free(kss[i]);
                    }
                    free(ids);
                    free(kss);
                    cJSON_Delete(docs);
                    return NULL;
                }
                ids = gi;
                kss = gk;
                cap = grown;
            }
            ids[n] = shard_ids[r];
            kss[n] = strdup(t->keyspaces[k]);
            if (!kss[n]) {
                free(shard_ids[r]);
                continue;
            }
            n++;
            /* move the row document over so the caller's column union
             * sees every key of every merged keyspace */
            cJSON *shard_row = cJSON_GetArrayItem(shard_docs, (int)r);
            cJSON *row_copy = cJSON_Duplicate(shard_row, 1);
            if (!row_copy) {
                free(ids[n - 1]);
                free(kss[n - 1]);
                n--;
                continue;
            }
            cJSON_AddItemToArray(docs, row_copy);
        }
        free(shard_ids);
        cJSON_Delete(shard_docs);
    }
    *ids_out = ids;
    *nids_out = n;
    *ks_out = kss;
    *nks_out = n;
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
            if (cJSON_IsObject(item) || cJSON_IsArray(item)) {
                t->kinds[t->ncols] = 1;
            }
            snprintf(t->cols[t->ncols++], sizeof(t->cols[0]), "%s", key);
        }
        /* an existing column may first appear as scalar then as object */
        if (seen && (cJSON_IsObject(item) || cJSON_IsArray(item))) {
            for (size_t i = 0; i < t->ncols; i++) {
                if (strcmp(t->cols[i], key) == 0) {
                    t->kinds[i] = 1;
                    break;
                }
            }
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
 * docs (frees it); ids/nids are borrowed. row_ks (index-aligned with ids,
 * NULL for single-shard tables) supplies the owning keyspace per row so
 * the row map can route pw write-back to the right shard. */
static int materialize(sqlite3 *db, const eql_table *t, cJSON *docs,
                       char **ids, size_t nids, const char *const *row_ks,
                       eql_rowmap *map)
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
            break;
        }
        map_add_ks(map, t, sqlite3_last_insert_rowid(db), ids[r],
                   row_ks ? row_ks[r] : t->ks);
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

#define EQL_EXEC_TIMEOUT_MS 5000

typedef struct {
    long long deadline_us;
} eql_watchdog;

static long long eql_now_us(void)
{
    struct timespec ts;
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return (long long)ts.tv_sec * 1000000LL + ts.tv_nsec / 1000;
}

/* Progress callback: returns nonzero once the statement has burned more
 * than its deadline, making sqlite3_step return SQLITE_INTERRUPT. */
static int eql_progress(void *ud)
{
    eql_watchdog *w = ud;
    return eql_now_us() > w->deadline_us ? 1 : 0;
}

/* Denies every statement class a read-only query has no business running:
 * schema mutation, transaction/savepoint control, pragmas, and database
 * attach/detach. Row DML is allowed only for the single action class this
 * execution declared and only on shard-backed aliases. */
static int deny_authorizer(void *ud, int action, const char *a1,
                           const char *a2, const char *db_name,
                           const char *trigger)
{
    (void)a2;
    (void)db_name;
    (void)trigger;
    const eql_acl *acl = ud;
    bool is_dml_act = action == SQLITE_INSERT || action == SQLITE_UPDATE ||
                      action == SQLITE_DELETE;
    if (!is_dml_act) {
        switch (action) {
        case SQLITE_ATTACH:
        case SQLITE_DETACH:
        case SQLITE_CREATE_INDEX:
        case SQLITE_CREATE_TABLE:
        case SQLITE_CREATE_TEMP_INDEX:
        case SQLITE_CREATE_TEMP_TABLE:
        case SQLITE_CREATE_TEMP_TRIGGER:
        case SQLITE_CREATE_TEMP_VIEW:
        case SQLITE_CREATE_TRIGGER:
        case SQLITE_CREATE_VIEW:
        case SQLITE_PRAGMA:
        case SQLITE_TRANSACTION:
        case SQLITE_SAVEPOINT:
        case SQLITE_REINDEX:
        case SQLITE_ANALYZE:
        case SQLITE_ALTER_TABLE:
            (void)a1;
            return SQLITE_DENY;
        default:
            return SQLITE_OK;
        }
    }
    if (acl && acl->allow_op == action && a1) {
        for (size_t i = 0; i < acl->ntables; i++) {
            if (strcmp(acl->tables[i].alias, a1) == 0) {
                return SQLITE_OK;
            }
        }
    }
    (void)a1;
    return SQLITE_DENY;
}

/* --- replay: buffered events -> replicated changes -------------------- */

#define EQL_WRITE_ATTEMPTS 3
#define EQL_RETRY_SLEEP_MS 100

/* Sends one change through the normal write paths: replication fan-out
 * when a repl service is attached, local engine otherwise. Retries
 * transient failures; LWW conditional apply keeps replays idempotent. */
static int replicate_record(const edb_eql_ctx *ctx, const eql_table *t,
                            const char *ks, const char *id, cJSON *value,
                            int *out_code)
{
    long long ts = (long long)time(NULL);
    char *text = NULL;
    if (value) {
        /* value stays caller-owned: serialize manually so repeated retries
         * reuse it without duplicating ownership into the envelope */
        char *vtxt = cJSON_PrintUnformatted(value);
        size_t cap = strlen(vtxt ? vtxt : "") + EQL_ID_CAP +
                     strlen(t->db) + strlen(t->part) + strlen(ks) + 128;
        text = malloc(cap);
        if (!text) {
            free(vtxt);
            *out_code = 500;
            return -1;
        }
        snprintf(text, cap,
                 "{\"op\":\"put\",\"db\":\"%s\",\"partition\":\"%s\",\"keyspace\":\"%s\",\"id\":\"%s\",\"value\":%s,\"ttl_abs\":-1,\"ts\":%lld}",
                 t->db, t->part, ks, id, vtxt ? vtxt : "", ts);
        free(vtxt);
    } else {
        cJSON *change = cJSON_CreateObject();
        if (!change) {
            *out_code = 500;
            return -1;
        }
        cJSON_AddStringToObject(change, "op", "delete");
        cJSON_AddStringToObject(change, "db", t->db);
        cJSON_AddStringToObject(change, "partition", t->part);
        cJSON_AddStringToObject(change, "keyspace", ks);
        cJSON_AddStringToObject(change, "id", id);
        char tsbuf[32];
        snprintf(tsbuf, sizeof(tsbuf), "%lld", ts);
        cJSON_AddRawToObject(change, "ts", tsbuf);
        text = cJSON_PrintUnformatted(change);
        cJSON_Delete(change);
        if (!text) {
            *out_code = 500;
            return -1;
        }
    }
    if (!text) {
        *out_code = 500;
        return -1;
    }

    /* stage 9: classify the write before applying it (after_* scripts
     * distinguish inserts from updates; the delete handler receives the
     * record that was removed) */
    cJSON *existing = ctx->engine ? edb_get(ctx->engine, t->part, ks, id)
                                  : NULL;

    int code = 0;
    for (int attempt = 0; attempt < EQL_WRITE_ATTEMPTS; attempt++) {
        if (ctx->repl) {
            edb_repl_status st = edb_repl_write(ctx->repl, t->db, text);
            if (st == EDB_REPL_OK) {
                code = 200;
                break;
            }
            code = st == EDB_REPL_QUORUM_LOST ? 503 : 500;
        } else if (value) {
            char *body = cJSON_PrintUnformatted(value);

            bool ok = body &&
                      edb_put(ctx->engine, t->part, ks, id, body, -1);

            free(body);
            code = ok ? 200 : 500;
            if (ok) {
                break;
            }
        } else {
            code = edb_delete(ctx->engine, t->part, ks, id) ? 200 : 500;
            if (code == 200) {
                break;
            }
        }
        struct timespec pause = { 0, EQL_RETRY_SLEEP_MS * 1000000L };
        nanosleep(&pause, NULL);
    }
    free(text);
    *out_code = code;
    if (*out_code == 200 && ctx->config && value) {
        /* keep the config registry in sync like REST puts do so new
         * databases/partitions/keyspaces created by EQL statements appear
         * in admin listings; replication fans the records to peers */
        edb_partition_ensure(ctx->config, t->db, t->part, ks, NULL);
    }
    if (code == 200 && strcmp(t->db, EDB_SYSTEM_DB) != 0 && !ctx->repl) {
        /* stage 9: EQL writes fire after_* scripts (trusted: scripts
         * authored by admins run with full rights); best-effort only.
         * With replication attached the local apply already fired the
         * event once per node, so only the single-node path fires. */
        edb_lua_ctx lctx = { ctx->engine, ctx->config, ctx->repl, NULL };
        edb_lua_event_arg larg = { t->db, t->part, ks, id, NULL, 0, true };
        char *veto = NULL;
        edb_lua_event luev;
        if (value) {
            luev = existing ? EDB_LUA_AFTER_UPDATE : EDB_LUA_AFTER_INSERT;
        } else {
            luev = EDB_LUA_AFTER_DELETE;
            larg.value = &existing;
        }
        (void)edb_lua_fire(&lctx, luev, &larg, &veto);
        free(veto);
    }
    cJSON_Delete(existing);
    return 0;
}

/* Rebuilds the stored document for one row from its shadow-table columns.
 * Object/array columns were bound as JSON text during materialization and
 * are parsed back so write-back preserves nesting. The record key itself
 * travels in the change envelope, not inside the document body. */
static cJSON *row_to_doc(sqlite3_stmt *rd, const eql_table *t)
{
    cJSON *doc = cJSON_CreateObject();
    if (!doc) {
        return NULL;
    }
    int ncols = sqlite3_column_count(rd);
    for (int c = 1; c < ncols && (size_t)c < t->ncols; c++) {
        cJSON *val = NULL;
        switch (sqlite3_column_type(rd, c)) {
        case SQLITE_INTEGER:
            val = cJSON_CreateNumber((double)sqlite3_column_int64(rd, c));
            break;
        case SQLITE_FLOAT:
            val = cJSON_CreateNumber(sqlite3_column_double(rd, c));
            break;
        case SQLITE_TEXT: {
            const unsigned char *txt = sqlite3_column_text(rd, c);
            const char *str = txt ? (const char *)txt : "";
            if (t->kinds[c]) {
                val = cJSON_Parse(str);
            }
            if (!val) {
                val = cJSON_CreateString(str);
            }
            break;
        }
        case SQLITE_BLOB: {
            const unsigned char *blob =
                (const unsigned char *)sqlite3_column_blob(rd, c);
            size_t blen = (size_t)sqlite3_column_bytes(rd, c);
            char *hex = malloc(blen * 2 + 1);
            if (hex) {
                static const char digits[] = "0123456789abcdef";
                for (size_t b = 0; b < blen; b++) {
                    hex[b * 2] = digits[blob[b] >> 4];
                    hex[b * 2 + 1] = digits[blob[b] & 0xF];
                }
                hex[blen * 2] = '\0';
                val = cJSON_CreateString(hex);
                free(hex);
            }
            break;
        }
        default:
            val = cJSON_CreateNull();
            break;
        }
        if (!val) {
            val = cJSON_CreateNull();
        }
        cJSON_AddItemToObject(doc, t->cols[c], val);
    }
    return doc;
}

typedef struct {
    const eql_table *t;
    sqlite3_stmt *rd;
} eql_reader;

static sqlite3_stmt *reader_for(eql_reader *readers, size_t *nr,
                                const eql_table *t, sqlite3 *db)
{
    for (size_t i = 0; i < *nr; i++) {
        if (readers[i].t == t) {
            return readers[i].rd;
        }
    }
    size_t cap = 128;
    for (size_t i = 0; i < t->ncols; i++) {
        cap += strlen(t->cols[i]) + 8;
    }
    char *sql = malloc(cap);
    if (!sql) {
        return NULL;
    }
    snprintf(sql, cap, "SELECT ");
    for (size_t i = 0; i < t->ncols; i++) {
        snprintf(sql + strlen(sql), cap - strlen(sql), "%s\"%s\"",
                 i ? "," : "", t->cols[i]);
    }
    snprintf(sql + strlen(sql), cap - strlen(sql),
             " FROM \"%s\" WHERE rowid=?", t->alias);
    sqlite3_stmt *rd = NULL;
    if (sqlite3_prepare_v2(db, sql, -1, &rd, NULL) != SQLITE_OK) {
        fprintf(stderr, "eql: prepare reader failed: %s\n",
                sqlite3_errmsg(db));
        free(sql);
        return NULL;
    }
    free(sql);
    readers[*nr].t = t;
    readers[*nr].rd = rd;
    (*nr)++;
    return rd;
}

/* Applies buffered events best-effort once the statement has committed:
 * each record is retried independently and per-id outcomes are reported
 * to the caller rather than aborting the batch. */
static void process_events(const edb_eql_ctx *ctx, eql_trapbuf *buf,
                           eql_rowmap *map, sqlite3 *db,
                           cJSON **applied_out, cJSON **failed_out)
{
    cJSON *applied = cJSON_CreateArray();
    cJSON *failed = cJSON_CreateArray();
    eql_reader *readers = calloc(buf->n + 1, sizeof(*readers));
    size_t nr = 0;
    if (!applied || !failed || !readers) {
        cJSON_Delete(applied);
        cJSON_Delete(failed);
        free(readers);
        *applied_out = applied ? applied : cJSON_CreateArray();
        *failed_out = failed ? failed : cJSON_CreateArray();
        return;
    }

    for (size_t e = 0; e < buf->n && !buf->overflow; e++) {
        const eql_dml_ev *ev = &buf->ev[e];
        if (ev->op == SQLITE_DELETE) {
            char id[EQL_ID_CAP];
            char ks[128];
            if (!map_take(map, ev->t, ev->rowid, id, ks)) {
                cJSON_AddItemToArray(failed, cJSON_CreateString("-"));
                continue;
            }
            int code = 0;
            replicate_record(ctx, ev->t, ks, id, NULL, &code);
            cJSON_AddItemToArray(code == 200 ? applied : failed,
                                 cJSON_CreateString(id));
            continue;
        }

        /* INSERT / UPDATE: re-read the committed row content */
        sqlite3_stmt *rd = reader_for(readers, &nr, ev->t, db);
        if (!rd) {
            cJSON_AddItemToArray(failed, cJSON_CreateString("-"));
            continue;
        }
        sqlite3_reset(rd);
        sqlite3_bind_int64(rd, 1, ev->rowid);
        if (sqlite3_step(rd) != SQLITE_ROW) {
            sqlite3_reset(rd);
            char stale[EQL_ID_CAP];
            char stale_ks[128];
            bool known = map_take(map, ev->t, ev->rowid, stale, stale_ks);
            cJSON_AddItemToArray(known ? applied : failed,
                                 cJSON_CreateString(known ? stale : "-"));
            continue;
        }
        char cur_id[EQL_ID_CAP];
        const unsigned char *idtxt = sqlite3_column_text(rd, 0);
        snprintf(cur_id, EQL_ID_CAP, "%s", idtxt ? (const char *)idtxt : "");
        char old_id[EQL_ID_CAP];
        char old_ks[128];
        bool had_old = map_take(map, ev->t, ev->rowid, old_id, old_ks);
        map_add_ks(map, ev->t, ev->rowid, cur_id, old_ks);

        /* UPDATE renaming the primary key = delete old key + put new one */
        if (had_old && strcmp(old_id, cur_id) != 0) {
            int code = 0;
            replicate_record(ctx, ev->t, old_ks, old_id, NULL, &code);
            cJSON_AddItemToArray(code == 200 ? applied : failed,
                                 cJSON_CreateString(old_id));
        }

        cJSON *doc = row_to_doc(rd, ev->t);
        sqlite3_reset(rd);
        if (!doc) {
            cJSON_AddItemToArray(failed, cJSON_CreateString(cur_id));
            continue;
        }
        int code = 0;
        replicate_record(ctx, ev->t,
                         had_old ? old_ks : (ev->t->pw ? "" : ev->t->ks),
                         cur_id, doc, &code);
        cJSON_AddItemToArray(code == 200 ? applied : failed,
                             cJSON_CreateString(cur_id));
    }

    for (size_t i = 0; i < nr; i++) {
        sqlite3_finalize(readers[i].rd);
    }
    free(readers);
    *applied_out = applied;
    *failed_out = failed;
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

    eql_kind kind = classify_statement(sql);

    int status = 200;
    eql_table *tables = calloc(EQL_MAX_TABLES, sizeof(*tables));
    if (!tables) {
        return 500;
    }
    int ntables = collect_refs(sql, tables, 0);
    if (ntables <= 0) {
        *json_out = error_response(
            "no Database.Partition.Keyspace reference found in FROM/JOIN");
        free(tables);
        return 400;
    }

    /* INSERT has no per-row keyspace to attribute: the shadow table merges
     * every keyspace of the partition, so there is nowhere to put the
     * new record. Require an explicit keyspace like the REST API. */
    if (kind == EQL_KIND_INSERT) {
        for (int i = 0; i < ntables; i++) {
            if (tables[i].pw) {
                *json_out = error_response(
                    "INSERT requires an explicit Database.Partition."
                    "Keyspace reference");
                free(tables);
                return 400;
            }
        }
    }

    edb_permission need = EDB_PERM_READ;
    if (kind == EQL_KIND_DELETE) {
        need = EDB_PERM_DELETE;
    } else if (kind == EQL_KIND_UPDATE) {
        need = EDB_PERM_UPDATE;
    } else if (kind == EQL_KIND_INSERT) {
        need = EDB_PERM_CREATE;
    }

    for (int i = 0; i < ntables && status == 200; i++) {
        if (perm_denied(ctx, &tables[i], user_groups, trusted, need)) {
            status = 403;
        }
    }
    if (status != 200) {
        *json_out = error_response("permission denied");
        return status;
    }

    bool is_dml = kind == EQL_KIND_DELETE || kind == EQL_KIND_UPDATE ||
                  kind == EQL_KIND_INSERT;

    eql_rowmap rowmap;
    memset(&rowmap, 0, sizeof(rowmap));

    for (int i = 0; i < ntables; i++) {
        snprintf(tables[i].cols[tables[i].ncols++],
                 sizeof(tables[i].cols[0]), "id");
    }

    if (kind == EQL_KIND_INSERT) {
        scan_insert_columns(sql, tables, (size_t)ntables);
    } else if (kind == EQL_KIND_UPDATE) {
        scan_update_columns(sql, tables, (size_t)ntables);
    }

    char *rewritten = rewrite_sql(sql, tables, (size_t)ntables);
    if (!rewritten) {
        free(tables);
        return 500;
    }

    sqlite3 *db = NULL;
    sqlite3_stmt *stmt = NULL;
    if (sqlite3_open(":memory:", &db) != SQLITE_OK) {
        free(rewritten);
        free(tables);
        return 500;
    }
    harden_connection(db);

    eql_watchdog watchdog = { eql_now_us() + EQL_EXEC_TIMEOUT_MS * 1000LL };
    sqlite3_progress_handler(db, 20000, eql_progress, &watchdog);

    for (int i = 0; i < ntables && status == 200; i++) {
        size_t nids = 0;
        size_t nks = 0;
        char **ids = NULL;
        char **row_ks = NULL;
        cJSON *docs = fetch_docs(ctx, &tables[i], &ids, &nids, &row_ks,
                                 &nks);
        if (!docs) {
            status = 500;
            break;
        }
        const cJSON *doc = NULL;
        cJSON_ArrayForEach(doc, docs) {
            union_columns(&tables[i], doc);
        }
        status = materialize(db, &tables[i], docs, ids, nids,
                             (const char *const *)row_ks, &rowmap);
        if (status == 0) {
            status = 200;
        }
        for (size_t k = 0; k < nids; k++) {
            free(ids[k]);
        }
        free(ids);
        if (row_ks) {
            for (size_t k = 0; k < nks; k++) {
                free(row_ks[k]);
            }
            free(row_ks);
        }
    }
    if (status != 200) {
        map_free(&rowmap);
        free(tables);
        free(rewritten);
        sqlite3_close(db);
        return status > 0 ? status : 500;
    }

    eql_acl acl = { tables, (size_t)ntables, 0 };
    if (is_dml && kind == EQL_KIND_DELETE) {
        acl.allow_op = SQLITE_DELETE;
    } else if (is_dml && kind == EQL_KIND_UPDATE) {
        acl.allow_op = SQLITE_UPDATE;
    } else if (is_dml && kind == EQL_KIND_INSERT) {
        acl.allow_op = SQLITE_INSERT;
    }
    sqlite3_set_authorizer(db, deny_authorizer, &acl);

    const char *tail = NULL;
    if (sqlite3_prepare_v2(db, rewritten, -1, &stmt, &tail) != SQLITE_OK) {
        const char *msg = sqlite3_errmsg(db);
        if (!is_dml && sqlite3_extended_errcode(db) == SQLITE_AUTH) {
            *json_out =
                error_response("only SELECT statements are supported");
        } else {
            *json_out = error_response(msg ? msg : "prepare failed");
        }
        map_free(&rowmap);
        free(tables);
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
        map_free(&rowmap);
        free(tables);
        free(rewritten);
        sqlite3_finalize(stmt);
        sqlite3_close(db);
        return 400;
    }
    free(rewritten);
    rewritten = NULL;

    /* --- SELECT path ------------------------------------------------- */
    if (!is_dml) {
        if (!stmt || !sqlite3_stmt_readonly(stmt)) {
            *json_out =
                error_response("only SELECT statements are supported");
            if (stmt) {
                sqlite3_finalize(stmt);
            }
            map_free(&rowmap);
            free(tables);
            sqlite3_close(db);
            return 400;
        }
        int ncols = sqlite3_column_count(stmt);
        cJSON *result = cJSON_CreateObject();
        cJSON *cols_json =
            result ? cJSON_AddArrayToObject(result, "columns") : NULL;
        cJSON *rows_json =
            result ? cJSON_AddArrayToObject(result, "rows") : NULL;
        if (!result || !cols_json || !rows_json) {
            cJSON_Delete(result);
            sqlite3_finalize(stmt);
            map_free(&rowmap);
            free(tables);
            sqlite3_close(db);
            return 500;
        }
        cJSON_AddStringToObject(result, "status", "ok");
        for (int c = 0; c < ncols; c++) {
            const char *name = sqlite3_column_name(stmt, c);
            cJSON_AddItemToArray(cols_json,
                                 cJSON_CreateString(name ? name : ""));
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
                    val = cJSON_CreateNumber(
                        sqlite3_column_double(stmt, c));
                    break;
                case SQLITE_TEXT:
                    val = cJSON_CreateString(
                        (const char *)sqlite3_column_text(stmt, c));
                    break;
                case SQLITE_BLOB: {
                    const unsigned char *blob =
                        (const unsigned char *)
                            sqlite3_column_blob(stmt, c);
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
        int sel_code;
        if (step == SQLITE_INTERRUPT) {
            cJSON_Delete(result);
            *json_out = error_response(
                "statement exceeded the execution time limit");
            sel_code = 400;
        } else if (step != SQLITE_DONE) {
            const char *msg = sqlite3_errmsg(db);
            cJSON_Delete(result);
            *json_out =
                error_response(msg ? msg : "statement execution failed");
            sel_code = 400;
        } else {
            *json_out = cJSON_PrintUnformatted(result);
            cJSON_Delete(result);
            if (!*json_out) {
                *json_out = error_response("encode failed");
                sel_code = 500;
            } else {
                sel_code = 200;
            }
        }
        sqlite3_finalize(stmt);
        map_free(&rowmap);
        free(tables);
        sqlite3_close(db);
        return sel_code;
    }

    /* --- DML path under row hooks ------------------------------------ */
    eql_trapbuf buf;
    memset(&buf, 0, sizeof(buf));
    eql_trap_bundle bundle = { tables, (size_t)ntables, &buf };
    sqlite3_update_hook(db, trap_row_update, &bundle);

    int dstep = sqlite3_step(stmt);
    sqlite3_update_hook(db, NULL, NULL);
    sqlite3_progress_handler(db, 0, NULL, NULL);

    bool committed = dstep == SQLITE_DONE && !buf.overflow;
    bool interrupted = dstep == SQLITE_INTERRUPT;
    sqlite3_finalize(stmt);
    stmt = NULL;

    if (!committed) {
        const char *msg = interrupted
                              ? "statement exceeded the execution time limit"
                              : (buf.overflow
                                     ? "too many changes for one statement"
                                     : sqlite3_errmsg(db));
        *json_out = error_response(msg ? msg : "statement failed");
        free(buf.ev);
        map_free(&rowmap);
        free(tables);
        sqlite3_close(db);
        return 400;
    }

    cJSON *applied = NULL;
    cJSON *failed = NULL;
    process_events(ctx, &buf, &rowmap, db, &applied, &failed);
    free(buf.ev);
    map_free(&rowmap);
    free(tables);
    sqlite3_close(db);

    size_t napplied = applied ? (size_t)cJSON_GetArraySize(applied) : 0;
    size_t nfailed = failed ? (size_t)cJSON_GetArraySize(failed) : 0;
    size_t total = napplied + nfailed;

    int rc_code;
    cJSON *result = cJSON_CreateObject();
    if (!result) {
        cJSON_Delete(applied);
        cJSON_Delete(failed);
        return 500;
    }
    const char *opname = kind == EQL_KIND_DELETE
                             ? "delete"
                             : (kind == EQL_KIND_UPDATE ? "update"
                                                        : "insert");
    char cnt[32];
    snprintf(cnt, sizeof(cnt), "%zu", total);
    if (napplied > 0 || total == 0) {
        cJSON_AddStringToObject(result, "status", "ok");
        cJSON_AddStringToObject(result, "op", opname);
        cJSON_AddRawToObject(result, "count", cnt);
        cJSON_AddItemToObject(result, "applied",
                              applied ? applied : cJSON_CreateArray());
        cJSON_AddItemToObject(result, "failed",
                              failed ? failed : cJSON_CreateArray());
        rc_code = 200;
    } else {
        cJSON_AddStringToObject(result, "status", "error");
        cJSON_AddStringToObject(
            result, "message",
            "no records were replicated for this statement");
        cJSON_AddRawToObject(result, "count", cnt);
        cJSON_AddItemToObject(result, "applied",
                              applied ? applied : cJSON_CreateArray());
        cJSON_AddItemToObject(result, "failed",
                              failed ? failed : cJSON_CreateArray());
        rc_code = 503;
    }
    *json_out = cJSON_PrintUnformatted(result);
    cJSON_Delete(result);
    if (!*json_out) {
        *json_out = error_response("encode failed");
        return 500;
    }
    return rc_code;
}
