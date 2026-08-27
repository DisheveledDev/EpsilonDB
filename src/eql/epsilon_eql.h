/* epsilon_eql.h - Epsilon Query Language (EQL) execution engine.
 *
 * Executes one SQL-like statement against shard materialization in a
 * private in-memory SQLite database. The statement's FROM references
 * ("Database.Partition.Keyspace") are resolved to real shards, their live
 * documents (record key injected as the leading "id" field, same shape as
 * REST reads) are materialized into shadow tables, and the original SQL is
 * rewritten to reference those tables. SELECT results come back as JSON;
 * DML support is added by later stages through row-level write traps.
 *
 * Each call gets a fresh :memory: database, so there is no session state:
 * every statement sees live data, and a failed statement can never leave
 * residue behind.
 */

#ifndef EPSILON_EQL_H
#define EPSILON_EQL_H

#include <stdbool.h>
#include <stdint.h>

#include "../engine/epsilon_engine.h"
#include "../engine/epsilon_config.h"
#include "../socket/epsilon_repl.h"

typedef struct {
    edb_engine *engine;   /* required */
    edb_config *config;   /* required: partition permission masks */
    edb_repl *repl;       /* may be NULL: single-node reads */
} edb_eql_ctx;

/* HTTP-style status codes (200/400/403/500) matching API conventions. */
int edb_eql_execute(const edb_eql_ctx *ctx, const char *sql,
                    uint64_t user_groups, bool trusted, char **json_out);

#endif /* EPSILON_EQL_H */
