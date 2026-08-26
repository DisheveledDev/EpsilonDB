/* epsilon_benchmark.h - server-side workload benchmark.
 *
 * Runs a self-contained performance test against the local engine: create a
 * throwaway database + partitions, time bulk writes / gets / filtered queries
 * / updates / deletes, then delete the database and its shard files. Used by
 * the CLI and admin console to report throughput (ops/sec).
 */

#ifndef EPSILON_BENCHMARK_H
#define EPSILON_BENCHMARK_H

#include "epsilon_config.h"

/* Runs the benchmark and returns a report object (caller frees with
 * cJSON_Delete). The throwaway database, its partitions and their shard
 * files are deleted before returning. threads: number of worker threads
 * used per phase; partitions are spread across the workers (each partition
 * is owned by exactly one worker) so shards are exercised concurrently.
 * Pass 0 to use one thread per partition. */
cJSON *edb_benchmark_run(edb_config *cfg, int replication_factor,
                         long long cache_size, const char *journal_mode,
                         int partitions, int records_per_partition,
                         int threads);

#endif
