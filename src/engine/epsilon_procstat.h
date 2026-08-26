/* epsilon_procstat.h - per-process resource sampling for node analytics.
 *
 * Reports this process's resident memory and cumulative CPU time. CPU
 * percentage is intentionally not computed here: a single reading of
 * cumulative CPU time is meaningless on its own, so callers keep the
 * previous sample and divide the CPU delta by the wall-clock delta.
 *
 * All platform-specific code lives in epsilon_procstat.c behind this one
 * call, so supporting another OS means adding a single branch there.
 */
#ifndef EPSILON_PROCSTAT_H
#define EPSILON_PROCSTAT_H

#include <stdbool.h>
#include <stdint.h>

typedef struct {
    uint64_t rss_bytes;   /* resident set size, 0 if unavailable */
    uint64_t cpu_us;      /* cumulative user+system CPU time, microseconds */
    uint64_t wall_us;     /* monotonic wall clock at sample time */
} edb_proc_sample;

/* Fills `out`. Returns false only if nothing could be sampled at all;
 * individual fields are left at 0 when that particular value is not
 * available on the platform. */
bool edb_proc_sample_now(edb_proc_sample *out);

/* CPU use as a percentage of one core between two samples, clamped to
 * >= 0. Returns 0 when the interval is empty. Values above 100 are
 * possible and meaningful on multi-core machines. */
double edb_proc_cpu_percent(const edb_proc_sample *prev,
                            const edb_proc_sample *now);

#endif /* EPSILON_PROCSTAT_H */
