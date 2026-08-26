/* epsilon_procstat.c - per-process resource sampling.
 *
 * CPU time comes from getrusage(RUSAGE_SELF), which is POSIX and behaves
 * the same on Linux and macOS.
 *
 * Resident memory has no portable interface:
 *   - Linux  /proc/self/statm field 2 (resident pages) * page size.
 *   - macOS  task_info(MACH_TASK_BASIC_INFO).resident_size.
 * getrusage's ru_maxrss is deliberately not used as the primary source:
 * it reports the *peak*, not the current, and its unit differs between
 * the two platforms (kilobytes on Linux, bytes on macOS).
 */

#include "epsilon_procstat.h"

#include <stdio.h>
#include <string.h>
#include <sys/resource.h>
#include <sys/time.h>
#include <time.h>
#include <unistd.h>

#if defined(__APPLE__)
#include <mach/mach.h>
#endif

static uint64_t monotonic_us(void)
{
    struct timespec ts;
#if defined(CLOCK_MONOTONIC)
    if (clock_gettime(CLOCK_MONOTONIC, &ts) == 0) {
        return (uint64_t)ts.tv_sec * 1000000ULL +
               (uint64_t)ts.tv_nsec / 1000ULL;
    }
#endif
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return (uint64_t)tv.tv_sec * 1000000ULL + (uint64_t)tv.tv_usec;
}

static uint64_t resident_bytes(void)
{
#if defined(__linux__)
    FILE *fp = fopen("/proc/self/statm", "r");
    if (!fp) {
        return 0;
    }
    unsigned long long total = 0, resident = 0;
    int got = fscanf(fp, "%llu %llu", &total, &resident);
    fclose(fp);
    if (got != 2) {
        return 0;
    }
    long page = sysconf(_SC_PAGESIZE);
    if (page <= 0) {
        page = 4096;
    }
    return (uint64_t)resident * (uint64_t)page;
#elif defined(__APPLE__)
    mach_task_basic_info_data_t info;
    mach_msg_type_number_t count = MACH_TASK_BASIC_INFO_COUNT;
    if (task_info(mach_task_self(), MACH_TASK_BASIC_INFO,
                  (task_info_t)&info, &count) == KERN_SUCCESS) {
        return (uint64_t)info.resident_size;
    }
    return 0;
#else
    return 0;
#endif
}

bool edb_proc_sample_now(edb_proc_sample *out)
{
    if (!out) {
        return false;
    }
    memset(out, 0, sizeof(*out));
    out->wall_us = monotonic_us();
    out->rss_bytes = resident_bytes();

    struct rusage ru;
    if (getrusage(RUSAGE_SELF, &ru) == 0) {
        out->cpu_us = (uint64_t)ru.ru_utime.tv_sec * 1000000ULL +
                      (uint64_t)ru.ru_utime.tv_usec +
                      (uint64_t)ru.ru_stime.tv_sec * 1000000ULL +
                      (uint64_t)ru.ru_stime.tv_usec;
    }
    return true;
}

double edb_proc_cpu_percent(const edb_proc_sample *prev,
                            const edb_proc_sample *now)
{
    if (!prev || !now || now->wall_us <= prev->wall_us ||
        now->cpu_us < prev->cpu_us) {
        return 0.0;
    }
    uint64_t cpu_delta = now->cpu_us - prev->cpu_us;
    uint64_t wall_delta = now->wall_us - prev->wall_us;
    if (wall_delta == 0) {
        return 0.0;
    }
    double pct = ((double)cpu_delta / (double)wall_delta) * 100.0;
    return pct < 0.0 ? 0.0 : pct;
}
