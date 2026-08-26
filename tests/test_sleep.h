/* test_sleep.h - portable sub-second sleep for the test suites.
 *
 * usleep() was removed from POSIX.1-2008, so it is not declared when the
 * build defines _POSIX_C_SOURCE=200809L (as the Makefile does). GCC only
 * warns about the resulting implicit declaration, but Clang 16 and later
 * reject it outright, which broke `make test` on macOS. nanosleep() is the
 * POSIX-2008 replacement and is available on both macOS and Linux.
 */
#ifndef EPSILON_TEST_SLEEP_H
#define EPSILON_TEST_SLEEP_H

#include <errno.h>
#include <time.h>

static inline void edb_sleep_us(long long usec)
{
    if (usec <= 0) {
        return;
    }
    struct timespec ts;
    ts.tv_sec = (time_t)(usec / 1000000LL);
    ts.tv_nsec = (long)((usec % 1000000LL) * 1000LL);
    while (nanosleep(&ts, &ts) == -1 && errno == EINTR) {
        /* interrupted: ts holds the remaining time, so retry */
    }
}

#endif /* EPSILON_TEST_SLEEP_H */
