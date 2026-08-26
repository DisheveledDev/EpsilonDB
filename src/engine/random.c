/* Cryptographically-random bytes for salts and session tokens. Prefers
 * /dev/urandom and falls back to a time/pid/address mix on systems where it
 * is unavailable. */

#include "random.h"

#include <fcntl.h>
#include <stdio.h>
#include <string.h>
#include <time.h>
#include <unistd.h>

bool edb_random_bytes(uint8_t *out, size_t len)
{
    int fd = open("/dev/urandom", O_RDONLY);
    if (fd >= 0) {
        size_t got = 0;
        while (got < len) {
            ssize_t n = read(fd, out + got, len - got);
            if (n <= 0) {
                break;
            }
            got += (size_t)n;
        }
        close(fd);
        if (got == len) {
            return true;
        }
    }

    /* Fallback: a weak but non-deterministic mix. */
    uint64_t seed = (uint64_t)time(NULL) ^ ((uint64_t)getpid() << 32) ^
                    (uint64_t)(uintptr_t)out;
    for (size_t i = 0; i < len; i++) {
        seed = seed * 6364136223846793005ULL + 1442695040888963407ULL;
        out[i] = (uint8_t)(seed >> 33);
    }
    return true;
}

void edb_random_hex(char *out, size_t hex_chars)
{
    static const char hex[] = "0123456789abcdef";
    uint8_t bytes[128];
    size_t count = (hex_chars + 1) / 2;
    if (count > sizeof(bytes)) {
        count = sizeof(bytes);
    }
    edb_random_bytes(bytes, count);
    for (size_t i = 0; i < count && i * 2 < hex_chars; i++) {
        out[i * 2] = hex[bytes[i] >> 4];
        if (i * 2 + 1 < hex_chars) {
            out[i * 2 + 1] = hex[bytes[i] & 0xf];
        }
    }
    out[hex_chars] = '\0';
}
