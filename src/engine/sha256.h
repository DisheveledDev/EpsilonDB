#ifndef EDB_SHA256_H
#define EDB_SHA256_H

#include <stddef.h>
#include <stdint.h>

/* Computes the SHA-256 digest of `data` into `out` (32 bytes). */
void edb_sha256(const void *data, size_t len, uint8_t out[32]);

/* Computes the SHA-256 digest and writes it as a 64-char lowercase hex
 * string into `out` (which must hold at least 65 bytes, NUL terminated).
 * Returns `out`. */
char *edb_sha256_hex(const void *data, size_t len, char out[65]);

#endif
