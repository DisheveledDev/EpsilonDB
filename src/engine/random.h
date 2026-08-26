#ifndef EDB_RANDOM_H
#define EDB_RANDOM_H

#include <stdbool.h>
#include <stddef.h>
#include <stdint.h>

/* Fills `out` with `len` cryptographically random bytes (from /dev/urandom
 * when available, with a time/pid fallback). Returns true on success. */
bool edb_random_bytes(uint8_t *out, size_t len);

/* Writes `hex_chars` (an even number) lowercase hex digits of random bytes
 * plus a NUL terminator into `out`, which must hold hex_chars + 1 bytes. */
void edb_random_hex(char *out, size_t hex_chars);

#endif
