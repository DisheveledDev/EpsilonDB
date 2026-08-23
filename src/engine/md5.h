#ifndef ZDB_MD5_H
#define ZDB_MD5_H

#include <stddef.h>
#include <stdint.h>

/* Computes the MD5 digest of data and writes it as a 32-char lowercase hex
 * string into out (which must hold at least 33 bytes, NUL terminated).
 * Returns out. */
char *zdb_md5_hex(const void *data, size_t len, char out[33]);

#endif
