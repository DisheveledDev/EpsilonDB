/* epsilon_crypto.h - authenticated encryption and key derivation for the
 * node-to-node mesh. Self-contained (no external dependencies): ChaCha20
 * stream cipher (RFC 8439), HMAC-SHA256 (RFC 2104), HKDF-SHA256 (RFC 5869),
 * composed as Encrypt-then-MAC. SHA-256 reuses the vendored edb_sha256.
 *
 * The AEAD is ChaCha20 (IETF 96-bit nonce) for confidentiality plus
 * HMAC-SHA256 for integrity/authentication. Encrypt-then-MAC with
 * independent keys is the standard, provably-safe composition; it is used
 * here in place of Poly1305 so the mesh crypto needs only the primitives
 * already present in-tree.
 */

#ifndef EPSILON_CRYPTO_H
#define EPSILON_CRYPTO_H

#include <stddef.h>
#include <stdint.h>

#define EDB_NONCE_LEN 12
#define EDB_TAG_LEN   32

/* Runs the ChaCha20 keystream over `in`/`out` (length `len`) with the given
 * 32-byte key, 12-byte nonce and 32-bit block counter. Encryption and
 * decryption are the same operation. */
void edb_chacha20_xor(uint8_t *out, const uint8_t *in, size_t len,
                      const uint8_t key[32], const uint8_t nonce[12],
                      uint32_t counter);

/* HMAC-SHA256 (RFC 2104). */
void edb_hmac_sha256(const uint8_t *key, size_t key_len,
                     const uint8_t *msg, size_t msg_len, uint8_t out[32]);

/* HKDF-SHA256 (RFC 5869): derive okm_len bytes. Returns 0 on success. */
int edb_hkdf_sha256(const uint8_t *ikm, size_t ikm_len,
                    const uint8_t *salt, size_t salt_len,
                    const uint8_t *info, size_t info_len,
                    uint8_t *okm, size_t okm_len);

/* Encrypt-then-MAC AEAD. Encrypts `plain` under enc_key with a fresh
 * nonce, then authenticates (aad || nonce || ciphertext) with mac_key.
 * Writes nonce || ciphertext || tag into `out` (must hold
 * plain_len + EDB_NONCE_LEN + EDB_TAG_LEN bytes). Returns 0 on success. */
int edb_aead_seal(const uint8_t enc_key[32], const uint8_t mac_key[32],
                  const uint8_t nonce[12], const uint8_t *aad, size_t aad_len,
                  const uint8_t *plain, size_t plain_len,
                  uint8_t *out, size_t out_cap);

/* Verifies and decrypts a edb_aead_seal output. `in` is
 * nonce || ciphertext || tag (length in_len). Returns the plaintext length
 * on success, or -1 on authentication failure. */
int edb_aead_open(const uint8_t enc_key[32], const uint8_t mac_key[32],
                  const uint8_t *aad, size_t aad_len,
                  const uint8_t *in, size_t in_len,
                  uint8_t *out, size_t out_cap);

/* Runs RFC 8439 / RFC 4231 / RFC 5869 test vectors. Returns 0 if all pass,
 * non-zero otherwise. */
int edb_crypto_selftest(void);

#endif
