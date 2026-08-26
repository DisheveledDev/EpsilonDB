/* epsilon_crypto.c - see epsilon_crypto.h. */

#include "epsilon_crypto.h"

#include <stdlib.h>
#include <string.h>

#include "sha256.h"

/* ------------------------------------------------------------------ */
/* little-endian helpers                                               */

static uint32_t load32_le(const uint8_t *p)
{
    return (uint32_t)p[0] | ((uint32_t)p[1] << 8) | ((uint32_t)p[2] << 16) |
           ((uint32_t)p[3] << 24);
}

static void store32_le(uint8_t *p, uint32_t v)
{
    p[0] = (uint8_t)v;
    p[1] = (uint8_t)(v >> 8);
    p[2] = (uint8_t)(v >> 16);
    p[3] = (uint8_t)(v >> 24);
}

/* ------------------------------------------------------------------ */
/* ChaCha20 (RFC 8439)                                                 */

#define ROTL32(v, n) (((v) << (n)) | ((v) >> (32 - (n))))

#define QR(a, b, c, d)                                                      \
    do {                                                                    \
        (a) += (b); (d) ^= (a); (d) = ROTL32((d), 16);                     \
        (c) += (d); (b) ^= (c); (b) = ROTL32((b), 12);                     \
        (a) += (b); (d) ^= (a); (d) = ROTL32((d), 8);                      \
        (c) += (d); (b) ^= (c); (b) = ROTL32((b), 7);                      \
    } while (0)

static void chacha20_block(uint32_t out[16], const uint32_t in[16])
{
    uint32_t x[16];
    int i;
    for (i = 0; i < 16; i++) {
        x[i] = in[i];
    }
    for (i = 0; i < 10; i++) {
        QR(x[0], x[4], x[8], x[12]);
        QR(x[1], x[5], x[9], x[13]);
        QR(x[2], x[6], x[10], x[14]);
        QR(x[3], x[7], x[11], x[15]);
        QR(x[0], x[5], x[10], x[15]);
        QR(x[1], x[6], x[11], x[12]);
        QR(x[2], x[7], x[8], x[13]);
        QR(x[3], x[4], x[9], x[14]);
    }
    for (i = 0; i < 16; i++) {
        out[i] = x[i] + in[i];
    }
}

static void chacha20_init(uint32_t state[16], const uint8_t key[32],
                          uint32_t counter, const uint8_t nonce[12])
{
    int i;
    state[0] = 0x61707865;
    state[1] = 0x3320646e;
    state[2] = 0x79622d32;
    state[3] = 0x6b206574;
    for (i = 0; i < 8; i++) {
        state[4 + i] = load32_le(key + 4 * i);
    }
    state[12] = counter;
    for (i = 0; i < 3; i++) {
        state[13 + i] = load32_le(nonce + 4 * i);
    }
}

void edb_chacha20_xor(uint8_t *out, const uint8_t *in, size_t len,
                      const uint8_t key[32], const uint8_t nonce[12],
                      uint32_t counter)
{
    uint8_t keystream[64];
    while (len > 0) {
        uint32_t state[16];
        uint32_t block[16];
        size_t i;
        size_t n = len < 64 ? len : 64;
        chacha20_init(state, key, counter, nonce);
        chacha20_block(block, state);
        for (i = 0; i < 16; i++) {
            store32_le(keystream + 4 * i, block[i]);
        }
        for (i = 0; i < n; i++) {
            out[i] = in[i] ^ keystream[i];
        }
        out += n;
        in += n;
        len -= n;
        counter++;
    }
}

/* ------------------------------------------------------------------ */
/* HMAC-SHA256 (RFC 2104)                                              */

static void hmac_sha256(const uint8_t *key, size_t key_len,
                        const uint8_t *msg, size_t msg_len, uint8_t out[32])
{
    uint8_t k[64];
    uint8_t ipad[64];
    uint8_t opad[64];
    uint8_t *inner = NULL;
    uint8_t inner_hash[32];
    size_t i;

    if (key_len > 64) {
        edb_sha256(key, key_len, k);
        memset(k + 32, 0, 32);
    } else {
        memcpy(k, key, key_len);
        memset(k + key_len, 0, 64 - key_len);
    }
    for (i = 0; i < 64; i++) {
        ipad[i] = k[i] ^ 0x36;
        opad[i] = k[i] ^ 0x5c;
    }

    inner = malloc(64 + msg_len);
    if (!inner) {
        memset(out, 0, 32);
        return;
    }
    memcpy(inner, ipad, 64);
    memcpy(inner + 64, msg, msg_len);
    edb_sha256(inner, 64 + msg_len, inner_hash);
    free(inner);

    {
        uint8_t outer[96];
        memcpy(outer, opad, 64);
        memcpy(outer + 64, inner_hash, 32);
        edb_sha256(outer, 96, out);
    }
}

void edb_hmac_sha256(const uint8_t *key, size_t key_len,
                     const uint8_t *msg, size_t msg_len, uint8_t out[32])
{
    hmac_sha256(key, key_len, msg, msg_len, out);
}

/* ------------------------------------------------------------------ */
/* HKDF-SHA256 (RFC 5869)                                              */

int edb_hkdf_sha256(const uint8_t *ikm, size_t ikm_len,
                    const uint8_t *salt, size_t salt_len,
                    const uint8_t *info, size_t info_len,
                    uint8_t *okm, size_t okm_len)
{
    uint8_t prk[32];
    uint8_t *t = NULL;
    size_t t_len = 0;
    size_t offset = 0;
    uint8_t counter = 1;

    if (okm_len > 255 * 32) {
        return -1;
    }

    if (salt && salt_len) {
        hmac_sha256(salt, salt_len, ikm, ikm_len, prk);
    } else {
        uint8_t zeros[32];
        memset(zeros, 0, sizeof(zeros));
        hmac_sha256(zeros, sizeof(zeros), ikm, ikm_len, prk);
    }

    while (offset < okm_len) {
        size_t need = okm_len - offset < 32 ? okm_len - offset : 32;
        uint8_t *buf = malloc(t_len + info_len + 1);
        if (!buf) {
            free(t);
            return -1;
        }
        if (t_len) {
            memcpy(buf, t, t_len);
        }
        if (info_len) {
            memcpy(buf + t_len, info, info_len);
        }
        buf[t_len + info_len] = counter;
        {
            uint8_t next[32];
            hmac_sha256(prk, 32, buf, t_len + info_len + 1, next);
            free(buf);
            free(t);
            t = malloc(32);
            if (!t) {
                return -1;
            }
            memcpy(t, next, 32);
            t_len = 32;
        }
        memcpy(okm + offset, t, need);
        offset += need;
        counter++;
    }
    free(t);
    return 0;
}

/* ------------------------------------------------------------------ */
/* Encrypt-then-MAC AEAD                                               */

static int ct_equal(const uint8_t *a, const uint8_t *b, size_t n)
{
    uint8_t diff = 0;
    size_t i;
    for (i = 0; i < n; i++) {
        diff |= a[i] ^ b[i];
    }
    return diff == 0;
}

int edb_aead_seal(const uint8_t enc_key[32], const uint8_t mac_key[32],
                  const uint8_t nonce[12], const uint8_t *aad, size_t aad_len,
                  const uint8_t *plain, size_t plain_len,
                  uint8_t *out, size_t out_cap)
{
    uint8_t *mac_in;
    size_t mac_len;
    size_t total = EDB_NONCE_LEN + plain_len + EDB_TAG_LEN;
    if (out_cap < total) {
        return -1;
    }

    memcpy(out, nonce, EDB_NONCE_LEN);
    edb_chacha20_xor(out + EDB_NONCE_LEN, plain, plain_len, enc_key, nonce, 1);

    mac_len = aad_len + EDB_NONCE_LEN + plain_len;
    mac_in = malloc(mac_len ? mac_len : 1);
    if (!mac_in) {
        return -1;
    }
    if (aad_len) {
        memcpy(mac_in, aad, aad_len);
    }
    memcpy(mac_in + aad_len, out, EDB_NONCE_LEN + plain_len);
    edb_hmac_sha256(mac_key, 32, mac_in, mac_len,
                    out + EDB_NONCE_LEN + plain_len);
    free(mac_in);
    return 0;
}

int edb_aead_open(const uint8_t enc_key[32], const uint8_t mac_key[32],
                  const uint8_t *aad, size_t aad_len,
                  const uint8_t *in, size_t in_len,
                  uint8_t *out, size_t out_cap)
{
    const uint8_t *nonce;
    const uint8_t *cipher;
    const uint8_t *tag;
    size_t plain_len;
    uint8_t expected[32];
    uint8_t *mac_in;
    size_t mac_len;
    int ok;

    if (in_len < EDB_NONCE_LEN + EDB_TAG_LEN) {
        return -1;
    }
    plain_len = in_len - EDB_NONCE_LEN - EDB_TAG_LEN;
    if (plain_len > out_cap) {
        return -1;
    }
    nonce = in;
    cipher = in + EDB_NONCE_LEN;
    tag = in + EDB_NONCE_LEN + plain_len;

    mac_len = aad_len + EDB_NONCE_LEN + plain_len;
    mac_in = malloc(mac_len ? mac_len : 1);
    if (!mac_in) {
        return -1;
    }
    if (aad_len) {
        memcpy(mac_in, aad, aad_len);
    }
    memcpy(mac_in + aad_len, in, EDB_NONCE_LEN + plain_len);
    edb_hmac_sha256(mac_key, 32, mac_in, mac_len, expected);
    free(mac_in);

    if (!ct_equal(expected, tag, EDB_TAG_LEN)) {
        return -1;
    }
    (void)ok;
    edb_chacha20_xor(out, cipher, plain_len, enc_key, nonce, 1);
    return (int)plain_len;
}

/* ------------------------------------------------------------------ */
/* self-test                                                           */

static int hex_eq(const char *hex, const uint8_t *bytes, size_t n)
{
    static const char digits[] = "0123456789abcdef";
    size_t i;
    for (i = 0; i < n; i++) {
        int hi = bytes[i] >> 4;
        int lo = bytes[i] & 0xf;
        if (hex[2 * i] != digits[hi] || hex[2 * i + 1] != digits[lo]) {
            return 0;
        }
    }
    return 1;
}

static int test_chacha20(void)
{
    uint8_t key[32];
    uint8_t nonce[12] = { 0, 0, 0, 0, 0, 0, 0, 0x4a, 0, 0, 0, 0 };
    const char *plain =
        "Ladies and Gentlemen of the class of '99: If I could offer you "
        "only one tip for the future, sunscreen would be it.";
    size_t plen = strlen(plain);
    uint8_t out[128];
    int i;
    for (i = 0; i < 32; i++) {
        key[i] = (uint8_t)i;
    }
    edb_chacha20_xor(out, (const uint8_t *)plain, plen, key, nonce, 1);
    return hex_eq(
        "6e2e359a2568f98041ba0728dd0d6981"
        "e97e7aec1d4360c20a27afccfd9fae0b"
        "f91b65c5524733ab8f593dabcd62b357"
        "1639d624e65152ab8f530c359f0861d8"
        "07ca0dbf500d6a6156a38e088a22b65e"
        "52bc514d16ccf806818ce91ab7793736"
        "5af90bbf74a35be6b40b8eedf2785e42"
        "874d",
        out, plen);
}

static int test_hmac(void)
{
    uint8_t key[20];
    uint8_t out[32];
    memset(key, 0x0b, sizeof(key));
    edb_hmac_sha256(key, sizeof(key), (const uint8_t *)"Hi There", 8, out);
    return hex_eq(
        "b0344c61d8db38535ca8afceaf0bf12b"
        "881dc200c9833da726e9376c2e32cff7",
        out, 32);
}

static int test_hkdf(void)
{
    uint8_t ikm[22];
    uint8_t salt[13];
    uint8_t info[10];
    uint8_t okm[42];
    int i;
    memset(ikm, 0x0b, sizeof(ikm));
    for (i = 0; i < 13; i++) {
        salt[i] = (uint8_t)i;
    }
    for (i = 0; i < 10; i++) {
        info[i] = (uint8_t)(0xf0 + i);
    }
    if (edb_hkdf_sha256(ikm, sizeof(ikm), salt, sizeof(salt), info,
                        sizeof(info), okm, sizeof(okm)) != 0) {
        return 0;
    }
    return hex_eq(
        "3cb25f25faacd57a90434f64d0362f2a"
        "2d2d0a90cf1a5a4c5db02d56ecc4c5bf"
        "34007208d5b887185865",
        okm, 42);
}

static int test_aead(void)
{
    uint8_t enc_key[32];
    uint8_t mac_key[32];
    uint8_t nonce[12] = { 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };
    const char *plain = "attack at dawn";
    const char *aad = "estp";
    uint8_t sealed[128];
    uint8_t opened[128];
    int i;
    int n;

    for (i = 0; i < 32; i++) {
        enc_key[i] = (uint8_t)(0x40 + i);
        mac_key[i] = (uint8_t)(0x80 + i);
    }
    if (edb_aead_seal(enc_key, mac_key, nonce, (const uint8_t *)aad,
                      strlen(aad), (const uint8_t *)plain, strlen(plain),
                      sealed, sizeof(sealed)) != 0) {
        return 0;
    }
    n = edb_aead_open(enc_key, mac_key, (const uint8_t *)aad, strlen(aad),
                      sealed, strlen(plain) + EDB_NONCE_LEN + EDB_TAG_LEN,
                      opened, sizeof(opened));
    if (n != (int)strlen(plain) || memcmp(opened, plain, strlen(plain)) != 0) {
        return 0;
    }

    /* tampering must be rejected */
    sealed[EDB_NONCE_LEN] ^= 0x01;
    if (edb_aead_open(enc_key, mac_key, (const uint8_t *)aad, strlen(aad),
                      sealed, strlen(plain) + EDB_NONCE_LEN + EDB_TAG_LEN,
                      opened, sizeof(opened)) != -1) {
        return 0;
    }
    return 1;
}

int edb_crypto_selftest(void)
{
    if (!test_chacha20()) {
        return 1;
    }
    if (!test_hmac()) {
        return 2;
    }
    if (!test_hkdf()) {
        return 3;
    }
    if (!test_aead()) {
        return 4;
    }
    return 0;
}
