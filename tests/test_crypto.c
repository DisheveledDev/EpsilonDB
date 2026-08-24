/* Unit test for the vendored mesh crypto (ChaCha20 + HMAC + HKDF + AEAD)
 * using RFC 8439 / RFC 4231 / RFC 5869 test vectors. */

#include <stdio.h>

#include "../src/engine/zesty_crypto.h"

int main(void)
{
    int rc = zdb_crypto_selftest();
    if (rc == 0) {
        printf("tests/test_crypto: ok\n");
        return 0;
    }
    printf("tests/test_crypto: FAILED (%d)\n", rc);
    return 1;
}
