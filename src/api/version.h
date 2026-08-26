/* version.h - single source of version truth.
 *
 * sw_version is displayed by the /status endpoint (and from there by the
 * CLI's status/TUI and the admin console) and on every executable banner.
 * "0.0.0" is the dev placeholder: the CI/CD pipeline rewrites it with the
 * release tag before building (see .github/workflows/c-cpp.yml), so shipped
 * binaries report the exact tag they were built from.
 *
 * It is a per-translation-unit constant so the header can be included
 * safely wherever a version string is needed.
 */

#ifndef EDB_VERSION_H
#define EDB_VERSION_H

static const char *const sw_version = "0.0.0";

#endif
