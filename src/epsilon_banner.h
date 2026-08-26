/* epsilon_banner.h - the shared EpsilonDB ASCII banner used by every
 * executable (server, CLI, backup and benchmark tools). Keep the art in
 * one place so the branding stays identical across the project.
 */

#ifndef EPSILON_BANNER_H
#define EPSILON_BANNER_H

#define EDB_BANNER_LINES 8

static const char *const edb_banner[EDB_BANNER_LINES] = {
    "  ______           _ _             _____  ____  ",
    " |  ____|         (_) |           |  __ \\|  _ \\ ",
    " | |__   _ __  ___ _| | ___  _ __ | |  | | |_) |",
    " |  __| | '_ \\/ __| | |/ _ \\| '_ \\| |  | |  _ < ",
    " | |____| |_) \\__ \\ | | (_) | | | | |__| | |_) |",
    " |______| .__/|___/_|_|\\___/|_| |_|_____/|____/ ",
    "        | |                                     ",
    "        |_|                                     ",
};

#endif
