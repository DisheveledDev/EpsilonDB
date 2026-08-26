# Windows support: port plan

Status: **plan only — no code on this branch.** This is deliberately a
decision document, because "make it Windows compatible" is not a
compatibility pass on this codebase; it is a port, and the first decision
changes everything downstream.

## 1. The decision that has to come first

| approach | what it means | cost | result |
|---|---|---|---|
| **A. WSL2 only** | Document that Windows users run EpsilonDB inside WSL2. No source changes. | ~0 | Works today. Not a native Windows service; no native `.exe`. |
| **B. MinGW-w64 / MSYS2** | Cross-compile with GCC targeting Windows. Keeps GNU make and most POSIX idioms; pthreads via winpthreads. | Medium | Native `.exe`, but still needs Winsock and path work, and ships a MinGW runtime. |
| **C. MSVC** | First-class native Windows build with its own build system. | High | Best Windows citizen; largest diff and a second build system to maintain forever. |

**Recommendation: A now, B if a native binary is genuinely required, C only
if Windows becomes a first-class target.** Nothing below should be started
until this is chosen — the shim design differs for each.

## 2. What actually has to change (measured, not estimated)

Counts are call sites / files across `src/`, excluding `vendor/`:

| area | calls | files | Windows story |
|---|---:|---:|---|
| `pthread_mutex` / `cond` / `rwlock` | 407 | 18 | winpthreads (B) or SRWLOCK/CONDITION_VARIABLE shim (C) |
| `pthread_create` / `join` | 20 | 7 | same |
| BSD sockets | 61 | 7 | Winsock2: `WSAStartup`, `closesocket`, `SOCKET` not `int`, no `EINTR` |
| **Unix domain sockets** | 17 | 4 | The admin socket. `AF_UNIX` exists on Win10 1803+ but has no `SO_PEERCRED`, which is how local admin auth is established |
| `poll` / `select` | 8 | 3 | `WSAPoll` (has known quirks) or IOCP |
| `fork` / `exec` / `waitpid` | 8 | 3 | No `fork`. Needs `CreateProcess`; affects the backup tool's `zip`/`unzip` invocation |
| `termios` (the `epsilonctl` TUI) | 17 | 3 | Console API (`SetConsoleMode`, VT sequences) |
| signal handling | 6 | 3 | No `SIGHUP`; console control handlers instead |
| POSIX file I/O | 47 | 21 | Mostly mechanical, but see §3 |
| systemd installer | 29 | 1 | `src/epsilonctl_install.c` (746 lines) is entirely Linux service management |

Plus the build system itself: the `Makefile` uses `uname -s`, `mkdir -p`,
`rm -rf`, `-lpthread` and `-D_POSIX_C_SOURCE=200809L`, and hard-errors on
`STATIC=1` for non-Linux.

## 3. The parts that are not mechanical

These are where a naive port produces silent data corruption rather than
compile errors:

1. **SQLite file locking.** The engine opens shard files from multiple
   threads and relies on POSIX advisory locking semantics plus
   `sqlite3_busy_timeout`. Windows uses mandatory locking: a file cannot be
   deleted or renamed while open. The shard GC path (`edb_shard_gc`) and the
   restore path both delete and rename shard files, and the backup tool
   renames a downloaded shard into place. Every one of those needs the
   handle closed first on Windows.

2. **Local admin authentication.** The admin socket is trusted because it is
   a local Unix socket. Windows `AF_UNIX` has no peer-credential mechanism,
   so "local means trusted" has to be re-established some other way (named
   pipe with an ACL is the idiomatic answer) or the tools must always
   authenticate.

3. **Path handling.** Shard paths are built with `snprintf("%s/%s", ...)`
   throughout and `PATH_MAX` is assumed; Windows needs `MAX_PATH`
   awareness or long-path opt-in.

4. **Line endings.** The repo currently has no `.gitattributes`. On a
   Windows checkout with `core.autocrlf=true` every `tests/*.sh` gets CRLF
   endings and becomes unrunnable (`#!/bin/sh\r` is not an interpreter).
   This already bites anyone cloning on Windows today, independent of any
   port — see the recommendation in §5.

## 4. Suggested sequencing (if B or C is chosen)

1. Add a `src/platform/` layer with `edb_thread.h`, `edb_socket.h`,
   `edb_fs.h`, `edb_proc.h`. Implement the POSIX backend first as a pure
   pass-through so the refactor is behaviour-preserving and reviewable on
   its own.
2. Move every `pthread_*`, socket, and file call in `src/` behind it. No
   Windows code yet; the tree still builds and passes on Linux and macOS.
3. Add the Windows backend, starting with `epsilond` only. Leave
   `epsilonctl`'s TUI and `epsilonbkup` for later — they are the least
   important and the most POSIX-bound.
4. Replace the systemd installer with a Windows Service (`SCM`) path, or
   ship without service install on Windows initially.
5. Add a Windows CI job. Without one this will regress within a release,
   because nothing else in the project builds on Windows.

`src/engine/epsilon_procstat.c` (added on the node-metrics branch) is
already written as a single-call abstraction, so Windows support there is
one `#elif defined(_WIN32)` branch using `GetProcessMemoryInfo` and
`GetProcessTimes`. That is the shape the rest of the shim layer should
follow.

## 5. Worth doing regardless of the decision

Add a `.gitattributes` so shell scripts keep LF endings on Windows
checkouts:

```
*.sh text eol=lf
```

This costs nothing, is independent of any port, and fixes a real problem for
anyone cloning the repo on Windows today.
