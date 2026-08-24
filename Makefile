CC      ?= cc
CFLAGS  ?= -std=c11 -Wall -Wextra -Wpedantic -O2 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=
LDLIBS   = -lpthread

# --- static linking -------------------------------------------------------
# `make STATIC=1` links bin/zestyd and bin/zestyctl fully statically so they
# can be copied to other machines with the same CPU architecture and OS.
# SQLite and cJSON are already compiled in; this additionally links libc,
# libm, and pthread statically. Only Linux/glibc supports this: macOS ships
# no static libSystem, so on macOS the binaries already depend solely on the
# OS-provided system library and cannot be linked any further statically.
STATIC ?= 0
STATIC_LDFLAGS =
STATIC_LDLIBS =
ifeq ($(STATIC),1)
  ifeq ($(shell uname -s),Linux)
    STATIC_LDFLAGS := -static
    STATIC_LDLIBS  := -lm
  else
    $(error STATIC=1 is unsupported on $(shell uname -s): only Linux/glibc can be linked fully statically)
  endif
endif

# Vendored third-party sources (do not modify; see AGENTS.md)
VENDOR_SRC = vendor/cjson/cJSON.c src/sqlite/sqlite3.c

ENGINE_SRC = src/engine/md5.c src/engine/sha256.c src/engine/random.c \
             src/engine/zesty_crypto.c src/engine/shard.c \
             src/engine/manager.c src/engine/zesty_config.c
ENGINE_OBJ = $(ENGINE_SRC:.c=.o) $(VENDOR_SRC:.c=.o)
ENGINE_LIB = bin/libzesty.a
SERVER_BIN = bin/zestyd
CLI_BIN = bin/zestyctl

TEST_SRC = tests/test_crypto.c tests/test_engine.c tests/test_config.c \
           tests/test_http.c tests/test_replication.c \
           tests/test_structure.c tests/test_snapshot.c tests/test_delta.c \
           tests/test_rebalance.c tests/test_join.c tests/test_chaos.c \
           tests/test_console.c
TEST_BINS = tests/test_crypto tests/test_engine tests/test_config \
            tests/test_http tests/test_cluster tests/test_replication \
            tests/test_structure tests/test_snapshot tests/test_delta \
            tests/test_rebalance tests/test_join tests/test_chaos \
            tests/test_console
.PHONY: all test clean

all: $(SERVER_BIN) $(CLI_BIN)
$(ENGINE_LIB): $(ENGINE_OBJ)
	@mkdir -p bin
	ar rcs $@ $^

$(SERVER_BIN): src/zestyd.c src/api/zesty_api.c src/httpd/zesty_http.c \
               src/socket/zesty_cluster.c src/socket/zesty_repl.c \
               src/socket/zesty_snap.c src/admin/admin_console.o \
               $(ENGINE_LIB)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/zestyd.c src/api/zesty_api.c \
		src/httpd/zesty_http.c src/socket/zesty_cluster.c \
		src/socket/zesty_repl.c src/socket/zesty_snap.c \
		src/admin/admin_console.o \
		$(ENGINE_SRC:.c=.o) \
		$(filter %.o,$(VENDOR_SRC:.c=.o)) $(LDFLAGS) $(STATIC_LDFLAGS) \
		$(LDLIBS) $(STATIC_LDLIBS)

$(CLI_BIN): src/zestyctl.c vendor/cjson/cJSON.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/zestyctl.c vendor/cjson/cJSON.c \
		$(LDFLAGS) $(STATIC_LDFLAGS) $(LDLIBS) $(STATIC_LDLIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# The SQLite amalgamation predates strict C11 system-header interactions on
# some toolchains; compile it with relaxed flags.
src/sqlite/sqlite3.o: src/sqlite/sqlite3.c
	$(CC) -std=c11 -w -O2 -c -o $@ $<

# The embedded admin console is a single long string literal; relax the
# C99 4095-byte string-literal warning for it.
src/admin/admin_console.o: src/admin/admin_console.c
	$(CC) -std=c11 -w -O2 -D_POSIX_C_SOURCE=200809L -c -o $@ $<

test: all $(TEST_BINS)
	mkdir -p tests/data
	./tests/test_crypto && ./tests/test_engine && ./tests/test_config \
		&& ./tests/test_http_run.sh \
		&& ./tests/test_cluster && ./tests/test_replication \
		&& ./tests/test_structure && ./tests/test_snapshot \
		&& ./tests/test_delta && ./tests/test_rebalance \
		&& ./tests/test_join_run.sh && ./tests/test_chaos \
		&& ./tests/test_console_run.sh

tests/test_crypto: tests/test_crypto.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_engine: tests/test_engine.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_config: tests/test_config.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_http: tests/test_http.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS)

tests/test_console: tests/test_console.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS)

tests/test_cluster: tests/test_cluster.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< src/socket/zesty_cluster.c \
		src/socket/zesty_snap.c -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_replication: tests/test_replication.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< src/socket/zesty_cluster.c \
		src/socket/zesty_repl.c src/socket/zesty_snap.c \
		-Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_structure: tests/test_structure.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< src/socket/zesty_cluster.c \
		src/socket/zesty_snap.c -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_snapshot: tests/test_snapshot.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< src/socket/zesty_cluster.c \
		src/socket/zesty_snap.c -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_delta: tests/test_delta.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< src/socket/zesty_cluster.c \
		src/socket/zesty_repl.c src/socket/zesty_snap.c \
		-Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_rebalance: tests/test_rebalance.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< src/socket/zesty_cluster.c \
		src/socket/zesty_repl.c src/socket/zesty_snap.c \
		-Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_join: tests/test_join.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< src/socket/zesty_cluster.c \
		src/socket/zesty_repl.c src/socket/zesty_snap.c \
		-Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_chaos: tests/test_chaos.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf bin
	rm -f $(ENGINE_OBJ) $(TEST_BINS)
	rm -rf tests/data
