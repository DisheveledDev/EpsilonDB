CC      ?= cc
CFLAGS  ?= -std=c11 -Wall -Wextra -Wpedantic -O2 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=
LDLIBS   = -lpthread

# --- static linking -------------------------------------------------------
# `make STATIC=1` links bin/epsilond and bin/epsilonctl fully statically so they
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
VENDOR_SRC = vendor/cjson/cJSON.c vendor/sqlite/sqlite3.c \
             vendor/lua/lapi.c vendor/lua/lauxlib.c vendor/lua/lbaselib.c \
             vendor/lua/lcode.c vendor/lua/lcorolib.c vendor/lua/lctype.c \
             vendor/lua/ldblib.c vendor/lua/ldebug.c vendor/lua/ldo.c \
             vendor/lua/ldump.c vendor/lua/lfunc.c vendor/lua/lgc.c \
             vendor/lua/linit.c vendor/lua/liolib.c vendor/lua/llex.c \
             vendor/lua/lmathlib.c vendor/lua/lmem.c vendor/lua/loadlib.c \
             vendor/lua/lobject.c vendor/lua/lopcodes.c vendor/lua/loslib.c \
             vendor/lua/lparser.c vendor/lua/lstate.c vendor/lua/lstring.c \
             vendor/lua/lstrlib.c vendor/lua/ltable.c vendor/lua/ltablib.c \
             vendor/lua/ltm.c vendor/lua/lundump.c vendor/lua/lutf8lib.c \
             vendor/lua/lvm.c vendor/lua/lzio.c

ENGINE_SRC = src/engine/md5.c src/engine/sha256.c src/engine/random.c \
             src/engine/epsilon_crypto.c src/engine/shard.c \
             src/engine/manager.c src/engine/epsilon_config.c \
             src/engine/epsilon_config_entities.c \
             src/engine/epsilon_config_partitions.c \
             src/engine/epsilon_analytics.c src/engine/epsilon_benchmark.c \
             src/epsilon_log.c
ENGINE_OBJ = $(ENGINE_SRC:.c=.o) $(VENDOR_SRC:.c=.o)
ENGINE_LIB = bin/libepsilon.a
SERVER_BIN = bin/epsilond
CLI_BIN = bin/epsilonctl
BACKUP_BIN = bin/epsilonbkup
BENCH_BIN = bin/epsilonbench
EQL_BIN = bin/eql

# cluster module (mesh core + wire codec + rebalancing)
CLUSTER_SRC = src/socket/epsilon_cluster.c src/socket/estp_wire.c \
              src/socket/epsilon_cluster_rebalance.c

# replication module (core + persisted change cache + quorum reads)
REPL_SRC = src/socket/epsilon_repl.c src/socket/epsilon_repl_cache.c \
           src/socket/epsilon_repl_read.c

# EQL module
EQL_SRC = src/eql/epsilon_eql.c

# Lua scripting engine
LUA_SRC = src/lua/epsilon_lua.c

TEST_SRC = tests/test_crypto.c tests/test_engine.c tests/test_config.c \
           tests/test_http.c tests/test_replication.c \
           tests/test_structure.c tests/test_snapshot.c tests/test_delta.c \
           tests/test_rebalance.c tests/test_join.c tests/test_chaos.c \
           tests/test_console.c tests/test_eql.c \
           tests/test_eql_api.c tests/test_lua.c tests/test_lua_api.c
TEST_BINS = tests/test_crypto tests/test_engine tests/test_config \
            tests/test_http tests/test_cluster tests/test_replication \
            tests/test_structure tests/test_snapshot tests/test_delta \
            tests/test_rebalance tests/test_join tests/test_chaos \
            tests/test_console tests/test_eql tests/test_eql_api \
            tests/test_lua tests/test_lua_api
.PHONY: all test clean

all: $(SERVER_BIN) $(CLI_BIN) $(BACKUP_BIN) $(BENCH_BIN) $(EQL_BIN)
$(ENGINE_LIB): $(ENGINE_OBJ)
	@mkdir -p bin
	ar rcs $@ $^

API_SRC = src/api/epsilon_api.c src/api/epsilon_api_data.c \
          src/api/epsilon_api_admin.c src/api/epsilon_api_cluster.c \
          src/api/epsilon_api_settings.c src/api/epsilon_api_console.c \
          src/api/epsilon_api_eql.c src/api/epsilon_api_lua.c

$(SERVER_BIN): src/epsilond.c $(API_SRC) src/httpd/epsilon_http.c \
               $(CLUSTER_SRC) $(REPL_SRC) $(LUA_SRC) \
               src/socket/epsilon_snap.c src/admin/admin_console.o \
               src/api/version.h $(ENGINE_LIB)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/epsilond.c $(API_SRC) \
		src/httpd/epsilon_http.c $(CLUSTER_SRC) \
		$(REPL_SRC) $(EQL_SRC) $(LUA_SRC) src/socket/epsilon_snap.c \
		src/admin/admin_console.o \
		$(ENGINE_SRC:.c=.o) \
		$(filter %.o,$(VENDOR_SRC:.c=.o)) $(LDFLAGS) $(STATIC_LDFLAGS) \
		$(LDLIBS) $(STATIC_LDLIBS)

CTL_SRC = src/epsilonctl.c src/epsilonctl_tui.c src/epsilonctl_install.c

$(CLI_BIN): $(CTL_SRC) vendor/cjson/cJSON.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ $(CTL_SRC) vendor/cjson/cJSON.c \
		$(LDFLAGS) $(STATIC_LDFLAGS) $(LDLIBS) $(STATIC_LDLIBS)

$(EQL_BIN): src/eql.c vendor/cjson/cJSON.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/eql.c vendor/cjson/cJSON.c 		$(LDFLAGS) $(STATIC_LDFLAGS) $(LDLIBS) $(STATIC_LDLIBS)

$(BENCH_BIN): src/epsilonbench.c src/api/version.h vendor/cjson/cJSON.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/epsilonbench.c vendor/cjson/cJSON.c \
		$(LDFLAGS) $(STATIC_LDFLAGS) $(LDLIBS) $(STATIC_LDLIBS)

$(BACKUP_BIN): src/epsilonbkup.c src/epsilonbkup_http.c src/api/version.h \
               $(ENGINE_LIB)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/epsilonbkup.c src/epsilonbkup_http.c \
		$(CLUSTER_SRC) src/socket/epsilon_snap.c \
		-Lbin -lepsilon $(LDFLAGS) $(STATIC_LDFLAGS) $(LDLIBS) \
		$(STATIC_LDLIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# The SQLite amalgamation predates strict C11 system-header interactions on
# some toolchains; compile it with relaxed flags.
vendor/sqlite/sqlite3.o: vendor/sqlite/sqlite3.c
	$(CC) -std=c11 -w -O2 -c -o $@ $<

# Lua is a C89 codebase; build it with C11 relaxed the same way so its
# implicit-int/prototype-legacy code compiles cleanly under our default flags.
vendor/lua/%.o: vendor/lua/%.c
	$(CC) -std=c11 -w -O2 -c -o $@ $<

# The embedded admin console is a single long string literal; relax the
# C99 4095-byte string-literal warning for it.
src/admin/admin_console.o: src/admin/admin_console.c
	$(CC) -std=c11 -w -O2 -D_POSIX_C_SOURCE=200809L -c -o $@ $<

test: all $(TEST_BINS)
	mkdir -p tests/data
	./tests/test_crypto && ./tests/test_engine && ./tests/test_config \
		&& ./tests/test_eql && ./tests/test_lua \
		&& ./tests/test_http_run.sh \
		&& ./tests/test_cluster && ./tests/test_replication \
		&& ./tests/test_structure && ./tests/test_snapshot \
		&& ./tests/test_delta && ./tests/test_rebalance \
		&& ./tests/test_join_run.sh && ./tests/test_chaos \
		&& ./tests/test_console_run.sh && ./tests/test_lua_apirun.sh

tests/test_crypto: tests/test_crypto.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_engine: tests/test_engine.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_config: tests/test_config.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_http: tests/test_http.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS)

tests/test_console: tests/test_console.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS)

tests/test_cluster: tests/test_cluster.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLUSTER_SRC) \
		src/socket/epsilon_snap.c -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_replication: tests/test_replication.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLUSTER_SRC) \
		$(REPL_SRC) src/socket/epsilon_snap.c \
		-Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_structure: tests/test_structure.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLUSTER_SRC) \
		src/socket/epsilon_snap.c -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_snapshot: tests/test_snapshot.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLUSTER_SRC) \
		src/socket/epsilon_snap.c -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_delta: tests/test_delta.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLUSTER_SRC) \
		$(REPL_SRC) src/socket/epsilon_snap.c \
		-Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_rebalance: tests/test_rebalance.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLUSTER_SRC) \
		$(REPL_SRC) src/socket/epsilon_snap.c \
		-Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_join: tests/test_join.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(CLUSTER_SRC) \
		$(REPL_SRC) src/socket/epsilon_snap.c \
		-Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_chaos: tests/test_chaos.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_eql_api: tests/test_eql_api.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS)

tests/test_eql: tests/test_eql.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(EQL_SRC) $(LUA_SRC) $(REPL_SRC) $(CLUSTER_SRC) \
		src/socket/epsilon_snap.c -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_lua: tests/test_lua.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< $(LUA_SRC) $(REPL_SRC) $(CLUSTER_SRC) \
		src/socket/epsilon_snap.c -Lbin -lepsilon $(LDFLAGS) $(LDLIBS)

tests/test_lua_api: tests/test_lua_api.c
	$(CC) $(CFLAGS) -o $@ $< $(LDFLAGS) $(LDLIBS)

clean:
	rm -rf bin
	rm -f $(ENGINE_OBJ) $(TEST_BINS)
	rm -rf tests/data
