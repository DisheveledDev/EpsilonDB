CC      ?= cc
CFLAGS  ?= -std=c11 -Wall -Wextra -Wpedantic -O2 -D_POSIX_C_SOURCE=200809L
LDFLAGS ?=
LDLIBS   = -lpthread

# Vendored third-party sources (do not modify; see AGENTS.md)
VENDOR_SRC = vendor/cjson/cJSON.c src/sqlite/sqlite3.c

ENGINE_SRC = src/engine/md5.c src/engine/shard.c src/engine/manager.c \
             src/engine/zesty_config.c
ENGINE_OBJ = $(ENGINE_SRC:.c=.o) $(VENDOR_SRC:.c=.o)
ENGINE_LIB = bin/libzesty.a
SERVER_BIN = bin/zestyd
CLI_BIN = bin/zestyctl

TEST_SRC = tests/test_engine.c tests/test_config.c tests/test_http.c \
           tests/test_replication.c tests/test_structure.c \
           tests/test_snapshot.c tests/test_delta.c
TEST_BINS = tests/test_engine tests/test_config tests/test_http \
            tests/test_cluster tests/test_replication tests/test_structure \
            tests/test_snapshot tests/test_delta
.PHONY: all test clean

all: $(SERVER_BIN) $(CLI_BIN)
$(ENGINE_LIB): $(ENGINE_OBJ)
	@mkdir -p bin
	ar rcs $@ $^

$(SERVER_BIN): src/zestyd.c src/api/zesty_api.c src/httpd/zesty_http.c \
               src/socket/zesty_cluster.c src/socket/zesty_repl.c \
               src/socket/zesty_snap.c \
               $(ENGINE_LIB)
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/zestyd.c src/api/zesty_api.c \
		src/httpd/zesty_http.c src/socket/zesty_cluster.c \
		src/socket/zesty_repl.c src/socket/zesty_snap.c \
		$(ENGINE_SRC:.c=.o) \
		$(filter %.o,$(VENDOR_SRC:.c=.o)) $(LDFLAGS) $(LDLIBS)

$(CLI_BIN): src/zestyctl.c vendor/cjson/cJSON.c
	@mkdir -p bin
	$(CC) $(CFLAGS) -o $@ src/zestyctl.c vendor/cjson/cJSON.c \
		$(LDFLAGS) $(LDLIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c -o $@ $<

# The SQLite amalgamation predates strict C11 system-header interactions on
# some toolchains; compile it with relaxed flags.
src/sqlite/sqlite3.o: src/sqlite/sqlite3.c
	$(CC) -std=c11 -w -O2 -c -o $@ $<

test: all $(TEST_BINS)
	mkdir -p tests/data
	./tests/test_engine && ./tests/test_config && ./tests/test_http_run.sh \
		&& ./tests/test_cluster && ./tests/test_replication \
		&& ./tests/test_structure && ./tests/test_snapshot \
		&& ./tests/test_delta

tests/test_engine: tests/test_engine.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_config: tests/test_config.c $(ENGINE_LIB)
	$(CC) $(CFLAGS) -o $@ $< -Lbin -lzesty $(LDFLAGS) $(LDLIBS)

tests/test_http: tests/test_http.c
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

clean:
	rm -rf bin
	rm -f $(ENGINE_OBJ) $(TEST_BINS)
	rm -rf tests/data
