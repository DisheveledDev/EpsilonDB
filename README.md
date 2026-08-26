# EpsilonDB

A standalone distributed key/value database server in ANSI C, modeled on
the Switchblade Swift library. One SQLite file per `(partition, keyspace)`
shard, a REST API for clients, an admin API plus `epsilonctl` CLI for
operations, and a peer-to-peer mesh that replicates and rebalances shards
across nodes.

EpsilonDB is a **multi-master, scale-out** key/value store: every node is a
full peer that can accept writes and answer any query. Data is sharded
across a hash space that grows with the cluster, so adding a node adds
storage *and* query processing power. It deploys as a single self-contained
binary, needs no external services, and ships with built-in workload
analytics, slow-query profiling, and a performance benchmark so you can
watch it perform as you grow it.

## Why EpsilonDB

- **Multi-master** — no single writer, no single point of failure. Any node
  accepts writes; they fan out to replicas and commit when a quorum of
  holders acknowledges. A node that drops offline is no problem: its peers
  cache the changes it missed and replay them when it returns.
- **Scale-out by adding nodes** — shards are spread across contiguous hash
  ranges assigned to nodes. Every node you join brings more disk, more
  cache, and more query CPU; the hash space simply rebalances to include it.
- **Write leveling** — each `(partition, keyspace)` maps to its own shard
  placed by hash, so unrelated workloads land on different nodes instead of
  hammering one box.
- **Queries spread across nodes and shards** — a partition/keyspace read
  only touches the shards that hold it, and quorum reads fan out to all
  replica holders in parallel. Bigger clusters answer bigger workloads.
- **Any node answers any query** — reads can be served locally, proxied to
  the owner, or resolved by quorum comparison across replicas.
- **Trivial to deploy** — a single C11/POSIX binary with SQLite and cJSON
  compiled in. `make STATIC=1` produces a standalone executable you can copy
  onto any same-architecture machine.
- **Built-in observability** — per-node reads/writes/queries, hot-shard
  detection, slowest operations, and replication backlog are recorded,
  synced around the cluster, and rendered as a chart.js dashboard in the
  admin console. A one-command benchmark profiles writes, gets, filtered
  queries, updates and deletes.

## Features

- **REST data API**: put/get/delete/all/ids/query with typed JSON filters,
  nested key paths, comparison operators, and TTLs (`?ttl=seconds`).
- **Sharded storage**: one SQLite database per shard, named from a framed
  digest of partition and keyspace; legacy concatenated-name shards migrate
  lazily; soft deletes; a 60s cleanup pass with a 2h grace window (lets
  offline nodes replay missed changes). Per-partition tuning: SQLite cache
  size, journal mode (DELETE/TRUNCATE/WAL, default TRUNCATE), and
  VACUUM/REINDEX intervals, applied on shard open and on update.
- **Clustering**: permanent TCP mesh between peers (ESTP protocol),
  heartbeat-based membership, deterministic leader election
  (lexicographically smallest online node id), contiguous hash-range
  placement of shards.
- **Replication**: per-database replication factor. Writes are rejected
  (HTTP 503) when fewer than `rf/2+1` holders acknowledge - no
  accept-and-queue. Changes destined for offline nodes are cached in a
  persistent local change log and replayed when the node returns.
- **Quorum reads**: with rf > 1, GET/all/ids/query fan out to replica
  holders and return only records a quorum agrees on (last-write-wins,
  node id as deterministic tie-breaker).
- **Online rebalancing**: joining nodes receive shard snapshots via the
  SQLite online backup API, replay cached deltas, then report compliance;
  when every node complies the leader promotes the new structure and old
  owners garbage-collect redundant shards one at a time. Only one node
  joins at a time (leader-held global rebalance lock).
- **Workload analytics**: every node records reads, writes, updates and
  deletes per shard plus latency, flushes a single snapshot into the system
  store every 10s (30-minute TTL, self-cleaning), and the whole cluster view
  is available from any node. Slow operations are keyed by
  partition/keyspace (and filter key for queries) - filter *values* are
  never recorded.
- **Performance benchmark**: `epsilonctl bench` or the admin console creates a
  throwaway database and 10 partitions, times writes/gets/filtered
  queries/updates/deletes, reports ops/sec, and deletes it all afterwards.
  Partitions are spread across worker threads (one per partition by
  default), so every shard is read/written independently and concurrently.
- **Auth**: Bearer token or `authorization` key; the token is the
  username. Per-partition create/update/read/delete group bitmasks
  (mask 0 = allow all groups). Before the first user exists,
  unauthenticated requests have full rights so the first admin can be
  created.

## High-performance, scale-out model

EpsilonDB treats every node as an equal. There is no primary/replica split
and no hot master: the cluster elects a leader only to coordinate
rebalancing, never to gate ordinary reads and writes.

**Write path.** A write to any node is applied locally, fanned out to the
other replica holders, and acknowledged only once a quorum
(`rf/2+1`) confirms it. Because each `(partition, keyspace)` shard is placed
on a different slice of the hash space, writes across your workload spread
evenly over the nodes rather than concentrating on one - that is the write
leveling that lets larger clusters absorb more write throughput.

**Read path.** With a replication factor of 1 a read is served directly from
the node that owns the shard (or proxied there). With `rf > 1`, reads fan
out to every replica holder and records are returned only where a quorum
agrees, so a stale or partitioned node can never hand back a wrong answer.
Collection queries stay within the shards that actually hold the matching
data, so query work is spread across nodes *and* across shards.

**Scale.** Because the hash space is re-partitioned on every membership
change, adding a node both expands total capacity and adds another set of
CPU cores for query execution. Rebalancing is online and transactional:
snapshots move via the SQLite backup API while the cluster keeps serving,
deltas written during the move are replayed, and the new layout is promoted
atomically. A bigger cluster is a more performant cluster, which is exactly
what you want on cloud hardware where you can grow CPU, disk and network
together.

**Resilience.** Multi-master means an offline node doesn't stop the system:
writes keep committing on the surviving quorum, cached changes are replayed
when the node returns, and if the leader itself disappears the remaining
nodes elect a new one and carry on.

## Why SQLite

Each shard is a single SQLite database file, and that choice is deliberate:
SQLite's design maps almost one-to-one onto what a EpsilonDB shard needs, so
we get a battle-tested storage layer instead of writing one ourselves.

**One file per shard.** A shard is `(partition, keyspace)` — small,
self-contained, and movable. SQLite is a single-file format with no server
process, no configuration, and no daemon to operate. Snapshotting a shard
for rebalancing is the online backup API; garbage-collecting a shard after a
move is deleting one file. Because every shard is small and independent,
each file's overhead and blast radius is bounded: a corrupt or full disk
affects one shard, never the cluster.

**Filtering, in the database.** EpsilonDB's typed JSON filters (dotted paths,
`eq`/`ne`/`gt`/`gte`/`lt`/`lte`, type-aware values) are evaluated directly
against the stored document by SQLite's JSON1 functions, close to the data.
A raw key/value store would force us to scan and parse every document in our
own C and reimplement JSON type coercion; SQLite already does that, and keeps
the door open to expression indexes on hot JSON paths later.

**TTL, soft deletes and cleanup as SQL.** TTL is an indexed integer column;
a delete is `UPDATE Data SET ttl = now - 5`; the 60-second cleanup pass is a
single `DELETE ... WHERE ttl < ?` with a two-hour grace window so offline
nodes can replay missed changes. Ordered iteration
(`ORDER BY timestamp ASC`) for `ids`/`all`/`query` falls out of the B-tree
for free, as does VACUUM/REINDEX maintenance after update-heavy periods.

**Transactional snapshot transfer.** Rebalancing copies a live shard with the
`sqlite3_backup_*` API, which streams a transactionally-consistent image
while writes keep flowing — never a raw file copy. `synchronous=FULL` (and a
configurable journal mode: DELETE/TRUNCATE/WAL) makes every commit and every
transferred copy crash-safe.

**A concurrency model that matches.** Each shard is serialized by one mutex
and opened with `SQLITE_OPEN_FULLMUTEX`. SQLite's locking is sometimes cited
as a limitation in shared-database deployments; here it is a feature, because
a shard already has exactly one writer at a time by design.

**Bulletproof by construction.** SQLite is the most widely deployed database
engine in the world — inside phones, browsers, embedded devices and servers —
and is tested to an unusual degree (the SQLite team reports 100% MC/DC branch
coverage, with far more test code than library code). It is vendored here as a
single amalgamation compiled straight into `epsilond`, so there is no external
dependency, no version skew, and no runtime to monitor. The result is a
storage layer that has been exercised on billions of devices and keeps
EpsilonDB's own footprint small and dependable.

## Build

Requires a C11 compiler, POSIX.1-2008 (pthreads). No external
dependencies: SQLite and cJSON are vendored under `src/sqlite/` and
`vendor/cjson/`.

    make            # builds bin/epsilond, bin/epsilonctl, bin/libepsilon.a
    make test       # full suite (engine, config, http, cluster,
                    # replication, structure, snapshot, delta, rebalance,
                    # live join, chaos); takes a few minutes
    make clean      # removes bin/ and test artifacts
    make STATIC=1   # Linux/glibc only: fully static executables

SQLite and cJSON are already compiled into the executables, so the only
external dependency is the C library. `make STATIC=1` links that too on
Linux/glibc, producing standalone binaries that can be copied to any other
machine with the same architecture. macOS does not ship a static system
library, so `STATIC=1` is rejected there; macOS binaries are already
self-contained apart from the OS-provided libSystem.

## Running

Start a single server:

    bin/epsilond -p 8123 -d ./data

Options: `-p port` (HTTP, default 8123), `-b addr` (bind address),
`-d dir` (data dir), `-s path` (Unix admin socket, default
`./epsilon-admin.sock`). Add `-n port` to enable clustering (peer port) and
`-A addr` to override the advertised address.

On startup the server prints a short "how to connect" notice with the admin
console and REST URLs, plus a reminder to open the HTTP port (and the peer
port for cluster nodes) in any firewall, and to put a TLS-terminating
reverse proxy in front of the HTTP port for encrypted remote access.

The server writes a timestamped log to `/var/log/epsilondb/epsilondb.log`
(create the directory first, or run with privileges to write there). Use
`-l path` to override the log file; when the file cannot be opened the
server falls back to logging on the console. The log is rotated once per
day (renamed with a date suffix and gzip-compressed).

## Installing as a service (macOS)

`epsilonctl install` walks you through the server parameters and writes a
`launchd` agent (`~/Library/LaunchAgents/com.epsilondb.server.plist`):

    bin/epsilonctl install

It asks for the HTTP bind address, HTTP port, cluster peer port
(0 = single node) and advertised address, then the data directory and log
path. It reports which TCP ports to open in a firewall, how to run the
service via `launchctl`, and offers to open the admin console in your
browser. Run it again any time:

    bin/epsilonctl setup

`setup` re-asks the same questions, rewrites the service file, and pushes
the new parameters to the running server as settings (`server.bind`,
`server.http_port`, `server.peer_port`, `server.advertise`,
`server.data_dir`, `server.log_path`) — restart the service afterwards so
they take effect.

## Admin console

The embedded Bootstrap web console is served at `/admin` straight from the
`epsilond` binary. On first run it shows a setup form that creates the `admin`
user with a password; afterwards it presents a login form. The console then
exposes the same operations as `epsilonctl` (status, databases, groups, users,
partitions, keyspaces, settings, data, analytics, benchmark, and cluster)
backed by the JSON API.

The **Analytics** tab gives you the live cluster picture: summary cards for
nodes, reads, writes, updates, deletes, average read/write latency and
replication backlog; a chart.js bar chart of reads/writes per shard (hot
spots); a top-10 slowest-operations chart colour-coded by kind (read, write,
delete, query); and tabular breakdowns. The **Benchmark** tab runs the
performance test from the browser with your chosen record count, replication
factor, cache size and journal mode.

Passwords are stored as salted, iterated SHA-256 hashes and authenticate via
short-lived session tokens (`Authorization: Bearer <token>`). The console's
own HTML/JS is embedded; only the Bootstrap stylesheet and script load from
the jsDelivr CDN (the default Bootstrap theme). Use a TLS-terminating reverse
proxy in front of the HTTP port in production.

The Unix admin socket speaks HTTP without authentication and is what
`epsilonctl` uses by default (full local admin rights).

## Monitoring

Workload and performance data is collected on every node and replicated
through the system store, so any node can show the whole cluster's metrics:

- `GET /admin/analytics` (or the Analytics tab) aggregates per-node
  snapshots into reads/writes/updates/deletes per partition/keyspace,
  slowest operations with latency, and the pending-replication backlog.
- `epsilonctl bench [records] [rf] [cache_size] [journal_mode] [threads]`
  runs the throwaway benchmark and prints writes/s, gets/s, queries/s,
  updates/s and deletes/s. The optional trailing `threads` argument
  controls how many worker threads the partitions are spread across
  (0 = one thread per partition); the reported count in the result
  reflects the threads actually used.

Snapshots are refreshed every 10 seconds and carry a 30-minute TTL, so a
departed node simply ages out of the picture.

## Quick start

    # bootstrap the first admin (works pre-bootstrap over plain HTTP)
    curl -X POST localhost:8123/admin/users \
         -d '{"name":"root","groups":1}'

    # create a database with replication factor 2
    curl -u ignored:root -X POST localhost:8123/admin/databases \
         -d '{"name":"app","replication_factor":2}'

    # write and query documents with typed, nested JSON filters
    curl -u ignored:root -X PUT \
         'localhost:8123/data/app/main/kv/user-1?ttl=3600' \
         -d '{"name":"Ada","age":43,"manager":{"age":51}}'
    curl -u ignored:root \
         'localhost:8123/data/app/main/kv/user-1'
    curl -u ignored:root -X POST \
         'localhost:8123/data/app/main/kv/query' \
         -d '{"key":"manager.age","operator":"gt","value":42}'

Or with the CLI (talks to the local admin socket):

    bin/epsilonctl status
    bin/epsilonctl create database app 1
    bin/epsilonctl put app/main/kv/user-1        # reads JSON from stdin
    bin/epsilonctl get app/main/kv/user-1
    bin/epsilonctl all app/main/kv \
      --filter '{"key":"age","operator":"gte","value":18}'
    bin/epsilonctl list databases|groups|users|partitions|settings|nodes
    bin/epsilonctl cluster
    bin/epsilonctl bench 100000 1 2048 TRUNCATE 10

## JSON filters

Collection reads accept one filter object or a `filters` array in a POST body:

```json
{
  "filters": [
    {"key": "age", "operator": "gte", "value": 18},
    {"key": "manager.age", "operator": "lt", "value": 65}
  ]
}
```

Keys use dot notation to traverse nested objects. Supported operators are
`eq`, `ne`, `gt`, `gte`, `lt`, and `lte`; multiple filters use AND semantics.
Values retain their JSON type, so `42`, `"42"`, `true`, and `null` are distinct.
Use POST with `/all`, `/ids`, or `/query`; unfiltered GET requests remain valid.
The CLI accepts the same objects with repeated `--filter '<json>'` arguments.

## Clustering

Every node runs the same binary. To form a cluster:

    # seed
    bin/epsilond -p 8123 -n 9123 -d node-a
    # joiners (join through any existing member's peer port)
    bin/epsilond -p 8124 -n 9124 -d node-b
    curl -X POST localhost:8124/admin/join -d '{"addr":"127.0.0.1","port":9123}'
    bin/epsilond -p 8125 -n 9125 -d node-c
    curl -X POST localhost:8125/admin/join -d '{"addr":"127.0.0.1","port":9123}'

`POST /admin/join` runs the whole flow: the joiner snapshots the config
shards, the leader publishes a target range table, the joiner pulls every
shard assigned to it (snapshot + cached delta replay), reports compliance,
and the leader promotes the target structure automatically. A failed join
rolls the cluster back to the live structure. Only one node may join at a
time (leader-held global rebalance lock).

Observe the cluster:

    bin/epsilonctl cluster          # or: GET /admin/cluster
    bin/epsilonctl list nodes

The response includes live/target structure versions, rebalance lock
state, per-node compliance generation, and the live/target hash ranges.

Failure semantics:

- A node that stops receiving heartbeats from a peer marks it offline
  after ~12s; writes it would have received are cached locally and
  replayed when it returns.
- If the leader dies, the remaining nodes elect the lexicographically
  smallest online id and continue serving writes.
- Writes that cannot reach a quorum of holders are rejected with HTTP
  503; they are never silently queued.

## Architecture

| Surface | Purpose |
|---|---|
| HTTP port | client REST + admin API (put TLS termination in a reverse proxy) |
| Unix admin socket | local management traffic for `epsilonctl`, unauthenticated by design |
| Peer TCP mesh | ESTP frames: identity/state gossip, replication, quorum queries, snapshot transfer |

Directory map:

    src/engine/    shard manager, SQLite shards, TTL cleanup, config store,
                   analytics, benchmark
    src/socket/    ESTP wire codec, cluster mesh, replication, snapshots
    src/httpd/     minimal HTTP/1.1 server (keep-alive, routes, static files)
    src/api/       REST handlers (data ops, sync protocol, admin ops)
    src/sqlite/    vendored SQLite amalgamation (do not modify)
    vendor/        vendored cJSON (do not modify)
    tests/         plain-assert test harness, run via make test

Design notes inherited from Switchblade:

- Delete is a soft delete (`ttl = now - 5`); rows are physically removed
  only after a 2-hour grace window so returning nodes can catch up.
- Filter queries use SQLite JSON functions directly against document values.
  Filters have `key`, `operator`, and typed `value` fields; dotted keys traverse
  nested objects, and multiple filters use AND semantics. Supported operators
  are `eq`, `ne`, `gt`, `gte`, `lt`, and `lte`.
- Shard files are copied between nodes only through `sqlite3_backup_*`
  while `synchronous=FULL`, so transfers are always consistent regardless of
  the configured journal mode.

## Status

Stages 0-7 complete: engine, config store, HTTP/CLI, clustering,
replication + quorum reads, live/target rebalancing (snapshot transfer,
delta catch-up, promotion + GC, end-to-end join wiring), multi-node
chaos/failure testing, workload analytics, and performance benchmarking.
See `AGENTS.md` for the detailed stage history and gotchas.
