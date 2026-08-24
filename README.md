# ZestyDB

A standalone distributed key/value database server in ANSI C, modeled on
the Switchblade Swift library. One SQLite file per `(partition, keyspace)`
shard, a REST API for clients, an admin API plus `zestyctl` CLI for
operations, and a peer-to-peer mesh that replicates and rebalances shards
across nodes.

## Features

- **REST data API**: put/get/delete/all/ids/query with optional indexed
  filters (`?filter=k=v`, repeatable) and TTLs (`?ttl=seconds`).
- **Sharded storage**: shard file = SQLite db named from a framed digest of
  partition and keyspace; legacy concatenated-name shards migrate lazily;
  soft deletes; 60s cleanup pass
  with a 2h grace window (lets offline nodes replay missed changes);
  automatic VACUUM after 10k expirations.
- **Clustering**: permanent TCP mesh between peers (ZSTP protocol),
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
  owners garbage-collect redundant shards one at a time.
- **Auth**: Bearer token or `authorization` key; the token is the
  username. Per-partition create/update/read/delete group bitmasks
  (mask 0 = allow all groups). Before the first user exists,
  unauthenticated requests have full rights so the first admin can be
  created.

## Build

Requires a C11 compiler, POSIX.1-2008 (pthreads). No external
dependencies: SQLite and cJSON are vendored under `src/sqlite/` and
`vendor/cjson/`.

    make            # builds bin/zestyd, bin/zestyctl, bin/libzesty.a
    make test       # full suite (engine, config, http, cluster,
                    # replication, structure, snapshot, delta, rebalance,
                    # live join, chaos); takes a few minutes
    make clean      # removes bin/ and test artifacts

## Running

Start a single server:

    bin/zestyd -p 8123 -d ./data

Options: `-p port` (HTTP, default 8123), `-b addr` (bind address),
`-d dir` (data dir), `-a dir` (admin UI static dir), `-s path`
(Unix admin socket, default `./zesty-admin.sock`). Add `-n port` to
enable clustering (peer port) and `-A addr` to override the advertised
address.

The Unix admin socket speaks HTTP without authentication and is what
`zestyctl` uses by default (full local admin rights).

## Quick start

    # bootstrap the first admin (works pre-bootstrap over plain HTTP)
    curl -X POST localhost:8123/admin/users \
         -d '{"name":"root","groups":1}'

    # create a database with replication factor 2
    curl -u ignored:root -X POST localhost:8123/admin/databases \
         -d '{"name":"app","replication_factor":2}'

    # write and read documents ("filters" index the doc for queries)
    curl -u ignored:root -X PUT \
         'localhost:8123/data/app/main/kv/user-1?ttl=3600&filter=kind=user' \
         -d '{"name":"Ada","kind":"user"}'
    curl -u ignored:root \
         'localhost:8123/data/app/main/kv/user-1'
    curl -u ignored:root \
         'localhost:8123/data/app/main/kv/query?filter=kind=user'

Or with the CLI (talks to the local admin socket):

    bin/zestyctl status
    bin/zestyctl create database app 1
    bin/zestyctl put app/main/kv/user-1        # reads JSON from stdin
    bin/zestyctl get app/main/kv/user-1
    bin/zestyctl all app/main/kv --filter kind=user
    bin/zestyctl list databases|groups|users|partitions|settings|nodes
    bin/zestyctl cluster

## Clustering

Every node runs the same binary. To form a cluster:

    # seed
    bin/zestyd -p 8123 -n 9123 -d node-a
    # joiners (join through any existing member's peer port)
    bin/zestyd -p 8124 -n 9124 -d node-b
    curl -X POST localhost:8124/admin/join -d '{"addr":"127.0.0.1","port":9123}'
    bin/zestyd -p 8125 -n 9125 -d node-c
    curl -X POST localhost:8125/admin/join -d '{"addr":"127.0.0.1","port":9123}'

`POST /admin/join` runs the whole flow: the joiner snapshots the config
shards, the leader publishes a target range table, the joiner pulls every
shard assigned to it (snapshot + cached delta replay), reports compliance,
and the leader promotes the target structure automatically. A failed join
rolls the cluster back to the live structure. Only one node may join at a
time (leader-held global rebalance lock).

Observe the cluster:

    bin/zestyctl cluster          # or: GET /admin/cluster
    bin/zestyctl list nodes

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
| Unix admin socket | local management traffic for `zestyctl`, unauthenticated by design |
| Peer TCP mesh | ZSTP frames: identity/state gossip, replication, quorum queries, snapshot transfer |

Directory map:

    src/engine/    shard manager, SQLite shards, TTL cleanup, config store
    src/socket/    ZSTP wire codec, cluster mesh, replication, snapshots
    src/httpd/     minimal HTTP/1.1 server (keep-alive, routes, static files)
    src/api/       REST handlers (data ops, sync protocol, admin ops)
    src/sqlite/    vendored SQLite amalgamation (do not modify)
    vendor/        vendored cJSON (do not modify)
    tests/         plain-assert test harness, run via make test

Design notes inherited from Switchblade:

- Delete is a soft delete (`ttl = now - 5`); rows are physically removed
  only after a 2-hour grace window so returning nodes can catch up.
- Filter queries use an md5("key=value") index table with AND semantics.
- Shard files are copied between nodes only through `sqlite3_backup_*`
  while journal mode is DELETE / synchronous FULL, so transfers are
  always consistent.

## Status

Stages 0-7 complete: engine, config store, HTTP/CLI, clustering,
replication + quorum reads, live/target rebalancing (snapshot transfer,
delta catch-up, promotion + GC, end-to-end join wiring), multi-node
chaos/failure testing. See `AGENTS.md` for the detailed stage history
and gotchas.
