# Cache stampede lab

A runnable Go demonstration of how cache-miss coordination protects PostgreSQL. Three application replicas share Redis and PostgreSQL behind Nginx. Prometheus collects each replica's metrics, and an open-loop generator offers 2,000 requests per second by default.

OpenAI describes cache locking and leasing so one reader refills a missing key while others wait in its [PostgreSQL scaling article](https://openai.com/index/scaling-postgresql/). Go, Redis, Lua, and this particular protocol are choices made for this prototype. This is not a claim about OpenAI's implementation.

## Run

Requires Docker with Compose, Go 1.23 or newer, and Python 3.9 or newer. Python scripts use only the standard library. On machines with the Compose plugin, replace `docker-compose` below with `docker compose`; the benchmark runner detects either form.

```sh
docker-compose up -d --build
go build -o bin/loadgen ./cmd/loadgen
curl http://localhost:18080/items/1
curl http://localhost:18081/metrics
```

The database seeds 10,000 records. The endpoint is `GET /items/{id}`. Invalid IDs return 400; absent database rows return 404 and are not negatively cached. Redis failures and admission rejections return 503. Exhausted wait/query deadlines return 504. `/healthz` is a process liveness endpoint, not a dependency readiness check.

Ports: Nginx `18080`, replicas `18081–18083`, Prometheus `19090`, PostgreSQL `55439`, Redis `56389`. All published ports bind to loopback. The `stampede` database username/password are disposable local lab credentials. Stop only this project with `docker-compose down`.

```sh
MODE=naive docker-compose up -d
MODE=singleflight docker-compose up -d
MODE=lease docker-compose up -d
```

When recreating replicas individually, reload Nginx to refresh its resolved upstream addresses: `docker-compose exec -T lb nginx -s reload`. The benchmark runner does this automatically.

## Architecture

```mermaid
flowchart LR
    G[Constant arrival rate generator] --> L[Nginx]
    L --> A[Go replica 1]
    L --> B[Go replica 2]
    L --> C[Go replica 3]
    A & B & C --> R[(Redis data and leases)]
    A & B & C --> P[(PostgreSQL)]
    A & B & C --> M[Prometheus]
```

| Mode | On a cache miss |
| --- | --- |
| `naive` | Each request may query PostgreSQL, subject to the shared benchmark's configured limits. |
| `singleflight` | Recheck and refresh once per key per process. Three replicas can still issue three simultaneous refreshes. |
| `lease` | Local singleflight plus a Redis lease coordinates refreshes across replicas. |

Data lives in `data:<id>` and leases in `lease:<id>`. Acquisition uses `SET NX PX` with a cryptographically random ownership token. The winner rechecks the cache. Lua atomically checks ownership before publishing, and another Lua operation checks ownership before releasing. An expired owner cannot overwrite a new owner's data or delete its lease on the current Redis server.

Followers poll with exponential backoff and jitter capped at 100 ms. They can reacquire after an owner disappears, within their bounded shared refresh budget. Individual callers have separate bounded waits and cancellation; cancelling one caller does not cancel a shared refresh. Exhausted followers return an error instead of bypassing coordination. Shutdown drains HTTP requests and cancels bounded shared work.

## Configuration

Compose applies identical limits to every mode. Limits are **per replica**, so the default three-replica maximum is 48 PostgreSQL connections and 96 admitted database fallbacks, including pool waiters.

| Environment variable | Compose default | Purpose |
| --- | --- | --- |
| `MODE` | `lease` | `naive`, `singleflight`, or `lease` |
| `CACHE_TTL` | `60s` | Per-key data expiration |
| `LEASE_TTL` | `2s` | Ownership expiration; no renewal |
| `SIMULATED_DELAY` | `200ms` | Deliberate `pg_sleep` inside the PostgreSQL read |
| `QUERY_TIMEOUT` | `2s` | Pool acquisition plus query budget |
| `REFRESH_TIMEOUT` | `3s` | Shared refresh lifetime |
| `FOLLOWER_TIMEOUT` | `3s` | Individual coordinated caller wait budget |
| `MAX_DB_CONNS` | `16` | Maximum connections per replica |
| `MAX_CONCURRENT_DB` | `32` | Nonblocking database fallback admission cap |

Direct execution also accepts `DATABASE_URL`, `REDIS_ADDR`, `INSTANCE`, and `ADDR`; standalone defaults are in `DefaultConfig`. The Compose settings above are the benchmark settings. The benchmark records every effective value and applies its `--simulated-delay`, `--lease-ttl`, `--max-db-conns`, and `--max-concurrent-db` flags identically across modes. It overrides inherited environment settings for comparability.

The simulated delay occupies a database connection and widens the refill window reproducibly. These are local simulation measurements, not production PostgreSQL performance claims. Set `SIMULATED_DELAY=0s` to remove it from manual runs.

## Benchmark

```sh
go build -o bin/loadgen ./cmd/loadgen
python3 scripts/benchmark.py --rate 2000 --duration 12 --delete-at 4 --repeats 2 --failures --docker-loadgen --output results/my-run
```

`--docker-loadgen` runs the same Go generator inside the Compose network, avoiding host port-forwarding capacity limits. Omit it to measure through the published host endpoint instead; both paths record their URLs and environment. The main experiment warms a hot key, starts steady traffic, deletes only `data:1` around four seconds, and continues through recovery. It repeats each mode twice. Deletion timestamps are bracketed around the actual Redis command and recorded relative to the generator's start. No scenario uses `FLUSHALL`.

`--failures` additionally runs 2,000 distinct initially missing keys in every mode, a cold 20-key data cache with a preserved sentinel lease, a killed refresh owner verified through `pg_stat_activity`, a 75 ms lease with a 200 ms query, and Redis stopped during traffic. The owner-kill run uses an 800 ms simulated query so the runner can observe the active owner before killing it. These scenario changes are recorded individually.

Regenerate the comparison table with `python3 scripts/report.py --input results/my-run`.

Standalone load and chart commands:

```sh
bin/loadgen -url 'http://localhost:18080/items/{key}' -rate 2000 -duration 15s -keys 1 -output results/manual
python3 scripts/charts.py --input results/manual --deletion-second 4
```

Requests are scheduled against absolute timestamps, independently of response completion. The generator records offered traffic, actual dispatch, scheduling lag, completed responses, and drops at its configurable `-max-inflight` bound. Completed responses include errors; successful throughput must subtract them. Latency percentiles include error responses, so every chart includes errors, timeouts, and offered/completed rates. One-second DB QPS uses differences between measured replica counters. Missing scrapes and decreases in the summed counter are gaps, not zeros. A single-replica counter reset can be masked by increases elsewhere; inspect raw replica snapshots when diagnosing restarts.

Each run saves `requests.csv`, `series.csv`, `summary.json`, `config.json`, `events.json`, raw `metrics-*.prom` snapshots, service logs, and `charts.svg`. The parent folder saves environment details, image identifiers, and resolved Compose configuration. The final response drain is included in the reported completed rate.

## Measured results

The saved [2026-10-09 experiment](../results/20261009-2000rps) ran two 12-second hot-key trials per mode, with a 200 ms simulated PostgreSQL delay, 2 s leases, and identical limits. The generator ran inside a Colima VM with 2 CPUs and approximately 3.8 GiB RAM; 26 unrelated containers were also running. This is a shared local simulation environment.

Each trial offered and completed 24,000 requests with zero generator drops. Achieved completion rates, including final drain, ranged from 1,999.82 to 2,000.00 RPS. Completions include errors.

| Mode / repeat | DB starts after deletion | Errors | Overall p99 ms | Peak one-second p99 ms |
| --- | ---: | ---: | ---: | ---: |
| naive / 1 | 96 | 308 | 1.14 | 428.62 |
| naive / 2 | 96 | 309 | 1.32 | 379.25 |
| singleflight / 1 | 3 | 0 | 87.98 | 196.37 |
| singleflight / 2 | 3 | 0 | 87.23 | 195.47 |
| lease / 1 | 1 | 0 | 105.23 | 213.00 |
| lease / 2 | 1 | 0 | 138.69 | 268.60 |

Naive mode's low overall p99 is misleading in isolation: most requests hit cache, rejected requests fail quickly, and fewer than 1% of requests execute the slow database reads. The peak one-second latency and errors reveal the refill disturbance. Coordination protects PostgreSQL and avoids rejection here; it does not eliminate the wait for the deliberately slow read. Local singleflight cannot coordinate the three processes, which is why its refill count is three rather than one.

Charts: [naive](../results/20261009-2000rps/naive-1/charts.svg), [singleflight](../results/20261009-2000rps/singleflight-1/charts.svg), [lease](../results/20261009-2000rps/lease-1/charts.svg). The [comparison CSV](../results/20261009-2000rps/comparison.csv) includes every completed scenario, both repetitions, achieved and successful rates, errors, percentiles, and scheduling lag. Each scenario directory contains raw requests and Prometheus snapshots.

Earlier [interrupted](../results/20261009-interrupted/STATUS.txt) and [host-forwarded](../results/20261009-host-forwarding/STATUS.txt) attempts are retained separately. Host SSH forwarding failed with `poll: Invalid argument` during the short-lease workload; those failures are not service-performance findings. The final suite routes both workload and administrative HTTP traffic inside Docker. A temporary Unix-socket tunnel was used for Docker control without restarting the shared VM.

Additional measured scenarios:

| Scenario | Observed DB starts | Errors / 24,000 requests | Result |
| --- | ---: | ---: | --- |
| 2,000 distinct keys, naive | 2,000 | 7,074 | Capacity limits reject unrelated-key work |
| 2,000 distinct keys, singleflight | 2,000 | 7,078 | Local coordination does not reduce distinct reads |
| 2,000 distinct keys, lease | 2,000 | 7,034 | Per-key leases do not solve total-capacity overload |
| Cold 20-key data cache, lease | 20 | 0 | Sentinel lease preserved; all keys refilled |
| 75 ms lease / 200 ms query | 94 | 16,093 | 15,772 HTTP 504s and 321 client timeouts; publication repeatedly loses ownership |
| Owner killed during 800 ms query | Partial metrics | 98 | Owner app1 was observed querying and killed; a surviving replica refilled; proxy returned 98 HTTP 502s |
| Redis unavailable | 0 | 15,915 | HTTP 503s protect PostgreSQL by failing closed |

All 13 runs offered and dispatched 24,000 requests each with zero generator drops. Short-lease completion throughput was 1,821.67 RPS including a 13.17-second drain, despite maintaining the 2,000 RPS offered schedule. Its p99 was approximately 3 seconds. This is deadline failure, not generator throttling. The killed-owner run had approximately 2.71-second overall p99 and recovered before its end. The aggregate database chart has gaps while the killed replica cannot be scraped; raw snapshots show the surviving refresher's query counter, and `events.json` records the observed killed query. No total is fabricated for that missing interval.

Validation passed: formatting, `go vet ./...`, ordinary tests, and `go test -race ./...` against real Redis/PostgreSQL. All 13 SVG charts parse, all 312,000 raw request rows reconcile with summaries, and all three Prometheus targets were up after the final failures. The macOS race linker emitted an `LC_DYSYMTAB` warning; tests still passed. No integration tests ran alongside the final in-network suite.

The failed host-forwarding listeners were restored after the experiment without restarting Colima or unrelated containers. Normal Docker access and `http://localhost:18080/items/1` were verified afterward. Forwarding is a machine-specific concern; prefer `--docker-loadgen` for this Colima setup.

## Checks

```sh
gofmt -w cmd internal
go vet ./...
TEST_DATABASE_URL='postgres://stampede:stampede@127.0.0.1:55439/stampede?sslmode=disable' \
TEST_REDIS_ADDR=127.0.0.1:56389 go test ./... -count=1
TEST_DATABASE_URL='postgres://stampede:stampede@127.0.0.1:55439/stampede?sslmode=disable' \
TEST_REDIS_ADDR=127.0.0.1:56389 go test -race ./... -count=1
```

Integration tests use real Redis and PostgreSQL and explicitly skip if their environment variables are absent. Use dedicated lab dependencies: tests clear selected keys in the `9001–9012` range. Tests cover local suppression, separate-instance coordination, safe release and publication, stale and expired owners, follower timeout and recovery, Redis failure, caller cancellation, and database admission. Load-generator tests verify continued offers during slow responses and body-read timeout accounting.

Metrics include `stampede_db_query_starts_total`, `stampede_events_total{kind,outcome}`, `stampede_duration_seconds{operation}`, and `stampede_active{operation}`. Event kinds distinguish cache hit/miss/error, query starts/completions/errors, refresh owners, local and distributed followers, lease acquired/contended/error/expired outcomes, releases, HTTP statuses, and rejected database fallbacks. Duration histograms separate HTTP, PostgreSQL execution, and connection-pool wait. Query starts are counted at query submission after pool acquisition, rather than counting misses or rejected attempts. Cache counters count Redis lookups, including rechecks and polling. Distributed follower events count polling iterations; the active waiter gauge includes the first caller waiting for detached shared work. Generator timeout counts are client transport timeouts; service 504 responses are counted as HTTP errors and retain their status in raw requests.

## Failure behavior and limits

A Redis lease reduces duplicate reads under healthy coordination. Lease expiry or Redis failover can permit overlapping refreshes; there is no unconditional exactly-once guarantee. Ownership checks protect publication on the authoritative Redis instance, not arbitrary database writes or external side effects.

No lease renewal is implemented. If every query exceeds the lease, owners may repeatedly lose publication rights until deadlines expire. Redis unavailability fails closed in all modes: this protects PostgreSQL by sacrificing availability. Many distinct missing keys still require separate database reads; per-key coordination does not solve total-capacity overload. Equal connection and fallback limits keep that overload bounded.

There is no stale serving, negative cache, Redis Cluster support, authentication, TLS, distributed rate limiter, or replicated Redis failover experiment. Redis failover limitations are documented, not experimentally validated. The cold-data helper uses `KEYS` inside a Lua operation only for this small isolated lab; it is not a production cache-clearing recommendation. The generator keeps per-request observations in memory, so very long high-rate runs need a streaming aggregation design. See [architecture details](architecture.md) for protocol and measurement semantics.
