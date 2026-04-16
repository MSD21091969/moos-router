# moos-router

Federation gateway and HTTP reverse proxy for the moos ecosystem.
Routes rewrite and query requests to distributed kernel backends via
URN-prefix longest-match shard rules.

Go 1.23, stdlib only. Module: `moos/router`.

---

## Architecture

```
Incoming request
      │
      ├─ POST /rewrites or POST /programs
      │     Extract URN from JSON body → longest-prefix shard match → forward to one kernel
      │
      ├─ GET /state/nodes/{urn}
      │     Route by URN → try local shard first → cascade to peer routers on 404 (WF16)
      │
      ├─ GET /healthz
      │     Parallel health checks on all unique kernel backends
      │
      └─ All other paths (fan-out)
            Concurrent request to every unique backend → merge JSON arrays/objects
```

**Routing logic**: rules are sorted descending by prefix length, then by
Priority (desc), then alphabetically. First match wins.

**URN extraction** (POST body): checked in order — `node_urn`, `target_urn`,
`src_urn`, `relation_urn`. Handles both JSON objects and JSON arrays.

**Fan-out merge**: JSON arrays from each backend are concatenated. Partial
success (some kernels down) is allowed; all-fail returns 502.

**Peer cascade** (`--peer`): used only on `GET /state/nodes/{urn}` when the
local shard returns 404. Implements WF16 federation read path.

---

## Package Structure

```
internal/proxy/
  proxy.go       — Router struct, ShardRule, ServeHTTP dispatcher,
                   all routing / fan-out / health logic
  proxy_test.go  — 8 test functions (unit + integration via httptest)
cmd/router/
  main.go        — CLI entry point, flag parsing, wires ShardRules → Router
```

---

## Running

```bash
go run ./cmd/router \
  --listen :9000 \
  --shard "urn:moos:ws:hp-laptop=http://localhost:8000" \
  --shard "urn:moos:ws:hp-z440=http://localhost:8001" \
  --default "http://localhost:9001" \
  --peer "http://peer-router:9000"
```

Build a binary:

```bash
go build -o moos-router ./cmd/router
```

### CLI Flags

| Flag | Default | Purpose |
|------|---------|--------|
| `--listen` | `:9000` | Router listen address |
| `--shard` | (repeatable) | `urn_prefix=http://host:port` shard rule |
| `--default` | (none) | Fallback kernel URL when no prefix matches |
| `--health-timeout` | `2s` | Per-kernel health check timeout |
| `--peer` | (repeatable) | Peer router URL for WF16 federation cascade |

`--shard` format: `urn_prefix=http://host:port`. Multiple `--shard` flags are
allowed. The `--default` rule gets Priority=-1 and an empty prefix, ensuring
it is always matched last.

---

## Testing

```bash
go test ./internal/proxy   # or go test ./...
```

8 test functions in `proxy_test.go`:

| Test | What it covers |
|------|---------------|
| `TestRoute_LongestPrefixMatch` | Longer prefix wins over shorter |
| `TestRoute_PriorityTiebreak` | Priority breaks ties at equal prefix length |
| `TestRoute_NoMatch` | Returns "" when no rule matches |
| `TestServeHTTP_PostRewrites_Routed` | POST /rewrites routed to correct shard only |
| `TestServeHTTP_FanOut_StateNodes` | GET fans out to all kernels and merges |
| `TestServeHTTP_FanOut_StateNodesPartialSuccess` | Partial failure still returns surviving data |
| `TestServeHTTP_FanOut_AllFailures` | All-fail returns 502 |
| `TestServeHTTP_Healthz` | Health check hits all kernels |
| `TestServeHTTP_Healthz_UsesConfiguredTimeout` | Timeout causes kernel to report "down" |

All tests use `net/http/httptest` — no real network required.

---

## Go Conventions

- **No external dependencies** — `go.mod` has no require block.
- `Router` is an `http.Handler` — wire directly into `http.ListenAndServe`.
- Fan-out goroutines use `sync.WaitGroup` + buffered channel.
- Health checks use `context.WithTimeout`.
- The router is **stateless** — no graph state, no rewrite validation.

---

## Ecosystem Integration

```
Client / moos-viz / agent
        │
   moos-router :9000
        │
   ┌────┴───────┐
kernel :8000  kernel :8001
(hp-laptop)   (hp-z440)
```

Shard rules use the same URN namespace as the kernels (`urn:moos:ws:<workstation>`).
WF16 (Federation) in the ontology describes the topology that routers and kernels form.
`GET /healthz` on the router aggregates `GET /healthz` from all kernel shards.

---

## Safety

- The router has no authentication layer — run behind a trusted network boundary.
- Never embed credentials in `--shard` or `--peer` URLs in committed config.
- `ffs0/` is the workspace parent — ontology and running state live there, not here.
