# llm-conduit Design Document

## 1. Overview

`llm-conduit` is a lightweight, single-binary AI model gateway with optional TLS. It exposes one unified OpenAI-compatible API, routes traffic to different upstream LLM services (vLLM, TGI, Ollama or any other OpenAI-compatible server) based on the model identifier in the request, and maps the caller's key to the matching upstream key. The design goals are simple deployment on ARM Linux, constant memory usage, and streaming support.

## 2. Goals and Non-Goals

**Goals**
- Single binary, no runtime dependencies, supports ARM64 / x86_64 Linux.
- Full support for standard OpenAI API requests: callers change only `base_url` and `api_key`, never application code.
- One entry point: a single key and a single port in front of many upstreams.
- Key mapping: clients hold a unified key, the gateway replaces it with the upstream key.
- Routing from the `X-Model` header or the `model` field of the JSON body, with optional `model` rewriting.
- Stream both the request and the response body; for long conversations memory is proportional to the request body size, not to the number of concurrent requests.
- Optional server-side TLS, optional upstream HTTPS.
- Config-file driven and easy to extend.

**Non-Goals**
- No sophisticated load balancing, dynamic service discovery, or rate limiting/quotas (possible future extensions).
- Does not parse the full JSON message array; only the top-level `model` field is extracted.
- No admin UI.

## 3. Architecture

```
Client ──HTTP/HTTPS──▶ llm-conduit ──HTTP/HTTPS──▶ Upstream A (:8000)
   │                        │
   │                        └──HTTP/HTTPS──▶ Upstream B (:8001)
   │
   └─ Authorization: Bearer <unified key>
                              │
                              └─ replaced by "Bearer <upstream key>" when forwarding
```

**Components**
- **Listener**: listens on the configured address and port, optionally terminates TLS.
- **Auth**: validates the caller's unified key, returns 401 on failure.
- **Router**: extracts the model identifier and matches an upstream.
- **KeyMapper**: replaces the caller's key with the upstream key.
- **ModelRewriter**: optional, rewrites the `model` field of the body to match the upstream.
- **Proxy**: streams the request and response bodies, filters hop-by-hop headers.
- **Config**: TOML file defining the server, auth, upstreams and TLS.

**Data flow**
1. Accept the client request.
2. Validate the client's unified key.
3. Read the request body, extract `model` (or `X-Model`).
4. Match an upstream and build the target URL.
5. Optionally rewrite the `model` field of the body.
6. Drop the client `Authorization` header and inject the upstream key.
7. Stream the request body to the upstream.
8. Stream the upstream response back to the client.

## 4. Standard OpenAI API Support

### 4.1 Supported endpoints

Every endpoint is **passed through**: the gateway only routes, authenticates and maps keys. It never changes request or response formats.

| Endpoint | Method | Purpose | Routing input |
| :--- | :--- | :--- | :--- |
| `/v1/chat/completions` | POST | chat completion | body `model` or `X-Model` |
| `/v1/completions` | POST | text completion | body `model` or `X-Model` |
| `/v1/embeddings` | POST | embeddings | body `model` or `X-Model` |
| `/v1/models` | GET | model list, aggregated by the gateway | answered locally, see 12.4 |
| `/v1/models/{model}` | GET | model details | path parameter |
| other `/v1/*` | any | pass through | body `model` or `X-Model` |

### 4.2 Auth compatibility

Clients use the standard OpenAI-style `Authorization: Bearer <key>`. The gateway validates the unified key and swaps in the upstream key when forwarding. Callers change nothing but `base_url` and `api_key`.

```python
from openai import OpenAI

client = OpenAI(
    base_url="https://gateway:4000/v1",
    api_key="sk-gateway-0001"        # unified key
)

resp = client.chat.completions.create(
    model="llama-3.1-8b-instruct",   # routes to upstream A
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True
)
```

### 4.3 Routing on `model` and rewriting it

In a standard OpenAI request the `model` field has two roles:
- **the gateway routes on it**;
- **the upstream may validate it** (for example vLLM's `--served-model-name`).

If the routing name differs from the upstream `served_model_name`, passing it through verbatim makes the upstream fail. The `upstream_model` setting solves this:

- **`upstream_model` not configured**: the original body is passed through, which requires the routing name to equal the upstream `served_model_name`.
- **`upstream_model` configured**: the JSON is parsed, the `model` field is rewritten, and the result is forwarded.

**Memory trade-off**: rewriting `model` means parsing and re-serializing JSON, which for long conversations allocates memory proportional to the body size. Keeping the routing name equal to the upstream `served_model_name` avoids it and is the preferred approach.

## 5. Configuration Design

The config file is `config.toml`:

```toml
[server]
listen = "0.0.0.0:4000"
default_upstream = "llama-3.1-8b-instruct"   # optional, fallback upstream when no model is given
max_body_bytes = 200000000                   # optional, default 200MB, larger bodies get a 413

# Optional: upstream probing behind GET /readyz; these are the defaults
[server.health]
enabled = true
interval_secs = 10
timeout_secs = 2
mode = "models"                              # "models" probes GET /v1/models, "tcp" only connects

# Optional: server-side TLS. Without it the gateway only speaks HTTP
[server.tls]
cert_path = "/etc/llm-conduit/cert.pem"
key_path  = "/etc/llm-conduit/key.pem"

# Optional: caller authentication
[auth]
enabled = true
# Unified keys: key = "caller description"
keys = { "sk-gateway-0001" = "frontend", "sk-gateway-0002" = "backend" }

# Upstreams (routing name = the model clients request)
[upstreams."llama-3.1-8b-instruct"]
base_url = "http://127.0.0.1:8000"
api_key  = "upstream-key-0001"                       # optional, injected when forwarding
upstream_model = "meta-llama/Llama-3.1-8B-Instruct"  # optional, rewrites the model field when forwarding
insecure_skip_verify = false                         # optional, only relevant for upstream HTTPS

[upstreams.mistral-7b-instruct]
base_url = "https://127.0.0.1:8001"
api_key  = "upstream-key-0002"
insecure_skip_verify = false
```

**Field reference**

| Field | Required | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.listen` | yes | — | Listen address; also used when TLS is enabled |
| `server.default_upstream` | no | none | Fallback upstream when no model is specified |
| `server.max_body_bytes` | no | 200MB | Request body limit; larger requests get a 413 |
| `server.tls` | no | none | Present enables server-side HTTPS |
| `server.health.enabled` | no | true | Probe upstreams in the background for `/readyz` |
| `server.health.interval_secs` | no | 10 | Seconds between two probe passes |
| `server.health.timeout_secs` | no | 2 | Timeout of a single probe |
| `server.health.mode` | no | `models` | `models` needs a successful `GET /v1/models`, `tcp` only opens a connection |
| `auth.enabled` | no | false | Enable caller authentication |
| `auth.keys` | no | empty | Mapping from unified key to description |
| `upstreams.<name>.base_url` | yes | — | Upstream root address, may include a path prefix |
| `upstreams.<name>.api_key` | no | none | Bearer token injected when forwarding |
| `upstreams.<name>.upstream_model` | no | none | Value written into the `model` field when forwarding |
| `upstreams.<name>.insecure_skip_verify` | no | false | Skip certificate verification for upstream HTTPS |

> **Upstream naming**: `<name>` in `[upstreams.<name>]` is the `model` value clients send.
> TOML dots separate levels, so **a name containing `.` must be quoted**:
> `[upstreams."llama-3.1-8b-instruct"]` (without quotes it is parsed as a sub-table of `upstreams.llama-3`
> and startup fails). Unknown fields, an unsupported scheme, a `default_upstream` that does not exist,
> `auth.enabled` with an empty `keys` map, and similar mistakes all abort startup with an error instead
> of running in a broken state.

## 6. Request Handling Flow

```
1. Accept the request
       │
       ▼
2. Validate the unified key (when auth.enabled)
       │  failure → 401
       ▼
3. Read the request body (Bytes); over the limit → 413
       │
       ▼
4. Resolve the model identifier (priority order)
       │  ├─ X-Model header
       │  ├─ top-level model field of the JSON body
       │  └─ server.default_upstream
       │  all missing → 400
       ▼
5. Match an upstream
       │  not found → 404
       ▼
6. If upstream_model is configured and differs from the request:
       │  parse the JSON, rewrite the model field, re-serialize
       │  otherwise pass the original body through
       ▼
7. Build the upstream request
       │  ├─ keep path + query
       │  ├─ copy every header except hop-by-hop ones
       │  └─ drop the client Authorization, inject the upstream api_key
       ▼
8. Forward upstream and stream the response back
```

**Step details**

**Step 2: authentication**
- Look at the `Authorization: Bearer <key>` or `X-API-Key` header.
- Look the key up in `auth.keys`; matching keys pass, anything else is a 401.
- Log the description attached to the key for auditing.

**Step 4: model extraction**
- Prefer the `X-Model` header, which avoids parsing JSON at all.
- Otherwise parse the top-level `model` field, letting serde stream past large arrays such as `messages`; the only extra memory is the `model` string.
- Fall back to `default_upstream` last.

**Step 6: model rewriting**
- Only performed when `upstream_model` is configured and differs from the original value.
- Parse the full JSON → replace the `model` field → re-serialize.
- For long conversations this allocates memory proportional to the body size, so avoid it when possible.

**Step 7: key mapping**
- Skip `Authorization` and `X-API-Key` while iterating the original headers.
- If the upstream has an `api_key`, inject `Authorization: Bearer <api_key>`.
- If it does not, inject nothing and let the upstream treat the request as unauthenticated.

## 7. Routing Strategy

| Priority | Source | Notes |
| :--- | :--- | :--- |
| 1 | `X-Model` header | Best performance, the gateway never parses the body |
| 2 | `model` field of the JSON body | Standard OpenAI format, most compatible |
| 3 | `server.default_upstream` | Fallback |

**Path-prefix routing (optional extension)**: `/v1/llama/chat/completions` routes to `llama-3.1-8b-instruct`, `/v1/mistral/chat/completions` routes to `mistral-7b-instruct`.

## 8. Streaming and Memory

- The request body is kept as a single `Bytes`; extracting `model` makes serde stream past every other field.
- The response body is wrapped in `BoxBody` and forwarded as it arrives, never buffered as a whole.
- Upstream connections are reused through the `hyper-util` client connection pool.
- Memory usage ≈ request body size + fixed overhead, and does not grow linearly with concurrency.
- **Model rewriting is the only path that amplifies memory**; keep the routing name consistent to avoid it.

## 9. TLS Design

### 9.1 Server-side TLS

- When `[server.tls]` is configured with a valid certificate, `tokio-rustls` wraps the `TcpStream`.
- PEM certificate chains and private keys are supported.
- Without TLS the gateway serves plain HTTP on a plain `TcpListener`.

**Dependencies**
```toml
tokio-rustls = "0.26"
rustls-pemfile = "2"
```

### 9.2 Upstream TLS

- When an upstream `base_url` uses `https://`, the `HttpsConnector` from `hyper-rustls` is used.
- System root certificates are used by default.
- With `insecure_skip_verify = true` a custom `ServerCertVerifier` skips verification (test environments only).

### 9.3 Certificate loading

- Certificates and private keys are read at startup; failures exit with a printed error.
- Private keys may be PKCS#8, RSA or EC.

## 10. Key Mapping Design

### 10.1 Two layers of keys

| Layer | Held by | Purpose |
| :--- | :--- | :--- |
| **Unified key** | Callers | Authentication, auditing, (future) rate limiting |
| **Upstream key** | The gateway | Credentials used to reach upstreams |

### 10.2 Rules

1. The client request carries the unified key, which the gateway validates.
2. The client's `Authorization` and `X-API-Key` headers are dropped before forwarding.
3. If the upstream has an `api_key`, `Authorization: Bearer <api_key>` is injected.
4. If it does not, nothing is injected.

### 10.3 Benefits

- **Decoupling**: callers never know upstream keys, so model switches, scaling and key rotation stay invisible to them.
- **Containment**: key exposure shrinks from N callers to a single gateway.
- **Auditability**: logs record the description of the caller key, never the upstream key.

## 11. Error Handling and Logging

- Every error is returned as OpenAI-style JSON:
  ```json
  {"error":{"message":"...","type":"gateway_error"}}
  ```
- Logs go to stderr, one line per request, with the fields listed in 12.5 (request ID, caller, model, upstream, status, latency, byte counts, streaming flag).
- The log level is controlled by the `RUST_LOG` environment variable.

**Main error codes**

| Scenario | Status code |
| :--- | :--- |
| Missing or invalid unified key | 401 |
| No model specified | 400 |
| Unknown model | 404 |
| Request body over the limit | 413 |
| Upstream error | 502 |
| Internal error | 500 |

## 12. Health, Readiness and Observability

### 12.1 Endpoints answered by the gateway

| Endpoint | Status | Behaviour |
| :--- | :--- | :--- |
| `GET /healthz` | always `200` | The process is alive and serving; never depends on an upstream. |
| `GET /readyz` | `200` / `503` | `200` only when every upstream passed its last probe, `503` otherwise. |
| `GET /v1/models` | `200` | Aggregated from the configuration (see 12.4). |

Both health endpoints are answered *before* authentication, are never routed to an upstream and
are not written to the request log, because orchestrators poll them every few seconds. They
are the only paths the gateway answers itself besides `/v1/models`.

A `/readyz` body names every upstream, so an operator can see what is wrong:

```json
{"probing":"enabled","status":"degraded","upstreams":{"primary":{"checked_secs_ago":2,"error":"GET /v1/models returned 503","status":"down"},"secondary":{"latency_ms":3,"checked_secs_ago":2,"status":"up"}}}
```

An upstream that has not been probed yet counts as ready, so turning probing on does not make
the gateway unready during the first interval.

### 12.2 Upstream probing

A background task probes every upstream concurrently, controlled by `server.health` (section 5):

- `mode = "models"` (default) requires a successful `GET /v1/models`. This is the functional
  check, and it also catches wrong credentials, because a `401` counts as unhealthy.
- `mode = "tcp"` only opens a connection. Use it for upstreams that do not serve `/v1/models`.

Probe results only drive `/readyz`. Routing is deliberately unaffected: a configured upstream
keeps receiving requests while it is marked unhealthy, and the caller sees the upstream's own
error rather than a gateway invented one.

State *changes* are logged (`upstream is healthy` / `upstream is unhealthy`); the steady state
is logged at debug level so a long outage does not add one line per interval.

### 12.3 Request IDs

Every request carries an `X-Request-ID`:

- a caller supplied value is reused, unless it is empty, non ASCII, longer than 128 characters
  or contains control characters;
- otherwise a random UUID v4 is generated;
- it is forwarded to the upstream, so gateway and upstream logs can be correlated;
- it is echoed back in the response headers, including for error responses.

### 12.4 `/v1/models`

The gateway answers `GET /v1/models` itself, with one entry per configured upstream, so an SDK
calling `client.models.list()` only ever sees names the gateway can actually route:

```json
{"object":"list","data":[{"id":"llama-3.1-8b-instruct","object":"model","created":1757940000,"owned_by":"llm-conduit"}]}
```

The body is built once at startup, so it costs nothing per request. Like every other `/v1`
endpoint it requires the caller's unified key. `GET /v1/models/{model}` is not intercepted and
is still passed through (section 4.1).

### 12.5 Structured logs

One line per request is written to stderr when the response body is done — finished or aborted —
so the byte counts and the latency are the real ones. A long streaming request therefore logs
when the stream ends, not when it starts.

```
INFO request completed request_id=5f8c1b2a-…-… caller=frontend model=llama-3.1-8b-instruct upstream=primary status=200 elapsed_ms=1832 request_bytes=412 response_bytes=20481 stream=true aborted=false method=POST path=/v1/chat/completions error=-
```

| Field | Meaning |
| :--- | :--- |
| `request_id` | Request ID, see 12.3 |
| `caller` | Description of the unified key; `-` when auth is disabled |
| `model` | Routing name the request resolved to; `-` for `/v1/models` and for failures before routing |
| `upstream` | Upstream that served the request |
| `status` | HTTP status returned to the client |
| `elapsed_ms` | From receiving the request to finishing the response body |
| `request_bytes` | Size of the request body |
| `response_bytes` | Bytes forwarded to the client |
| `stream` | `true` for server-sent event responses |
| `aborted` | `true` when the client disconnected before the stream ended |
| `method`, `path` | Request line |
| `error` | Gateway error message; `-` on success |

`4xx`/`5xx` responses and aborted streams log at `WARN`, everything else at `INFO`.

## 13. Deployment and Builds

**Command line**

```
llm-conduit [OPTIONS] [CONFIG]

Arguments:
  [CONFIG]             path to the TOML config file, default config.toml, overridable via LLM_CONDUIT_CONFIG

Options:
  -c, --config <PATH>  path to the config file (wins over CONFIG/the environment variable)
  -h, --help           help (-h is the short form, --help the long form)
  -V, --version        version
```

Exit codes: help and version go to stdout and exit with 0; usage errors go to stderr and exit with 2;
startup failures (unparsable config, unusable certificate, port already in use, ...) go to stderr and exit with 1.

**Native build (on the ARM machine)**
```bash
cargo build --release
./target/release/llm-conduit config.toml
```

**Cross-compiled static binary (x86 → ARM64)**
```bash
rustup target add aarch64-unknown-linux-musl
cargo build --release --target aarch64-unknown-linux-musl
```
Artifact: `target/aarch64-unknown-linux-musl/release/llm-conduit`, roughly 3–5 MB, no glibc dependency.

**Docker (optional)**
```dockerfile
FROM scratch
COPY llm-conduit /
COPY config.toml /
ENTRYPOINT ["/llm-conduit", "/config.toml"]
```

**systemd unit example**
```ini
[Unit]
Description=llm-conduit AI Gateway
After=network.target

[Service]
ExecStart=/usr/local/bin/llm-conduit /etc/llm-conduit/config.toml
Restart=on-failure
Environment=RUST_LOG=info

[Install]
WantedBy=multi-user.target
```

## 14. Usage Examples

**Option 1: `X-Model` header (recommended)**
```bash
curl https://gateway:4000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-gateway-0001" \
  -H "X-Model: mistral-7b-instruct" \
  -d '{"messages":[{"role":"user","content":"Hello!"}],"stream":true}'
```

**Option 2: standard OpenAI format**
```bash
curl https://gateway:4000/v1/chat/completions \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer sk-gateway-0002" \
  -d '{"model":"llama-3.1-8b-instruct","messages":[{"role":"user","content":"Hello!"}],"stream":true}'
```

**Option 3: OpenAI Python SDK**
```python
from openai import OpenAI

client = OpenAI(
    base_url="https://gateway:4000/v1",
    api_key="sk-gateway-0002"
)

# routes to upstream A
resp = client.chat.completions.create(
    model="llama-3.1-8b-instruct",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True
)

# routes to upstream B
resp = client.chat.completions.create(
    model="mistral-7b-instruct",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True
)
```

## 15. Future Extensions

- Active failover: skip upstreams that are marked unhealthy when routing.
- Simple circuit breaking: temporarily skip an upstream after N consecutive failures.
- Per-key rate limiting, quotas and billing.
- Request logging to disk or OpenTelemetry integration.
- Multi-prefix routing.
- Config file hot reload.
- `GET /v1/models/{model}` served locally, like the list endpoint.

## 16. Appendix: Complete Config Example

```toml
[server]
listen = "0.0.0.0:4000"
default_upstream = "llama-3.1-8b-instruct"
max_body_bytes = 200000000

[server.tls]
cert_path = "/etc/llm-conduit/fullchain.pem"
key_path  = "/etc/llm-conduit/privkey.pem"

[auth]
enabled = true
keys = { "sk-gateway-0001" = "frontend", "sk-gateway-0002" = "backend" }

[upstreams."llama-3.1-8b-instruct"]
base_url = "http://127.0.0.1:8000"
api_key  = "upstream-key-0001"
upstream_model = "meta-llama/Llama-3.1-8B-Instruct"

[upstreams.mistral-7b-instruct]
base_url = "http://127.0.0.1:8001"
api_key  = "upstream-key-0002"
```

---

**Naming conventions**
- Project name: `llm-conduit`
- Binary name: `llm-conduit`
- Config file: `config.toml`
- Default port: `4000`
- Environment variables: `RUST_LOG`, `LLM_CONDUIT_CONFIG` (optional, overrides the config file path)
