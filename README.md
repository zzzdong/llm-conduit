# llm-conduit

[![CI](https://github.com/zzzdong/llm-conduit/actions/workflows/ci.yml/badge.svg)](https://github.com/zzzdong/llm-conduit/actions/workflows/ci.yml)
[![Release](https://img.shields.io/github/v/release/zzzdong/llm-conduit?sort=semver)](https://github.com/zzzdong/llm-conduit/releases/latest)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](LICENSE)

A lightweight, single-binary, optionally TLS-enabled AI model gateway. It exposes one
OpenAI-compatible API, routes every request to a different upstream LLM service based on the
model name, and swaps the caller's unified API key for the matching upstream key.

```
Client ──HTTP/HTTPS──▶ llm-conduit ──HTTP/HTTPS──▶ Upstream A (:8000)
   │                        │
   │                        └──HTTP/HTTPS──▶ Upstream B (:8001)
   └─ Authorization: Bearer <unified key>  →  Bearer <upstream key>
```

- **One endpoint, many upstreams** — clients keep a single `base_url` and `api_key`; no application changes.
- **Model-based routing** — `X-Model` header, the top-level `model` field of the body, or a configured default.
- **Key mapping** — callers never hold upstream credentials, so rotation stays invisible to them.
- **Streaming end to end** — memory is proportional to a single request body, not to the number of concurrent requests.
- **Small and static** — ~6 MB static binaries for `x86_64` and `aarch64` Linux (musl), no runtime dependencies.
- **Optional TLS** — server-side TLS termination, optional upstream HTTPS (including self-signed).
- **Health and readiness** — `GET /healthz` and `GET /readyz` backed by background upstream probing.
- **Aggregated models** — `GET /v1/models` lists every routable name, so `client.models.list()` works.
- **Traceable** — `X-Request-ID` in, forwarded upstream and echoed back, plus one structured log line per request.

The full design rationale is in [`docs/DESIGN.md`](docs/DESIGN.md).

## Quick start

**1. Get the binary.** Download a tarball from
[Releases](https://github.com/zzzdong/llm-conduit/releases/latest), or build it:

```bash
cargo build --release                                        # native (glibc)
cargo build --release --target x86_64-unknown-linux-musl     # static musl, see Development below
```

The tarballs contain the binary plus `config.example.toml`:

```bash
tar -xzf llm-conduit-0.2.0-aarch64-unknown-linux-musl.tar.gz
cd llm-conduit-0.2.0-aarch64-unknown-linux-musl
```

**2. Write a config.** A minimal `config.toml`:

```toml
[server]
listen = "0.0.0.0:4000"

[auth]
enabled = true
keys = { "sk-gateway-0001" = "frontend" }

[upstreams."llama-3.1-8b-instruct"]
base_url = "http://127.0.0.1:8000"
api_key = "upstream-key-0001"
```

Every option is documented in [`config.example.toml`](config.example.toml).

**3. Run it.**

```bash
RUST_LOG=info ./llm-conduit config.toml
```

**4. Point any OpenAI client at it** — only `base_url` and `api_key` change:

```python
from openai import OpenAI

client = OpenAI(base_url="http://127.0.0.1:4000/v1", api_key="sk-gateway-0001")

resp = client.chat.completions.create(
    model="llama-3.1-8b-instruct",
    messages=[{"role": "user", "content": "Hello!"}],
    stream=True,
)
```

## Routing

| Priority | Source | Notes |
| :--- | :--- | :--- |
| 1 | `X-Model` header | Fastest: the gateway does not parse the body at all |
| 2 | `model` field of the body | Standard OpenAI format, most compatible |
| 3 | `server.default_upstream` | Fallback |

The routing name must match the name of an `[upstreams.<name>]` section. Anything else is a `404`.

## Model rewriting (`upstream_model`)

The `model` field is used both for routing and, possibly, for validation by the upstream
(vLLM's `--served-model-name`). Set `upstream_model` when the two differ:

```toml
[upstreams."llama-3.1-8b-instruct"]
base_url = "http://127.0.0.1:8000"
upstream_model = "meta-llama/Llama-3.1-8B-Instruct"   # body model is rewritten to this
```

Without it the original body is passed through untouched, which is cheaper: rewriting parses
and re-serializes the JSON, so the allocation grows with the body size. Prefer keeping the
routing name equal to the upstream `served_model_name`.

## Configuration reference

| Field | Required | Default | Description |
| :--- | :--- | :--- | :--- |
| `server.listen` | yes | — | Listen address, e.g. `0.0.0.0:4000` |
| `server.default_upstream` | no | none | Fallback upstream when no model is specified |
| `server.max_body_bytes` | no | `200000000` | Request body limit; larger requests get a `413` |
| `server.tls.cert_path` / `key_path` | no | none | PEM certificate chain and private key; enables server-side HTTPS |
| `server.health.enabled` | no | `true` | Probe upstreams in the background for `/readyz` |
| `server.health.interval_secs` | no | `10` | Seconds between two probe passes |
| `server.health.timeout_secs` | no | `2` | Timeout of a single probe |
| `server.health.mode` | no | `models` | `models` (`GET /v1/models` must succeed) or `tcp` (connect only) |
| `auth.enabled` | no | `false` | Require a unified key |
| `auth.keys` | no | empty | Table of `key = "caller description"` (the description is logged) |
| `upstreams.<name>.base_url` | yes | — | Upstream root address, may include a path prefix |
| `upstreams.<name>.api_key` | no | none | Bearer token injected when forwarding |
| `upstreams.<name>.upstream_model` | no | none | Value written into the `model` field when forwarding |
| `upstreams.<name>.insecure_skip_verify` | no | `false` | Skip certificate verification for upstream HTTPS |

> A dot is TOML's level separator, so an upstream name containing one must be quoted:
> `[upstreams."llama-3.1-8b-instruct"]`.
>
> Configuration errors are fatal at startup: unknown fields, a bad scheme, an undefined
> `default_upstream`, or `auth.enabled` with an empty `keys` table all abort with a message.

## Command line

```
llm-conduit [OPTIONS] [CONFIG]

  [CONFIG]             path to the TOML config file (default: config.toml)
  -c, --config <PATH>  path to the config file, overrides CONFIG
  -h, --help           help
  -V, --version        version
```

The config path can also come from the `LLM_CONDUIT_CONFIG` environment variable. Help and
version exit `0`, usage errors exit `2`, and startup failures exit `1`. Logs go to stderr and
are controlled by `RUST_LOG` (`RUST_LOG=llm_conduit=debug` for verbose output).

## Error responses

Errors are returned in OpenAI's shape, so existing clients display them correctly:

```json
{"error":{"message":"unknown model 'ghost': no upstream with that name is configured","type":"gateway_error"}}
```

| Scenario | Status |
| :--- | :--- |
| Missing or invalid unified key | `401` |
| No model specified | `400` |
| Unknown model | `404` |
| Request body over the limit | `413` |
| Upstream unreachable | `502` |
| Internal error | `500` |

Upstream responses are passed through untouched, including their status code and body.

## Health and readiness

| Endpoint | Response |
| :--- | :--- |
| `GET /healthz` | Always `200`: the process is alive. Never touches an upstream. |
| `GET /readyz` | `200` when every upstream passed its last probe, `503` otherwise. |
| `GET /v1/models` | Every configured upstream, in OpenAI's format, so `client.models.list()` works. |

Neither health endpoint needs a key, and neither is written to the request log, because
orchestrators poll them constantly. `/readyz` names the upstream that is in trouble:

```json
{"status":"degraded","probing":"enabled","upstreams":{"primary":{"status":"down","error":"GET /v1/models returned 503","checked_secs_ago":2}}}
```

Upstreams are probed in the background — by default every 10 s with a 2 s timeout:

```toml
[server.health]
enabled = true
interval_secs = 10
timeout_secs = 2
mode = "models"    # "models" needs a successful GET /v1/models; "tcp" only opens a connection
```

Probes only drive `/readyz`: routing is unaffected, so a request for an upstream that is marked
down is still forwarded and returns that upstream's own error. `enabled = false` makes
`/readyz` always ready.

## Request IDs and logs

Every request carries an `X-Request-ID`: a caller supplied value is reused, otherwise a UUID v4
is generated. It is forwarded to the upstream and echoed back, so gateway and upstream logs line
up. One line per request is written once the response body is done:

```
INFO request completed request_id=5f8c1b2a-…-… caller=frontend model=llama-3.1-8b-instruct upstream=primary status=200 elapsed_ms=1832 request_bytes=412 response_bytes=20481 stream=true aborted=false effort=high max_tokens=1024 requested_choices=- prompt_tokens=412 completion_tokens=128 total_tokens=540 reasoning_tokens=64 cached_tokens=- method=POST path=/v1/chat/completions error=-
```

`request_bytes` and `response_bytes` are the sizes actually transferred, `stream` marks
server-sent event responses and `aborted` marks a client that disconnected mid-stream.
`4xx`/`5xx` responses and aborted requests are logged at `WARN`.

### Thinking strength and token usage

`effort` is the thinking strength the caller asked for, read from the request body. The first
field present wins, so all the usual spellings work:

| Key | Note |
| :--- | :--- |
| `reasoning_effort` | OpenAI's own field |
| `thinking` | e.g. `"off"` / `"low"` / `"high"`, or `{"type": "high"}` |
| `reasoning` | `{"reasoning": {"effort": "…"}}` |
| `thinking_effort` | Alias |

The token counters come from the upstream's `usage` object and are read while the response is
forwarded, so `prompt_tokens` / `completion_tokens` / `total_tokens`, plus the optional
`reasoning_tokens` and `cached_tokens`, cost nothing extra. Streamed responses work too: the
gateway scans every server-sent event, joins one that is split across two writes, and logs the
most complete counters it saw — the ones from the final chunk.

`max_tokens` is the budget the caller requested and `requested_choices` is `n`. Anything the
upstream did not report is logged as `-`, so the fields of a line never shift.

## Deployment

Static musl binaries run on any Linux distribution. A systemd unit:

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

`SIGTERM` and `Ctrl-C` trigger a graceful shutdown: the listener stops accepting new
connections and in-flight requests (including streaming ones) are allowed to finish.

## Releases

Tagging `v<version>` (matching the version in `Cargo.toml`) builds and publishes:

| Artifact | Target |
| :--- | :--- |
| `llm-conduit-<version>-aarch64-unknown-linux-musl.tar.gz` | ARM64 Linux, static |
| `llm-conduit-<version>-x86_64-unknown-linux-musl.tar.gz` | x86_64 Linux, static |

Each tarball ships with a `.sha256` file, and a combined `SHA256SUMS` is attached to the
release:

```bash
sha256sum -c SHA256SUMS
```

The release pipeline can also be triggered manually from the Actions tab to verify a build
without publishing anything.

## Development

```bash
cargo fmt --all --check
cargo clippy --all-targets -- -D warnings
cargo test
```

All three run in CI on every push and pull request.

Static musl builds are done per architecture, on a machine of that architecture: Ubuntu's
archives have no musl cross compiler, so `musl-tools` (plus `cmake`, which AWS-LC needs) is
all it takes:

```bash
rustup target add x86_64-unknown-linux-musl     # aarch64-unknown-linux-musl on ARM
sudo apt install musl-tools cmake
CC=musl-gcc cargo build --release --target x86_64-unknown-linux-musl
```

`CC` is what compiles the C dependency (AWS-LC, through CMake); the Rust linker is deliberately
left alone. Overriding `CARGO_TARGET_<TARGET>_LINKER` with a distribution `musl-gcc` turns the
link dynamic (`/lib/ld-musl-*.so.1`) instead of static, and that binary segfaults on the
`x86_64` target. The release workflow builds `x86_64-unknown-linux-musl` on `ubuntu-24.04` and
`aarch64-unknown-linux-musl` on `ubuntu-24.04-arm`.

## License

[MIT](LICENSE)
