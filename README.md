# tailgate

> Live P2P escape hatch for firewalled Jenkins: tail running builds and pull artifacts directly from the build node over iroh — no ingress, no VPN, no S3.

**Type**: RDD Spec (Type A) — agent implements from this document.
**Author**: Emmanuel Joliet (`ejoliet`) · **Date**: 2026-08-22 · **Status**: Draft
**License**: MIT

---

## Purpose

Jenkins controllers and agents commonly sit behind corporate NAT/VPN with no inbound access. Sharing a live build log or a large artifact with someone outside the network means screenshots, VPN onboarding, or copying to S3.

`tailgate` runs a small Rust sidecar next to Jenkins. For each build it mints an expiring **ticket**, posts it to Slack, and serves the log stream (iroh-gossip) and artifacts (iroh-blobs) directly from the build host over hole-punched QUIC. Anyone holding the ticket runs the `tailgate` CLI from anywhere — hotel wifi included — and connects peer-to-peer.

Who benefits: platform teams with air-gapped or on-prem Jenkins; anyone tired of "can you paste the log?"

## Architecture

```
┌─ Jenkins host (behind NAT) ─────────────────────────┐
│  Jenkins ◄── REST poll ── tailgated (sidecar)       │
│                             ├─ iroh Endpoint         │
│                             │   ├─ gossip: log lines │
│                             │   └─ blobs: artifacts  │
│                             ├─ ticket minter (Ed25519)│
│                             └─ Slack webhook poster  │
└──────────────────────────────────┬──────────────────┘
                 hole-punched QUIC │ (relay fallback)
┌─ Anywhere ───────────────────────▼──────────────────┐
│  tailgate CLI:  watch <ticket> │ pull <ticket>      │
└─────────────────────────────────────────────────────┘
```

Data flow:

1. `tailgated` polls Jenkins REST API; detects new build → mints ticket → posts Slack message.
2. During build: polls `logText/progressiveText`, broadcasts new lines on a per-build gossip topic.
3. On completion: imports archived artifacts into the blobs store, publishes a **manifest blob**, broadcasts a final `BuildFinished` gossip event carrying the manifest hash.
4. `tailgate watch` joins the gossip topic via the ticket and prints lines. `tailgate pull` fetches the manifest, then requested artifact blobs (BLAKE3-verified, resumable).

> 💡 Sidecar-over-REST, not a Jenkins plugin: works on any Jenkins ≥ 2.4xx, no Java, no plugin review cycle, no controller restart.

## Recommended Stack

| Layer | Chosen | Why | Rejected |
|-------|--------|-----|----------|
| P2P transport | `iroh` 1.0.x | 1.0 released; dial-by-key, hole-punching, relay fallback, QUIC streams ([docs.rs/iroh](https://docs.rs/crate/iroh/latest)) | libp2p (heavier, no batteries-included relays); raw QUIC (no NAT traversal) |
| Log pub-sub | `iroh-gossip` (1.0-rc-compatible line) | Per-topic epidemic broadcast; scales to many watchers without per-watcher streams ([docs.rs/iroh-gossip](https://docs.rs/iroh-gossip/latest/iroh_gossip/)) | Per-client bi-streams (O(n) sender load, no late-join relay between peers) |
| Artifact transfer | `iroh-blobs` | BLAKE3 content-addressed, verified streaming, KB→TB, resumable ([docs.rs/iroh-blobs](https://docs.rs/iroh-blobs/latest/iroh_blobs/)) | HTTP range over tunnel (needs a tunnel — the whole problem) |
| CLI | `clap` v4 (derive) | Standard | argh |
| Async | `tokio` | Required by iroh | — |
| Jenkins client | `reqwest` + hand-rolled REST calls | Only 3 endpoints needed | `jenkins-api` crates (stale) |
| Serialization | `postcard` (ticket wire), `serde_json` (manifest, config) | Compact deterministic bytes for signing | CBOR (fine, heavier dep) |
| Signing | `ed25519-dalek` v2 | Ticket signatures | — |
| HTTP mocking (tests) | `wiremock` | Async-native Jenkins/Slack mocks | httpmock |

> ⚠️ **Version pinning decision required**: docs.rs states the latest `iroh-blobs` line is "not yet considered production quality; use 0.35 for production" — but 0.35 pairs with pre-1.0 iroh. For a launch demo, pin the iroh-1.0-compatible releases of all three crates and record exact versions in `Cargo.lock` + burn log. Revisit before any "production" claim. (Open Question 1.)

One override round available on this table before implementation.

## Repository Layout

```
tailgate/
├── Cargo.toml              # workspace
├── crates/
│   ├── tailgate-core/      # ticket, manifest, topic derivation, shared types
│   │   └── src/{ticket.rs, manifest.rs, topic.rs, error.rs}
│   ├── tailgated/          # sidecar daemon
│   │   └── src/{main.rs, config.rs, jenkins.rs, watcher.rs,
│   │            publisher.rs, blobs.rs, slack.rs}
│   └── tailgate/           # client CLI
│       └── src/{main.rs, watch.rs, pull.rs}
├── examples/docker-demo/   # docker compose: jenkins + tailgated, for the demo GIF
├── README.md               # this file → later split into OSS README (Type C)
└── LICENSE                 # MIT
```

## Ticket Format (load-bearing design)

A ticket is the only credential. Possession + validity = access.

**Wire format**: `tg1_` + base32(lowercase, no padding) of `postcard`-serialized:

```rust
struct Ticket {
    version: u8,                  // = 1
    endpoint: EndpointAddr,       // iroh endpoint id + relay/direct addr hints
    build: BuildRef,              // { job_path: String, build_number: u32 }
    topic: [u8; 32],              // random per build; the gossip TopicId
    caps: u8,                     // bitflags: WATCH=1, PULL=2
    expires_at: u64,              // unix seconds
    sig: [u8; 64],                // Ed25519 over postcard(all prior fields)
}
```

Rules:

- `topic` is 32 random bytes generated per build — unguessable; knowledge of the ticket is the capability.
- Sidecar **verifies** `sig` (its own long-lived key) and `expires_at` before serving any gossip join or blob request; expired/invalid → connection closed with typed error.
- Default TTL: 24 h for `WATCH|PULL`, configurable per job.
- `tailgate mint` (runs on sidecar host, local socket auth) re-mints tickets for past builds still in the blob store.
- Manifest hash is **not** in the ticket (unknown at build start). It arrives via the `BuildFinished` gossip event and via a `GetManifest` request on a dedicated QUIC stream (`ALPN: tailgate/ctl/1`), so `pull` works without having watched.

**Manifest** (JSON blob in the store):

```json
{ "build": {"job_path": "roman/ssc-pipeline", "build_number": 412},
  "result": "SUCCESS",
  "artifacts": [ {"name": "dist/pkg.tar.gz", "hash": "blake3:…", "size": 12884901888} ] }
```

## Interface Contract

### CLI

```bash
tailgate watch <ticket> [--from-start] [--quiet-relay-warn]
tailgate pull  <ticket> [NAME…] [--out DIR] [--list]
tailgate mint  --job roman/ssc-pipeline --build 412 [--caps watch,pull] [--ttl 24h]
```

- `watch`: joins topic, prints lines to stdout (raw; suitable for `| grep`). Exits 0 on `BuildFinished{SUCCESS}`, 1 on failure result, 2 on ticket error.
- `pull --list`: prints manifest table. Without `NAME…` pulls all artifacts. Verified + resumable by iroh-blobs semantics.
- Progress/diagnostics (hole-punch vs relay path) go to stderr.

### Sidecar control stream (`ALPN: tailgate/ctl/1`)

| Request | Response |
|---------|----------|
| `GetManifest { ticket }` | `Manifest` JSON or typed error |
| `Ping` | `Pong { version }` |

### Gossip message schema (postcard enum, versioned)

```rust
enum LogEvent { Line { seq: u64, text: String },
                BuildFinished { result: String, manifest_hash: [u8;32] } }
```

`seq` enables gap detection; `watch` warns on stderr if lines were missed (gossip is best-effort; acceptable for v1).

## Configuration Reference (`tailgated`)

| Env var / TOML key | Type | Default | Required | Notes |
|---|---|---|---|---|
| `JENKINS_URL` | url | — | yes | e.g. `https://jenkins.internal:8080` |
| `JENKINS_USER` | string | — | yes | |
| `JENKINS_API_TOKEN` | secret | — | yes | env only, never in TOML |
| `TAILGATE_JOBS` | list | — | yes | job paths to watch, glob ok |
| `TAILGATE_KEY_FILE` | path | `~/.tailgate/key` | no | Ed25519 signing key; created if absent, `0600` |
| `TAILGATE_BLOB_DIR` | path | `~/.tailgate/blobs` | no | persistent fs store |
| `TAILGATE_TICKET_TTL` | duration | `24h` | no | |
| `TAILGATE_MAX_ARTIFACT_BYTES` | u64 | `50GiB` | no | skip + warn above |
| `SLACK_WEBHOOK_URL` | secret | — | no | omit → tickets logged to stdout only |
| `TAILGATE_POLL_INTERVAL` | duration | `2s` | no | progressive-log poll |

> ⚠️ `gitignore-before-keygen`: repo `.gitignore` must exclude `*.key`, `.tailgate/` **in the scaffold commit, before any key-generating code exists**. (cullroom lesson.)

## Slack Integration (v1)

Plain incoming webhook. Message per build start:

```
🔴 roman/ssc-pipeline #412 running on jenkins-big-executor
Watch:  tailgate watch tg1_ab3k…
Pull:   tailgate pull  tg1_ab3k…   (available when build finishes)
```

Follow-up on completion with result + artifact count/sizes. Interactive buttons (Bolt app) are a **non-goal for v1**.

## Error Handling

| Error class | Behavior |
|---|---|
| `TicketError::{Expired, BadSignature, WrongVersion}` | CLI exit 2, one-line stderr message |
| `JenkinsError` (auth, 404, network) | sidecar: retry with exp backoff, cap 60 s; log structured warning |
| `TransferError` (blob interrupted) | resumable; CLI retries 3× then reports partial state + resume hint |
| Gossip gap (`seq` skip) | stderr warning, keep streaming |
| Slack post failure | log + continue — never blocks build watching |

## Testing

- `tailgate-core`: pure unit tests — ticket round-trip, signature tamper cases, expiry, topic uniqueness.
- `tailgated`: `wiremock` Jenkins (progressive log with `X-More-Data` semantics, artifact download); no real network.
- Integration: two in-process iroh endpoints on localhost — sidecar side + client side; assert watch stream and pull-verify end-to-end. No relay dependency in CI.
- `cargo clippy -- -D warnings` + `cargo fmt --check` gate.

## Non-Goals (v1)

- Browser/web viewer (iroh-in-WASM path — later, via relay bridge).
- Jenkins plugin (Java), Bolt Slack app with buttons, SSO/policy plane (that's the SaaS layer).
- GitHub Actions / GitLab runners.
- Cross-build artifact dedup UI (store already dedups by hash; surfacing it is v2).
- Log persistence/replay beyond the live gossip window (`--from-start` is best-effort from sidecar's in-memory ring buffer, default 10k lines).

## Open Questions

- [ ] **1. iroh-blobs version line**: latest (iroh-1.0-compatible, "not production quality" per docs) vs 0.35 (older iroh). Proposal: latest for launch, documented in burn log.
- [ ] 2. Relay policy: default n0 public relays acceptable for demo; enterprises will demand self-hosted relay config (`TAILGATE_RELAY_URL`) — include env var now or v2?
- [ ] 3. `--from-start` ring buffer size / memory cap per concurrent build.
- [ ] 4. Ticket revocation: TTL-only for v1, or maintain an in-memory revocation set on the sidecar?

## Agent Build Instructions

> Implement end-to-end from this README. Resolve Open Questions 1–2 with Emmanuel first; 3–4 may be deferred with defaults noted above.

### Build Order

| Phase | Deliverable | Done when |
|-------|-------------|-----------|
| 0 | Workspace scaffold, CI, `.gitignore` (keys!) | clippy/fmt gates pass |
| 1 | `tailgate-core`: ticket + manifest + topic | unit tests pass incl. tamper cases |
| 2 | `tailgated`: Jenkins watcher w/ wiremock | mocked build streams lines internally |
| 3 | iroh wiring: gossip publish, blobs import, ctl stream | localhost integration test passes |
| 4 | `tailgate` CLI: watch + pull | e2e test green; exit codes correct |
| 5 | Slack webhook + `mint` + docker-demo | demo compose produces the launch GIF flow |

### Constraints

- Rust 2021, stable toolchain; typed errors (`thiserror`), no `unwrap` outside tests.
- Secrets via env only. `AIDEV-` anchors on ticket verification, topic generation, and version-pin decisions.
- Tests never touch real Jenkins, Slack, or public relays.

### Acceptance Criteria

- [ ] Ticket tamper/expiry tests prove no unsigned or expired ticket is served.
- [ ] `watch` streams a live mocked build; `pull` verifies a ≥1 GiB artifact hash end-to-end on localhost.
- [ ] Interrupted `pull` resumes without re-downloading completed ranges.
- [ ] `docker compose up` in `examples/docker-demo` yields the full Slack-message → watch → pull flow.
- [ ] README Quick Start executes verbatim on a clean macOS + Linux box.

## Next Steps

1. Emmanuel: rule on Open Questions 1–2 and any stack-table overrides.
2. Agent: Phase 0–1 (core crate is pure logic — fastest de-risk).
3. Spike checkpoint after Phase 3: confirm hole-punch works sidecar→laptop across a real NAT before polishing CLI UX.
4. Launch prep: record demo GIF from docker-demo; run `ship-check` before repo goes public.
