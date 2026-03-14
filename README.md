# rust-holons

**Rust SDK for Organic Programming** — transport URI parsing, runtime
transport primitives, identity parsing, and filesystem discovery for
Rust holons.

## API surface

| Module | Description |
|--------|-------------|
| `holons::transport` | `listen(uri)`, `listen_stdio()`, `dial_tcp(uri)`, `dial_unix(uri)`, `parse_uri(uri)`, `scheme(uri)` |
| `holons::serve` | `parse_flags(args)`, `run_single(uri, service)`, `run(uri, extra_service, service)` with automatic `HolonMeta.Describe` registration from the current holon root |
| `holons::describe` | Build `DescribeResponse` payloads and `HolonMeta` services from `holon.yaml` and `protos/` |
| `holons::identity` | `parse_holon(path)` |
| `holons::discover` | `discover(root)`, `discover_local()`, `discover_all()`, `find_by_slug(slug)`, `find_by_uuid(prefix)` |

## Current scope

- Runtime transports: `tcp://`, `unix://`, `mem://`, plus `stdio://`
  helper support.
- `serve::*` auto-registers `HolonMeta.Describe` when `holon.yaml`
  exists in the current working directory, using `./protos` when present.
- `ws://` and `wss://` are normalized as listener metadata only.
- Discovery scans the current workspace, `$OPBIN`, and cache roots and
  deduplicates entries by UUID.

## Current gaps vs Go

- No `grpcclient` module for ws/mem dialing yet.
- No Rust Holon-RPC library module yet.
- `serve::run()` does not yet accept live `ws://` or `wss://` listeners.

## Test

```bash
cargo test
```
