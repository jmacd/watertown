# First-party site analytics collector

This standalone Unix service accepts Sitegen's first-visible page views and
writes durable local NDJSON. It has no pond, DataFusion, cloud, or TLS dependency.
Run one process, catalog registry, and log directory per **site and environment**.
Noyo and Caspar Water must not share a stream, even on the same serving host.

The collector is implemented; deployment is **not enabled**. The outer repository
contains a disabled `config/systemd/site-analytics@.service` template. Terraform
provisioning, binary promotion, proxy privacy verification, backup, and ingestion
integration are still deployment gates. The existing `pond` Debian package does
not yet install this binary.

## Build and configuration

```sh
cargo build --locked -p site-analytics-collector --release
site-analytics-collector --config /etc/watertown-analytics/site-staging/collector.json --check
site-analytics-collector --config /etc/watertown-analytics/site-staging/collector.json
```

`--check` validates configuration and all installed catalogs without opening the
log or listener. Configuration is JSON with unknown fields rejected:

```json
{
  "site_id": "caspar-water",
  "origin": "http://watershop.casparwater.us",
  "listen": "127.0.0.1:9321",
  "catalog_dir": "/etc/watertown-analytics/site-staging/catalogs",
  "log_dir": "/var/lib/watertown-analytics/site-staging",
  "endpoint": "/_analytics/view",
  "max_log_bytes": 67108864,
  "segment_bytes": 8388608
}
```

These capacities are examples, not an approved retention policy. The cap covers
managed file lengths, including inventories and recovery evidence; reserve
additional filesystem allocation/metadata space or use a filesystem quota.
Nothing deletes logs. Capacity exhaustion stops the writer, fails readiness, and
returns `503` until an operator resolves the problem and restarts the service.
The process remains available for private diagnostics after a writer failure.

Use the same `site_id` as Sitegen. The origin must be an exact canonical HTTP(S)
origin, without a trailing slash. Bind only to loopback. Create the log directory's
parent before startup; the collector can create its own final directory.
Provision `watertown-analytics-<instance>` as a dedicated account/group before
installing the unit. Keep configuration/catalogs root-owned and read-only to that
group, and the log directory owned by the collector (0700 or 0750). The unit's
0027 umask produces 0640 logs and inventories. Only explicitly authorized
ingestion readers may join this narrow group.

## Catalog installation and site isolation

Copy the complete Sitegen `_analytics/catalog.json` into the private registry as
`<catalog_id>.json` using a temporary non-JSON filename and an atomic rename.
The collector verifies the producer's SHA-256 identity, schema, site ownership,
page paths, and bounds. It rejects every unknown catalog/page pair. It does not
accept a browser-supplied site routing field.

Send `SIGHUP` (or `systemctl reload site-analytics@<instance>`) after installation.
Reload validates a complete replacement registry before swapping it in. Failed
reloads are logged and preserve the last valid registry. Removing historical
catalogs is rejected; explicit revocation is not implemented. Confirm the new ID
appears in the **private** `GET /_analytics/health` response before publishing the
site. No public catalog-registration API exists.

A registry is limited to 4,096 catalogs, 2 MiB per catalog and 64 MiB of serialized
catalogs total. Keep the registry outside the public root. A move to a different
domain can use the same site identity and retained catalogs, with the collector's
trusted origin changed for that site's deployment.

## HTTP and privacy contract

Only the configured exact path accepts `POST application/json`, with at most
2 KiB of body and a known canonical URI of at most 1 KiB. Content encoding,
unknown payload fields, and unsupported event/schema versions are rejected.
`Host` and `Origin` must match configuration. Missing `Origin` is rejected;
missing Fetch Metadata is accepted only with the correct Origin. When present,
Fetch Metadata must describe a same-origin, non-navigation request.
These headers are filters, not proof of a human reader.

Success is `204` with `Cache-Control: no-store`, **after** the log and progress
inventory reach their synchronization boundary. There are no cookies, CORS
permissions, response echoes, or request-body/header/address logs. Health includes
accepted/rejected/rate-limited/storage-failed/disconnected counters, bounded
pending work, catalog IDs, and durable segment progress. Counters are per-process;
they are not coverage intervals or audience counts.

Initial fixed limits are 64 concurrent connections, 8 KiB HTTP parser buffers,
a five-second whole-connection deadline, 100 requests per fixed one-second window,
a 256-event writer queue, and batches of at most 128 records or 10 ms.
Overload returns `429`/`503`; a saturated or timed-out connection can also close
without a response. A durable event whose response is lost is still recorded.
The browser must not retry.

Route the configured endpoint ahead of static rewrites in Caddy, preserving Host.
For a second site use a separate loopback collector and an explicitly configured
endpoint (for example `/_analytics/noyo/view`). Do not proxy the health path.
Staging's existing Caddy access log must exclude beacon requests; inspect **error
logs as well** under upstream failure before enabling collection. No active Caddy
configuration has been changed, and this proxy privacy gate has not been verified.
Production enablement, disclosure, and public aggregate granularity remain
separate approvals.

## Durable log and recovery contract

`views.jsonl` is active. Records contain `schema_version`, `received_at_us`,
`site_id`, `catalog_id`, `page_uri`, `segment_id`, and `record_seq`. Receipt times
can repeat or move backward; `(segment_id, record_seq)` is the record identity.

One writer holds `.writer.lock` for the process lifetime. Each group appends
complete lines, synchronizes the log, writes and synchronizes `inventory.tmp`,
renames it to `inventory.json`, and synchronizes the directory before replying.
The inventory records each segment's predecessor, record count, byte length, and
SHA-256 checksum, including the acknowledged active prefix.

At the size threshold, the next batch closes/synchronizes the active file,
renames it to immutable `views-<segment_id>.jsonl`, synchronizes the directory,
publishes the closed segment and new empty identity in the inventory, then
creates/synchronizes the new active file. A segment can exceed its threshold by
one bounded batch. Rotation never uses `copytruncate`.

Startup verifies inventory continuity and every listed segment/checksum. It
finishes interrupted rotations and validates the acknowledged active prefix.
Complete unacknowledged tail records are retained. An incomplete tail is first
saved and synchronized as private `recovery-<id>.bin` evidence; only bytes beyond
the acknowledged prefix may be truncated. Recovery is reported, and a used or
repaired active segment is closed before new collection. Missing/changed
acknowledged data, unlisted segments, or malformed complete records fail startup;
they are never silently skipped. Startup cost is linear in retained log bytes.

`.rotation.lock` is held exclusively during startup recovery and rotation. The
ingester must take a shared lock while discovering/opening and reading its host
snapshot. `logfile-ingest` participates when explicitly configured with
`analytics_site_id` and the collector's exact filenames. It verifies the complete
inventory and stages its durable prefixes under the shared lock, then releases
the lock before pond writes. Query that isolated raw destination through
`site-analytics://`. Ordinary glob matching without this opt-in is **not** the
collector protocol.

`SIGTERM`/`SIGINT` stop serving and drain queued writes before exit. Crash tests
cover append, log synchronization, inventory publication, rename, new-inventory,
and new-active boundaries, plus real process kill/restart. These are not a
power-loss certification: `sync_all` relies on the host filesystem/device honoring
synchronization. Local disk durability does not replace independently recoverable
raw backups. Retention, backups, segment export, and coverage intervals remain
work for the ingestion/operations phases.

## Validation

```sh
cargo test --locked -p site-analytics-collector
cargo clippy --locked -p site-analytics-collector --all-targets -- -D warnings
cargo tree --locked -p site-analytics-collector --edges normal
```
