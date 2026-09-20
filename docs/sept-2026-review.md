# TinyFS Architecture Review - September 2026

## Scope

This review evaluates the quality and coherence of the TinyFS abstraction in
`crates/tinyfs`, including its public API, node and path model, persistence
boundary, transaction behavior, metadata and versioning, memory and hostmount
implementations, Arrow and Parquet integration, and representative downstream
use.

TinyFS has a strong conceptual core, but its backend substitutability has
eroded. Its identity and path model remains coherent and extensively exercised.
Its operation contracts and persistence boundary are less coherent: several
APIs imply semantics that the hostmount or memory backends do not provide.
Some of these inconsistencies create credible data-loss or data-corruption
paths.

## Status on 2026-09-19

Findings 2-6 have been addressed with regression coverage. The subsequent
transaction-coherence work also made physical-series providers
generation-aware, rejected stale and post-transaction reads, rejected commit
with unfinished writers or active transaction reads, corrected latest-version
selection and cross-pond version lookup, and aligned memory-provider behavior.
That work is documented in `live-monitoring-projection-design.md`, but it is
general TinyFS/TLogFS hardening; live monitoring itself is paused.

Three review outcomes remain:

1. **Fix hostmount rename.** Finding 1 is still an open high-severity bug:
   `WD::rename_entry` composes `Directory::remove` and `insert`, while
   `HostDirectory::remove` physically deletes content.
2. **Expand the backend conformance suite.** The first shared TinyFS and
   DataFusion contracts now run against memory and TLogFS. Extend them to
   directory rename/unlink behavior, dynamic-node typing, error injection, and
   hostmount after its rename semantics are fixed.
3. **Split capabilities only after the tests expose the boundary.**
   `PersistenceLayer` remains broad and now also has optional coherence state.
   Capability decomposition is still desirable, but should be driven by the
   conformance matrix rather than started as an unconstrained refactor.

The next implementation item is therefore the hostmount-safe rename primitive
and enrolling hostmount in the applicable contract tests. Live monitoring is
not a prerequisite.

### Cross-persistence validation baseline

The first backend-independent harnesses now exercise the contracts needed by
transactional read-after-write:

- `tinyfs::testing::persistence_contract` drives TinyFS operations through an
  active filesystem and provider context. It verifies independent-handle
  visibility, transaction-global writer exclusion, immediate reads after
  writer shutdown, append ordering, exact-version reads, seeks, range
  validation, provider-cache generation invalidation, and stale-provider
  rejection.
- `provider::testing::assert_series_read_after_write` writes real Parquet
  `TablePhysicalSeries` versions and queries them through DataFusion. Version 1
  is queryable before commit; appending version 2 makes the old provider fail
  as stale; rebuilding through the same `ProviderContext` returns all rows.
- Memory instantiates both active-snapshot contracts in
  `crates/tinyfs/src/tests/memory.rs` and
  `crates/provider/tests/memory_persistence_tests.rs`.
- TLogFS instantiates both contracts inside a real write transaction in
  `crates/tlogfs/src/tests/persistence_contract.rs`, then verifies durable
  visibility after commit, rollback after guard drop, and rejection of both
  persistence reads and registered DataFusion providers after transaction
  closure.

| Contract | Memory | TLogFS | Hostmount |
|---|---|---|---|
| completed write is immediately readable | covered | covered in transaction | not enrolled |
| independent handles share writer exclusion and visibility | covered | covered in transaction | not enrolled |
| append versions are ordered and individually readable | covered | covered before and after commit | unsupported |
| seek and range semantics are consistent | covered | covered before commit | unsupported |
| provider cache follows mutation generation | covered | covered | unsupported |
| DataFusion reads a staged series version | covered | covered before commit | unsupported |
| old DataFusion provider fails after append | covered | covered | unsupported |
| rebuilt provider sees committed plus staged versions | covered | covered | unsupported |
| commit persists and closes old contexts/providers | not a memory capability | covered | unsupported |
| abort discards writes and closes old contexts/providers | not a memory capability | covered | unsupported |

The distinction in the last two rows is intentional. `MemoryPersistence`
applies writes immediately and its coherence state spans the persistence
instance; it does not stage a durable transaction or implement commit/abort.
It is therefore a useful reference backend for active-snapshot semantics, but
must not be used as evidence for rollback, durability, or post-transaction
lifecycle behavior.

The first TLogFS run exposed and fixed a coherence defect:
`InnerState::load_node` reconstructed pending directories but ignored
`pending_files`. A second TinyFS handle could see a newly inserted directory
entry while failing to load its still-open file, so it never reached the
transaction-global writer guard. Pending files are now reconstructed from
transaction state before consulting committed Delta records.

The next test increments should be:

1. add the atomic rename primitive and enroll memory, TLogFS, and hostmount in
   shared rename, replacement, and unlink tests;
2. add deterministic barrier-controlled tests for a provider scan overlapping
   mutation and commit, rather than testing active-query rejection only with a
   manually acquired guard;
3. add backend-neutral fault injection for writer finalization, metadata,
   version listing, and ranged reads, proving failures remain visible and do
   not publish partial state;
4. cover dynamic file/directory reconstruction and exact logical attributes
   through the same backend matrix; and
5. decide whether memory should gain explicit transaction snapshots or should
   formally advertise only the active-snapshot capability.

## Findings

Each finding retains the original failure description for rationale. Its
`Status` paragraph is authoritative for the current implementation.

### 1. High: hostmount rename destroys file and directory contents

Locations:

- `crates/tinyfs/src/dir.rs:84-85`
- `crates/tinyfs/src/wd.rs:591-608`
- `crates/tinyfs/src/hostmount/directory.rs:226-247`

Status: open. Rechecked on 2026-09-19: `WD::rename_entry` still calls
`remove` followed by `insert`, and `HostDirectory::remove` still uses
`remove_file` or `remove_dir_all`.

`Directory::remove` is documented and used as an unlink operation that returns
the removed node without deleting it. `WD::rename_entry` consequently implements
rename as remove followed by insert.

`HostDirectory::remove` does not implement unlink semantics. It physically
deletes a file or recursively deletes a directory. The subsequent insert creates
an empty replacement rather than moving the original object.

As a result, a host-backed `rename_entry` can lose file contents or an entire
directory tree. Host-backed factory paths can reach this operation, so this is
not merely an unused discrepancy between implementations.

The abstraction should provide an atomic directory rename primitive instead of
composing rename from remove and insert. The hostmount implementation should use
`std::fs::rename`; persistent implementations should perform an atomic directory
map mutation within their transaction model.

### 2. High: glob loop tracking is global, concurrency-unsafe, and leaked on errors

Locations:

- `crates/tinyfs/src/fs.rs:22,107-121`
- `crates/tinyfs/src/wd.rs:1233-1257`

Status: addressed in September 2026 by replacing `FS`-global active-node state
with a task-scoped traversal context. Nested dynamic-directory visits reuse the
current context, while independent traversals receive isolated state. RAII
guards remove active nodes after success, errors, or task cancellation.

Traversal stores its set of active nodes in an `FS`-global `HashSet<FileID>`.
This state describes one traversal stack, but its lifetime and scope are the
whole filesystem instance.

Two concurrent traversals through the same directory can therefore interfere
and report a false `VisitLoop`. In addition, an error after `enter_node` bypasses
`exit_node`, leaving the node permanently marked active. Later traversals on the
same `FS` can then fail even when no loop exists.

Traversal state should be local to each invocation and passed through recursive
calls. If shared state remains necessary, node entry should return an RAII guard
that removes the node on every exit path.

### 3. High: typed writers silently accept incompatible existing nodes

Location:

- `crates/tinyfs/src/wd.rs:707-733`

Status: addressed in September 2026 by rejecting existing nodes whose encoded
entry type differs from the requested writer type, with regression coverage for
rejection, non-mutation, and same-type append behavior.

`async_writer_path_with_type(path, requested_type)` uses `requested_type` only
when creating a missing node. If the path already exists, it returns that node's
writer without confirming that its encoded `EntryType` matches the requested
type.

A caller can therefore request one table or series type but write through an
existing node with different identity, append/version behavior, metadata
extraction, and query semantics. The bytes change while the node continues to
advertise its original type.

The immediate fix is to reject existing nodes whose entry type differs from the
requested type. Longer term, creation and opening semantics should be explicit,
for example:

- `create_typed`
- `open_writer`
- `open_or_create_typed`

### 4. High: the persistence boundary prevents genuine ranged and streaming reads

Locations:

- `crates/tinyfs/src/persistence.rs:110`
- `crates/provider/src/tinyfs_object_store.rs:276-320,405-539`

Status: addressed in September 2026 by adding owned streaming readers and
direct logical-version range reads to `PersistenceLayer`. The DataFusion object
store now honors bounded, offset, and suffix requests; tlogfs large-file ranges
seek directly into the chunked logical reader instead of reconstructing the
complete version.

`PersistenceLayer::read_file_version` returns `Vec<u8>`. The TinyFS DataFusion
object store must consequently load the complete retained version for each
`get_range` call and then slice and copy the requested bytes. Its streaming
response similarly buffers the complete version before yielding small chunks.

This makes memory and I/O proportional to total object size rather than requested
range size. Parquet footer and schema reads can materialize an entire large
version, weakening projection and predicate pushdown at a boundary intended to
support analytical workloads.

The persistence API should gain a streaming reader primitive and, preferably, a
direct ranged-read operation such as:

```rust
async fn read_version_range(
    &self,
    file_id: FileID,
    version: u64,
    range: Range<u64>,
) -> Result<Bytes>;
```

A whole-version `Vec<u8>` helper can remain as a convenience implemented on top
of the streaming or ranged interface. `TinyFsObjectStore` should be the first
consumer migrated.

### 5. Medium: `ChainedReader` advertises rewind support without rewinding

Location:

- `crates/tinyfs/src/chained_reader.rs:103-143`

Status: addressed in September 2026 by retaining seekable child readers and
mapping logical `Start`, `Current`, and `End` seeks onto consistent per-child
positions. Position changes are applied only after every child seek completes.

After data has been consumed, `start_seek(SeekFrom::Start(0))` returns success
without resetting the logical position, current segment, or underlying readers.
`poll_complete` reports the old position, and later reads continue from that
position or EOF.

This contradicts the `AsyncReadSeek` contract and the implementation's stated
support for seeking to the beginning. Either the chained reader must retain
seek-capable segment readers and implement real repositioning, or the general
streaming-reader contract must stop promising `AsyncSeek`.

### 6. Medium: memory persistence is not a conforming reference backend

Locations:

- `crates/tinyfs/src/memory/persistence.rs:116-153,301-329,447`
- `crates/tinyfs/src/memory/file.rs:50-52,494-510`

Status: addressed in September 2026. Metadata lookup no longer calls back into
the persistence layer while holding its state mutex; dynamic nodes retain their
file or directory operational type; version timestamps use Unix microseconds;
and writer shutdown propagates persistence and integrity-input failures while
releasing write state. Regression tests cover pending metadata, failure
propagation and retry, dynamic node types, and timestamp units.

Follow-up in progress: the shared harness now covers memory and TLogFS
read/write, version/range, and provider-coherence semantics. Hostmount cannot
join the directory mutation portion until Finding 1 is fixed, and dynamic-node
and fault-injection coverage remain to be added.

The memory implementation diverges from expected persistence semantics in
several ways:

1. Metadata lookup for a stored file without a finalized version can deadlock.
   `MemoryPersistence::metadata` holds the state mutex while metadata resolution
   calls back through a file handle that attempts to acquire the same mutex.
2. `create_dynamic_node` constructs `NodeType::File` even for
   `DirectoryDynamic`.
3. Version timestamps are recorded in milliseconds although
   `FileVersionInfo` specifies Unix microseconds.
4. `MemoryFileWriter` suppresses persistence errors during shutdown and reports
   success.

These differences make the memory backend unreliable as a reference or test
implementation. Code can pass against memory while depending on behavior that
production persistence does not share, and pending-file metadata access can
hang.

TinyFS needs a backend conformance suite that is run against every
`PersistenceLayer` implementation. It should cover operation semantics, dynamic
node typing, timestamp units, error propagation, pending writes, metadata,
versions, rename, and directory behavior.

## Coherence assessment

TinyFS remains viable, but it is no longer fully coherent as one abstraction.
The central issue is not the filesystem model itself; it is responsibility
accumulation at the persistence boundary.

`PersistenceLayer` now spans:

- node reconstruction;
- directory persistence;
- factory materialization;
- transactions;
- version history;
- logical attributes;
- temporal metadata;
- reserved-index maintenance and collapse behavior;
- object-store read concerns.

Optional operations and default no-op behavior make backend capabilities
implicit rather than type-checked. Memory, hostmount, and tlogfs consequently
appear interchangeable even where their semantics differ materially.

This boundary should gradually be split into a small persistence core and
explicit capability traits. Callers that require versioned reads, logical
metadata, factories, or maintenance operations should express those
requirements in their types rather than discover unsupported behavior at
runtime.

## Strengths to preserve

The review found several strong and coherent design choices:

- Pond-qualified `FileID` values make cross-pond identity explicit.
- Entry type is encoded into stable node identity.
- `NodePath` and `WD` separate identity, active path, and resolution context.
- Effective-root and read-only foreign-import behavior is well defined.
- Append-only series semantics and bounded reads are conservative.
- Tlogfs adds explicit transactions, poisoning, commit behavior, and integrity
  metadata.
- BAO/BLAKE3 validation gives corruption failures a clear boundary.
- `DirectoryEntry` supports lightweight enumeration before full node loading.
- Arrow and Parquet temporal bounds are propagated as filesystem metadata.
- Reserved log and index identities are excluded from ordinary user
  enumeration.

These features provide a sound basis for tightening the abstraction rather than
replacing it.

## Recommended remediation order

Completed:

- traversal loop state is invocation-local and RAII-safe;
- typed writers reject incompatible existing nodes;
- streaming and ranged version reads back the object-store adapter;
- `ChainedReader` implements real seeking; and
- the identified memory-persistence defects are repaired.

Remaining order:

1. Add cross-backend regression coverage for rename and replace
   remove-plus-insert with a backend rename primitive. Hostmount must use
   `std::fs::rename`; transactional backends must update one directory mapping
   atomically.
2. Grow the initial conformance suite to cover only semantics that each
   participating backend explicitly claims to support.
3. Use failures and explicit unsupported cases from that suite to define
   capability traits, then migrate callers incrementally.

This keeps the next step narrow and correctness-driven. It does not require
live monitoring or a speculative `PersistenceLayer` rewrite.
