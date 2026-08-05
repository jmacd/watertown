# Storage Profiles: Naming Connections Instead of Copying Them

> **Status:** Design proposal (unimplemented). Follows
> [`rate-limiter-design.md`](rate-limiter-design.md), which established the
> pattern this reuses: a factory node declares a policy, a consumer names it by
> path, and the binding is validated at attach time.
>
> **Targets:** MinIO is what runs today. **Azure is the next real backend.**
> AWS S3 and Cloudflare R2 are hypothetical and are *not* proposed for
> implementation -- they appear only where they are evidence that the shape
> extends.
>
> This document proposes **storage-profile factories** whose nodes live at
> `/sys/storage/<name>`, and an optional `storage:` field on `RemoteAttachment`
> that names one instead of carrying its own copy of the connection.

---

## 0. Problem statement

A remote attachment at `/sys/remotes/<name>` currently carries the entire
connection inline (`remote_config.rs:53-75`):

```yaml
url: s3://water-staging
region: us-east-1
endpoint: http://watershop:9000
access_key_id: pondwriter
secret_access_key: ${env:S3_SECRET_KEY}
allow_http: true
```

Six fields, five of which say nothing about *this remote* and everything about
*which storage system we are talking to*. On the `site` pond there are three
attachments, so the same five facts appear three times. Across the watershop
station there are roughly ten copies. Rotating an endpoint means editing every
one of them and getting all of them right.

### 0.1 The trigger: a bool that had nowhere to go

Converting `caspar.water/config/scripts/attach-remotes.sh` from CLI verbs to
declarative `pond apply` documents (Decision L11) ran aground on one field.
`pond apply` env-expands the **URL** only, deliberately: everything else is
stored as written so `${env:...}` references stay out of the replicated
document and resolve per-replica at use time (`apply.rs:425-440`). But
`allow_http` is a `bool`, so `allow_http: ${env:S3_ALLOW_HTTP}` cannot even
parse, and the shell script's `S3_ALLOW_HTTP` switch has nowhere to go.

The tempting fix is to add bool env-expansion. That would be fixing the
symptom. The real problem is that **`allow_http` is not a property of a
remote.** "Plain HTTP is acceptable" is a fact about the MinIO deployment on
watershop. It is not a fact about "the origin backup of the water pond," which
is what `/sys/remotes/origin` is supposed to describe. The field could not be
expressed per-environment because it was stored per-*remote*, in the wrong
place entirely.

### 0.1a Why it had nowhere to go: two configuration paths

The bool is a symptom of a split one level down. `pond apply` has **two
unrelated ways** to write configuration into a pond, and they follow different
rules about environment references.

**The `mknod` path (factories).** `mknod` and `kind: mknod` expand a copy of the
config for *validation only* and store the **raw** text, references intact
(`mknod.rs:28-58`, `apply.rs:288-330`). `FactoryRegistry::create_file` and
`create_directory` then expand again at every node materialization
(`registry.rs:314-366`). The invariant is: **expand at each use, never persist a
resolved value.** Any scalar can be a reference, because expansion happens on
text before it is ever typed.

**The remote path (attachments).** `kind: remote` / `kind: backup` never touches
the factory registry. `parse_remote_kind` deserializes a `RemoteAttachment`
straight into a typed struct and stores it as its own document. It expands
exactly one field, `url`, and stores that one **resolved**.

That is why `allow_http` had nowhere to go. On the mknod path the question never
arises -- `allow_http: ${env:S3_ALLOW_HTTP}` is just text until the moment it is
used. On the remote path the value is parsed as a `bool` before any expansion
could apply, so the reference cannot even be written down. The field was not
merely in the wrong struct; it was on the side of the split that cannot express
references at all.

So this design has a second purpose beyond §0.2. A storage profile is a real
factory node, which brings the *connection* half of a remote back onto the mknod
path and under its invariant. What remains on the attachment is only what is
genuinely per-attachment: `url`, `mount`, direction, `limits`, and the `storage:`
pointer itself. That is a narrowing of the special surface, in the direction of
all configuration being uniform, inspectable, infrastructure-as-code.

Two things it does **not** fix, recorded so they are not mistaken for done:

1. **The attachment is still not a factory node.** Profiles shrink the bespoke
   surface; they do not remove it. Making remotes true `mknod` nodes is a much
   larger change and is not proposed here -- but it is the direction this points,
   and worth deciding deliberately rather than drifting into.

2. **`url` is still stored resolved.** Defensible when written -- the URL is
   needed at attach time to open and validate the remote, and it is not a secret
   -- but it is the one place the "never persist a resolved value" invariant is
   knowingly broken. The cost is that an attachment document is not portable
   across replicas whose URL differs by environment, which is precisely the
   property replication is supposed to give. See §10.

### 0.2 The bigger problem, now that Azure is next

The URL scheme currently drives every storage decision in the codebase, by
string comparison, in eight places:

| Site | Test |
|---|---|
| `remote_config.rs:130` | `if self.url.starts_with("s3://")` -- gates **all** storage options |
| `push.rs:65`, `pull.rs:111`, `verify.rs:72`, `restore.rs:74` | gates `register_s3_handlers()` |
| `remote.rs:244`, `remote.rs:532` | gates `register_s3_handlers()` |
| `guard.rs:1292` | gates `register_s3_handlers()` on the auto-push path |

Adding Azure means finding and correctly amending all eight. Two of them are
worse than merely tedious:

1. **`remote_config.rs:130` fails silently.** `to_storage_options` returns an
   **empty map** for any URL that is not `s3://`. An `az://` attachment would
   therefore drop every credential it was given and surface as an opaque
   authentication failure, with the configuration looking perfectly correct.
   This is exactly the class of failure
   [`fallback-antipattern-philosophy.md`](fallback-antipattern-philosophy.md)
   exists to reject, sitting in the code today, latent until the first non-S3
   backend arrives.

2. **`register_s3_handlers` is a process-global side effect**
   (`sync-store/src/s3_registration.rs:72`) that inserts factories into
   delta-rs's scheme registry. Azure needs a parallel registration built on
   `MicrosoftAzureBuilder`, plus the `azure` feature on `object_store`
   (`Cargo.toml:78` currently enables `aws` only). Deciding *which* to call by
   sniffing a URL prefix at eight call sites is a worse arrangement than
   asking the profile, which already knows.

**A profile knows its provider.** That is the thing a URL prefix is being made
to approximate. Replacing eight scheme comparisons with one dispatch on the
profile kind is a larger practical payoff than the deduplication in §0, and it
is the reason to do this *before* Azure rather than after.

### 0.3 What we want

1. One place to say "this is the MinIO on watershop," named by path.
2. Attachments that carry only what is genuinely theirs: URL, mode, mount,
   limits.
3. Provider dispatch driven by a declared kind, not by string-matching a URL.
4. Illegal combinations that cannot be written down, rather than combinations
   discouraged by comments.
5. No weakening of the rule that keeps secrets out of replicated documents --
   in fact, a strengthening of it.
6. Full backward compatibility: every existing attachment keeps working,
   unmodified.

---

## 1. Why a pond node, and why not a config key

The control table (`pond config set`) is the other candidate home, and it is
the wrong one for the same reason it was the *right* one for limiter state, run
in reverse.

Limiter windows belong in the control table because they are **per-replica,
disposable, and high-churn** (Decision L1): a restored replica should start
with a fresh window, and bookkeeping should never become pond history. A
storage profile is the opposite on all three counts. It is **shared policy**,
it must **survive a restore**, and it changes about as often as the
infrastructure does. It also needs to be *reviewable*: the whole point of the
`pond apply` conversion is that this configuration lives in git and is read
before it is deployed.

So the profile is a pond node, versioned and replicated like the attachment
that references it, and it inherits `pond apply`'s existing idempotence and
multi-document support for free.

### 1.1 The boundary that must not blur

A profile node is **where credential *references* live, not where credentials
live.** This distinction already exists for attachments and is enforced
imperatively (`remote.rs:186-202`): `secret_access_key` must be an
`${env:VAR}` reference, because the attachment YAML is an oplog row that `pond
push` replicates to every backup, so a literal secret would be exposed on all
replicas.

A profile node makes that rule *more* important, for two reasons. It is
replicated the same way, and it is more inviting to inspect -- a node named
`/sys/storage/minio` is something an operator will `pond cat`, in a way they
never did with an attachment buried under `/sys/remotes/`. Concentrating
credentials in one obvious place is a real benefit for management and a real
hazard if the place ever holds plaintext.

**Decision A1.** Every credential field on a storage profile MUST be an
`${env:...}` reference, validated at creation time -- not just the secret. The
current rule covers `secret_access_key` alone; an access key id is a weaker
secret but still an identity, and an Azure account key is as sensitive as an S3
secret.

**A1a -- the rule must be checked on the *raw* config.** `mknod` and `pond
apply` both **validate the expanded config and store the raw one**, so that
secrets live in the environment and only references are persisted. That split
means a factory's ordinary `validate_config`, which receives expanded bytes,
*cannot* express this rule: expansion is precisely what turns a reference into a
literal, so after it the distinction no longer exists. Factories therefore gain
an optional `validate_raw_config`, run on the pre-expansion text before anything
is written. Checking on first use instead would be too late: by then the
plaintext is already in replicated, append-only history, where it cannot be
withdrawn.

**A1b -- consumers bind from the stored config, and the node's content is
redacted.** These are the same decision. `FactoryRegistry::create_file`
env-expands a stored config before handing it to the factory, so the node's
rendered content is built from *resolved* values -- the real secret. It follows
that (i) the rendered content MUST redact credentials, and (ii) consumers MUST
NOT bind from it, because a resolved value is exactly what a profile must not
hand out, and because a replica resolving as the *writer* would defeat A6.
`ResolvedStorage` therefore reads the raw stored config via
`get_dynamic_node_config`. So `pond cat` shows a safe, deliberately unusable
view (`secret_access_key: <redacted>`), while enforcement reads the references.

---

## 2. Not "auth" -- a profile

The natural name for this is an *auth* or *credentials* factory, and that name
is a trap, because it invites exactly the wrong scope.

If the node holds credentials only, then `endpoint`, `region`, and `allow_http`
stay on the attachment -- and §0.1, the thing that motivated the work, is not
fixed. Nor is §0.2: a credentials-only node does not know its provider, so the
eight scheme comparisons survive. Those fields are not credentials, but they
are precisely the fields in the wrong place.

There is good precedent for the wider scope. An AWS **profile** in
`~/.aws/config` contains exactly this set -- credentials, region, and endpoint,
under one name, referenced by name from everything that connects. Azure's
**connection string** is the same idea with the same contents.

**Decision A2.** The node is a *storage profile*: the complete description of
how to reach a storage system, credentials included. Nodes live at
`/sys/storage/<name>`; factories are named `storage-<provider>`.

---

## 3. The factories

**Decision A3.** One factory per provider, not one `storage` factory with
optional fields.

MinIO and Azure settle this on their own, without appeal to hypotheticals.
They do not differ in the *values* of a shared field set; they differ in what
the field set **is**. MinIO is an endpoint plus an access-key/secret pair.
Azure is an account name plus *one of* an account key, a SAS token, or a
service-principal triple -- and it reaches storage through a different
`object_store` builder, under different URL schemes. A single factory spanning
both would carry a dozen optional fields of which any given profile uses four,
which is a union type wearing a struct's clothes: it can express
`account_key` alongside `endpoint`, and has to reject at runtime what it should
not have been able to represent.

Separate kinds also make dispatch trivial (§0.2): the kind *is* the provider,
so it selects the handler registration and the option builder directly.

**Only `storage-minio` and `storage-azure` are proposed.** `storage-s3` and
`storage-r2` are sketched in §3.3 solely as evidence that the shape extends;
building them before there is a bucket to point them at would be speculation.

### 3.1 `storage-minio` (today)

```yaml
version: v1
kind: mknod
metadata:
  path: /sys/storage/minio
spec:
  factory: storage-minio
  config:
    endpoint: ${env:S3_ENDPOINT}
    region: ${env:S3_REGION}
    access_key_id: ${env:S3_ACCESS_KEY}
    secret_access_key: ${env:S3_SECRET_KEY}
```

`endpoint` is **required** -- a MinIO without one is not a MinIO. `allow_http`
does not appear: a `storage-minio` profile permits plain HTTP by definition,
which is the entire reason the kind exists. An operator running MinIO behind
TLS simply writes an `https://` endpoint; permitting HTTP does not require it.

This is where `S3_ALLOW_HTTP` goes. It stops being a variable and becomes the
choice of `kind`.

Path-style addressing is likewise a property of the kind rather than a side
effect. Today `to_storage_options` sets `virtual_hosted_style_request: false`
whenever an endpoint happens to be non-empty (`remote_config.rs:148-151`),
which is a MinIO accommodation applied by coincidence of another field's
emptiness.

Registration reuses `register_s3_handlers` unchanged
(`sync-store/src/s3_registration.rs:72`), which already covers S3-compatible
backends.

### 3.2 `storage-azure` (next)

```yaml
version: v1
kind: mknod
metadata:
  path: /sys/storage/azure
spec:
  factory: storage-azure
  config:
    account_name: ${env:AZURE_STORAGE_ACCOUNT}
    account_key: ${env:AZURE_STORAGE_KEY}
```

Or, with a SAS token instead:

```yaml
    account_name: ${env:AZURE_STORAGE_ACCOUNT}
    sas_token: ${env:AZURE_STORAGE_SAS}
```

Or a service principal:

```yaml
    account_name: ${env:AZURE_STORAGE_ACCOUNT}
    service_principal:
      client_id: ${env:AZURE_CLIENT_ID}
      client_secret: ${env:AZURE_CLIENT_SECRET}
      tenant_id: ${env:AZURE_TENANT_ID}
```

**Exactly one** credential shape must be present. Zero is an error, and so is
two -- a profile with both an account key and a SAS token has no unambiguous
intent, and silently preferring one would be the same anti-pattern as a
precedence rule in §4.1. This is the concrete reason Azure cannot be optional
fields on a shared struct: "exactly one of three groups" is not something a
flat field set can state.

Implementation notes, all of which are new work rather than configuration:

- `object_store` needs its `azure` feature (`Cargo.toml:78` enables `aws`
  only).
- A `sync-store/src/azure_registration.rs` mirroring `s3_registration.rs`,
  built on `MicrosoftAzureBuilder`/`AzureConfigKey`, registering the `az`,
  `azure`, `abfs`, and `abfss` schemes.
- The URL scheme accepted by an attachment must be validated against the
  profile kind (§4.2), so an `s3://` URL with an Azure profile is refused at
  attach rather than producing a confusing failure at first push.

### 3.3 `storage-s3` and `storage-r2` (not proposed)

Recorded as evidence for A3, not as planned work. AWS S3 would have no
`endpoint` and no `allow_http`. R2 would fix `region: auto` and *derive* its
endpoint from an `account_id` rather than have it pasted in, since a mistyped
R2 endpoint currently produces an authentication error rather than a clear one.
Both would serve the `prod_s3` block retained in
`caspar.water/terraform/station/watershop/watershop.tf:17-23` for a possible R2
re-enable. Neither should be built until that re-enable is real.

### 3.4 Registration

Each is a **leaf config node**, exactly like `rate-limit`: no content to
compute, nothing to execute, registered through `register_dynamic_factory!`
with a `validate` function that rejects a bad profile at `pond apply` time
rather than at first use (`rate_limit.rs:398-403`). A shared
`spec_from_config_bytes`-style entry point gives the factory and its consumers
exactly one interpretation of a node's config, as `rate_limit.rs:391` does for
budgets.

---

## 4. Referencing a profile from an attachment

### 4.1 Schema change

```rust
/// Storage profile describing how to reach `url`, as a pond path
/// (e.g. `/sys/storage/minio`).
///
/// Mutually exclusive with the inline `region` / `endpoint` /
/// `access_key_id` / `secret_access_key` / `allow_http` fields.
#[serde(default, skip_serializing_if = "Option::is_none")]
pub storage: Option<String>,
```

Same shape as `limits` (`remote_config.rs:77-98`): absent by default,
serializes away entirely, so every existing `/sys/remotes/<name>` document
stays byte-identical.

**Decision A4.** Naming a profile *and* setting any inline connection field is
a hard error, not a precedence rule. A precedence rule means an operator who
adds a profile to an attachment that still carries a stale inline endpoint gets
a working pond talking to the wrong storage. There is no reading of that
document whose intent is unambiguous, so it should not have a meaning.

An attachment reduces to what is actually its own:

```yaml
version: v1
kind: backup
metadata:
  path: /sys/remotes/origin
spec:
  url: s3://water-staging
  storage: /sys/storage/minio
  limits:
    bytes: /sys/limits/backup-bytes
    ops: /sys/limits/backup-ops
```

### 4.2 Validation at attach time

Attach already validates limiter bindings before touching pond state, on the
principle that a binding which silently fails to resolve is worse than no
binding (`remote.rs:179-183`). The profile reference gets the same treatment:
the node must exist, must be a `storage-*` node, and its kind must accept the
URL's scheme -- `storage-minio` accepts `s3://`/`s3a://`, `storage-azure`
accepts `az://`/`abfss://` and friends.

A `pond://` or `file://` URL with a profile is an error. Neither uses storage
options at all, and accepting one would let an operator believe credentials
were in play when they were not.

### 4.3 Dispatch (the §0.2 payoff)

The eight scheme comparisons collapse into the profile. Resolving a profile
yields a value that knows both how to register its handlers and how to build
its options, so each call site becomes one unconditional call rather than a
prefix test plus an S3-specific branch. The `starts_with("s3://")` gate in
`to_storage_options` (`remote_config.rs:130`), which today silently discards
credentials for any non-S3 URL, disappears entirely rather than being extended
with a second prefix.

Attachments with **no** profile keep exactly today's behavior, scheme sniffing
included -- the legacy path is preserved, not rewritten (§7).

---

## 5. Resolution: once, at the call site

This is the part that constrains the implementation, and it has already been
solved once on this branch.

`RemoteAttachment::to_storage_options()` is a **pure `&self` method**
(`remote_config.rs:128-158`) with six call sites: `push.rs:68`, `pull.rs:100`,
`verify.rs:76`, `restore.rs:77`, and `remote.rs:247` and `:531`. Reading a
profile node requires reading the pond, so a naive change makes it `async` and
needs a `Ship` -- which threads `Ship` through the entire transfer path and
risks the same async-recursion cycle the limiter hit (`guard.commit` →
`run_post_commit_remotes` → `Limiter::open` → `begin_read` → `commit`), fixed
there only by boxing.

**Decision A5.** Resolve the profile **once**, at the call site, into a
`ResolvedStorage`; keep option-building pure.

Every one of the six call sites already holds a `Ship`, so this is a local
change at each, and the transfer path stays synchronous and `Ship`-free. It is
precisely the pattern the limiter used: bind up front with `&mut Ship`, then
pass the resolved value into pure helpers
(`content_push.rs::push_content_to_remote_limited`).

**Decision A6.** `${env:...}` resolution stays at **use time**, per replica.

`resolve_field` currently resolves references when storage options are built,
which is what lets each replica authenticate as itself. Resolving a profile at
*attach* time and inlining the values into the attachment would replicate one
host's identity to every replica -- the exact failure the secret rule exists to
prevent, reintroduced through a side door. `ResolvedStorage` therefore means
"the profile document, read from the pond," not "the credentials, in memory."

---

## 6. Reporting

`pond status` gained per-remote limiter lines on this branch (Decision L9). The
profile belongs in the same block, because "which storage is this remote
actually talking to" stops being visible in the attachment once it is a
reference:

```
  origin
    url:          s3://water-staging
    storage:      /sys/storage/minio  (minio, http://watershop:9000)
    last pushed:  #412
    limits:
      bytes [/sys/limits/backup-bytes]: 4.0 MiB / 10.0 MiB (40%) per 1d
```

Credential fields are never printed, resolved or otherwise; for Azure the line
should name the credential *shape* (`account_key`, `sas_token`,
`service_principal`) without hinting at its value, since which shape is in use
is a genuine configuration question. A missing or malformed profile is
*reported* rather than propagated, as a broken limiter is, because a broken
profile is exactly when an operator needs `pond status` to still work.

---

## 7. Compatibility and rollout

`RemoteAttachment` is `#[serde(deny_unknown_fields)]` and the document is
replicated. A new binary reads an old document; an **old binary chokes on a
document containing `storage:`**. This is the same hazard `limits` introduced,
and it has the same answer: **binaries first, config second.** A profile must
not be introduced into a document until every replica that reads it can parse
it.

Inline fields are not deprecated on any schedule. They remain the only way to
express a one-off remote, they are what every existing document uses, and there
is no migration pressure -- an attachment converts when someone has a reason to
touch it. Concretely, the inline path stays S3-only forever: **Azure is
reachable only through a profile**, which is a feature, since the alternative
is extending eight scheme comparisons to keep a path alive that profiles exist
to replace.

---

## 8. Phasing

| Phase | Work | Verifies |
|---|---|---|
| A0 | `storage-minio` factory, `ResolvedStorage`, `storage:` field; inline path untouched | The whole idea, against the one provider actually in use |
| A1 | Attach-time validation, `pond status` line | The failure modes, before any deployment depends on them |
| A2 | Convert `attach-remotes.sh` to `pond apply` documents | §0.1 -- the thing that motivated this |
| -- | Store `url` raw, expand at use | §0.1a; separate change, see §10.5 |
| A3 | `storage-azure`: `object_store` azure feature, `azure_registration.rs`, the factory, dispatch | §0.2, and that the abstraction survives a genuinely different provider |
| -- | `storage-s3` / `storage-r2` | Deferred indefinitely; see §3.3 |

A2 is the MinIO payoff and should not be pulled earlier: converting the script
before profiles exist means writing YAML with duplicated inline credentials
that A0 would immediately rewrite. A3 is the Azure enablement and depends on
A0's dispatch existing, but not on A2.

---

## 9. Testing

Following the limiter suite's shape (`crates/cmd/tests/test_apply_remote.rs`),
each test should pin a *behavior we would regret losing*:

- A profile and an inline field together are refused -- the ambiguity in A4
  never acquires a meaning by accident.
- A literal credential in a profile is refused at `mknod` time, per kind.
- `render` and `pond status` never emit a resolved credential.
- An attachment naming a missing, wrong-kind, or scheme-incompatible profile is
  refused at attach.
- A `storage-minio` profile and the equivalent inline attachment produce
  **identical** storage options -- A0 is a move, not a rewrite.
- An Azure profile with zero credential shapes, or with two, is refused at
  `mknod` time.
- A profile resolves per-replica: two replicas with different environments
  authenticate differently from the same replicated document.
- An existing attachment with no `storage:` field serializes byte-identically.
- End-to-end: `pond apply` a profile and a backup referencing it in one
  document, then push -- the ordering case, as limiters have.

Azure has no `file://`-style local stand-in, so A3's end-to-end coverage stops
at option construction and scheme dispatch unless an Azurite container is
introduced. That is worth deciding before A3 rather than during it.

---

## 10. Open questions

1. **`/sys/storage` vs. `/sys/auth`.** §2 argues for `storage` because the node
   carries connection facts that are not credentials, and because dispatch
   (§4.3) needs it to know its provider. If the credential emphasis is
   preferred, `/sys/auth` with the same wide scope works; only the name
   changes.
2. **Azurite for A3 testing.** See §9. Without it, Azure coverage is unit-level
   only.
3. **Should a profile carry `limits`?** "This is the MinIO on watershop" and
   "this is what we will spend against it" are arguably one fact, and a
   station-wide byte budget shared by every attachment is plausible. Against:
   limits are per-remote today, and a shared budget changes the enforcement
   model (one window, many spenders, cross-remote starvation). Recommend
   deferring until A2 has produced real measurements.
4. **Should `url` move into the profile?** No -- the bucket or container is the
   one thing that genuinely differs per attachment. Recorded only to note it
   was considered.
5. **Should `url` be stored raw and expanded at use?** (§0.1a) It is the one
   knowingly resolved-at-write field left. Storing the reference and expanding
   at each use would put the remote path back under the mknod invariant and make
   an attachment document portable across replicas whose endpoint differs by
   environment. The work is small and the shape is already proven by A1b. The
   reason to hold: it changes the meaning of existing stored attachments, so it
   wants its own change with its own compatibility note (§7), not a rider on A0.
6. **Should attachments become factory nodes outright?** (§0.1a) The endpoint of
   the "all configuration through `mknod`" goal, and the only version of this
   that removes the special surface rather than narrowing it. Large, and it
   would have to preserve the attach-time validation and mount-conflict rules
   that currently live in `remote_config`. Named here so the direction is a
   decision rather than a drift.

---

## 11. Summary of decisions

| # | Decision |
|---|---|
| A1 | Every credential field on a profile must be an `${env:...}` reference -- stricter than today's `secret_access_key`-only rule. |
| A1a | That rule is checked on the **raw** config via a new `validate_raw_config` factory hook, because the ordinary validator only ever sees expanded text, and because catching a literal at first use is too late to withdraw it from replicated history. |
| A1b | The node's rendered content is built from expanded values, so it redacts credentials and is not parseable back; consumers bind from the raw stored config instead, which is what keeps per-replica resolution (A6) intact. |
| A2 | The node is a *storage profile* (connection + credentials), not an auth block: the misplaced fields that motivated this are not credentials, and dispatch needs the node to know its provider. Nodes at `/sys/storage/<name>`. |
| A3 | One factory per provider. MinIO and Azure do not differ in the values of a shared field set; they differ in what the field set is. Only `storage-minio` and `storage-azure` are proposed. |
| A4 | Naming a profile alongside any inline connection field is a hard error, never a precedence rule. Likewise an Azure profile must carry exactly one credential shape. |
| A5 | Profiles resolve **once at the call site** into a `ResolvedStorage`; option-building stays pure, so the transfer path stays synchronous and `Ship`-free. |
| A6 | `${env:...}` resolution stays at **use time, per replica**; a profile is never inlined into an attachment at attach time. |
| A7 | Inline connection fields remain supported indefinitely but stay S3-only; Azure is reachable only through a profile. Rollout is binaries-first, config-second. |
| A8 | Provider dispatch (handler registration and option building) keys off the profile kind, replacing the eight `starts_with("s3://")` comparisons -- including the one that today silently discards credentials for non-S3 URLs. |
