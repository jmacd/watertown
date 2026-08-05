# Storage Profiles: Naming Connections Instead of Copying Them

> **Status:** Design proposal (unimplemented). Follows
> [`rate-limiter-design.md`](rate-limiter-design.md), which established the
> pattern this reuses: a factory node declares a policy, a consumer names it by
> path, and the binding is validated at attach time.
>
> This document proposes a family of **storage-profile factories**
> (`storage-s3`, `storage-minio`, `storage-r2`, and later `storage-azure`) whose
> nodes live at `/sys/storage/<name>`, and an optional `storage:` field on
> `RemoteAttachment` that names one instead of carrying its own copy of the
> connection.

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

### 0.1 The concrete trigger

Converting `caspar.water/config/scripts/attach-remotes.sh` from CLI verbs to
declarative `pond apply` documents (Decision L11) ran aground on one field.
`pond apply` env-expands the **URL** only, deliberately: everything else is
stored as written so `${env:...}` references stay out of the replicated
document and resolve per-replica at use time (`apply.rs:425-440`). But
`allow_http` is a `bool`, so `allow_http: ${env:S3_ALLOW_HTTP}` cannot even
parse, and the shell script's `S3_ALLOW_HTTP` switch has nowhere to go.

The instinct is to add bool env-expansion. That would be fixing the symptom.
The real problem is that **`allow_http` is not a property of a remote.**
"Plain HTTP is acceptable" is a fact about the MinIO deployment on watershop.
It is not a fact about "the origin backup of the water pond," which is what
`/sys/remotes/origin` is supposed to describe. The field could not be expressed
per-environment because it was stored per-*remote*, in the wrong place
entirely.

### 0.2 What we want

1. One place to say "this is the MinIO on watershop," named by path.
2. Attachments that carry only what is genuinely theirs: the URL, the mode,
   the mount, the limits.
3. Illegal combinations that cannot be written down, rather than combinations
   that are merely discouraged by comments.
4. No weakening of the rule that keeps secrets out of replicated documents --
   in fact, a strengthening of it.
5. Full backward compatibility: every existing attachment document keeps
   working, unmodified.

---

## 1. Why a pond node, and why not just a config key

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
`${env:...}` reference, validated at `mknod` time, not just the secret. The
current rule covers `secret_access_key` alone; an access key id is a weaker
secret but still an identity, and there is no reason to accept a literal one.
`render` redacts credential fields regardless, so a profile node reads as
`secret_access_key: ${env:S3_SECRET_KEY}` and never as a value.

---

## 2. Not "auth" -- a profile

The natural name for this is an *auth* or *credentials* factory, and that name
is a trap, because it invites exactly the wrong scope.

If the node holds credentials only, then `endpoint`, `region`, and `allow_http`
stay on the attachment -- and §0.1, the thing that motivated the work, is not
fixed. Those three fields are not credentials, but they are precisely the
fields that were in the wrong place.

There is good precedent for the wider scope. An AWS **profile** in
`~/.aws/config` contains exactly this set: credentials, region, and endpoint,
under one name, referenced by name from everything that connects. That is the
same shape, solving the same problem.

**Decision A2.** The node is a *storage profile*: the complete description of
how to reach a storage system, credentials included. Nodes live at
`/sys/storage/<name>`; factories are named `storage-<provider>`.

---

## 3. The factories

**Decision A3.** One factory per provider, not one `storage` factory with
optional fields.

The honest counter-argument first: `storage-s3`, `storage-minio`, and
`storage-r2` are three profiles over *one* protocol, differing only in which
knobs they fix and which they expose. A single `storage-s3` factory with
optional `endpoint` and `allow_http` would cover all three with less code.

The reason to split them anyway is that optional fields make illegal states
representable. With one factory, `region: auto` (a Cloudflare-ism) combined
with an AWS endpoint parses fine and fails later; `allow_http: true` on an R2
profile parses fine and silently permits cleartext credentials to a service
that never needs it. With separate kinds, `storage-r2` fixes `region: auto` and
has no `allow_http` field at all, so neither mistake can be written down. That
is the same reasoning that made byte scales binary-only in the rate-limit unit
grammar (`rate_limit.rs:38-40`) and it follows
[`fallback-antipattern-philosophy.md`](fallback-antipattern-philosophy.md).

Azure settles it independently: its credentials are an account name plus key,
or a SAS token, or a connection string -- not an access-key/secret pair at all.
A single factory covering both would be a union type wearing a struct's
clothes.

### 3.1 `storage-minio`

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

`endpoint` is **required** (a MinIO without one is not a MinIO). `allow_http`
does not appear: a `storage-minio` profile permits plain HTTP by definition,
which is the whole reason the kind exists. An operator running MinIO behind
TLS simply writes an `https://` endpoint; permitting HTTP does not require it.

This is where `S3_ALLOW_HTTP` goes: it stops being a variable and becomes the
choice of `kind`.

### 3.2 `storage-s3`

```yaml
spec:
  factory: storage-s3
  config:
    region: ${env:AWS_REGION}
    access_key_id: ${env:AWS_ACCESS_KEY_ID}
    secret_access_key: ${env:AWS_SECRET_ACCESS_KEY}
```

No `endpoint` (AWS is the endpoint) and no `allow_http` (never correct against
AWS). Virtual-hosted-style addressing, the AWS default -- note that today
`to_storage_options` sets `virtual_hosted_style_request: false` whenever an
endpoint is present (`remote_config.rs:148-151`), which is a MinIO
accommodation currently applied by side effect of a field being non-empty.
Under profiles it becomes a property of the kind.

### 3.3 `storage-r2`

```yaml
spec:
  factory: storage-r2
  config:
    account_id: ${env:R2_ACCOUNT_ID}
    access_key_id: ${env:R2_ACCESS_KEY_ID}
    secret_access_key: ${env:R2_SECRET_KEY}
```

`region` is fixed to `auto` and is not a field. The endpoint is *derived* from
`account_id` (`https://<account>.r2.cloudflarestorage.com`) rather than pasted,
which removes a copy-paste failure mode that currently produces an
authentication error rather than a clear one. No `allow_http`.

This directly serves the `prod_s3` block in
`caspar.water/terraform/station/watershop/watershop.tf:17-23`, which is
retained for a future R2 re-enable and today differs from staging in exactly
the three ways this kind fixes.

### 3.4 `storage-azure` (later)

Listed to confirm the abstraction generalizes, not proposed for the first
implementation. Its config is an account name plus one of several credential
shapes, which is why it is a separate kind rather than an option set.

### 3.5 Registration

Each is a **leaf config node**, exactly like `rate-limit`: no content to
compute, nothing to execute, registered through
`register_dynamic_factory!` with a `validate` function that rejects a bad
profile at `pond apply` time rather than at first use
(`rate_limit.rs:398-403`). A shared `spec_from_config_bytes`-style entry point
gives the factory and its consumers exactly one interpretation of a node's
config, as `rate_limit.rs:391` does for budgets.

---

## 4. Referencing a profile from an attachment

### 4.1 Schema change

```rust
/// Storage profile governing how to reach `url`, as a pond path
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
adds a profile to an attachment that still has a stale inline endpoint gets a
working pond talking to the wrong storage. There is no reading of that document
whose intent is unambiguous, so it should not have a meaning.

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

Attach already validates the limiter bindings before touching pond state, on
the principle that a binding which silently fails to resolve is worse than no
binding (`remote.rs:179-183`). The profile reference gets the same treatment:
the node must exist, be a `storage-*` node, and be a kind compatible with the
URL scheme. A `pond://` or `file://` URL with a profile is an error -- neither
uses storage options at all, and accepting one would let an operator believe
credentials were in play when they were not.

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
`ResolvedStorage`; keep `to_storage_options(&ResolvedStorage)` pure.

Every one of the six call sites already holds a `Ship`, so this is a local
change at each, and the transfer path stays synchronous and `Ship`-free. It is
precisely the pattern the limiter used: bind up front with `&mut Ship`, pass
the resolved value into pure helpers
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

Credential fields are never printed, resolved or otherwise. A missing or
malformed profile is *reported* rather than propagated, as a broken limiter is,
because a broken profile is exactly when an operator needs `pond status` to
still work.

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
touch it.

---

## 8. Phasing

| Phase | Work | Verifies |
|---|---|---|
| A0 | `storage-minio` factory + `ResolvedStorage` + `storage:` field, inline path untouched | The whole idea, against the one provider actually in use |
| A1 | Attach-time validation, `pond status` line, redaction | The failure modes, before any deployment depends on it |
| A2 | Convert `attach-remotes.sh` to `pond apply` documents | §0.1, the thing that motivated this |
| A3 | `storage-s3` and `storage-r2` | The decomposition, against the `prod_s3` block that is waiting for it |
| A4 | `storage-azure` | The abstraction, if and when it is needed |

A2 is the payoff and should not be pulled earlier: converting the script before
profiles exist means writing YAML with duplicated inline credentials that A0
would immediately rewrite.

---

## 9. Testing

Following the limiter suite's shape (`crates/cmd/tests/test_apply_remote.rs`),
each test should pin a *behavior we would regret losing*:

- A profile and an inline field together are refused -- the ambiguity in A4
  never acquires a meaning by accident.
- A literal credential in a profile is refused at `mknod` time, for each kind.
- `render` and `pond status` never emit a resolved credential.
- An attachment naming a missing or wrong-kind profile is refused at attach.
- A `storage-minio` profile and the equivalent inline attachment produce
  **identical** storage options -- the refactor is a move, not a rewrite.
- A profile resolves per-replica: two replicas with different environments
  authenticate differently from the same replicated document.
- An existing attachment with no `storage:` field serializes byte-identically.
- End-to-end: `pond apply` a profile and a backup referencing it in one
  document, then push -- the ordering case, as limiters have.

---

## 10. Open questions

1. **`/sys/storage` vs. `/sys/auth`.** §2 argues for `storage` because the node
   carries connection facts that are not credentials, and calling it `auth`
   would invite someone to move them back out. If the credential emphasis is
   preferred, `/sys/auth` with the same wide scope is workable; only the name
   changes.
2. **Should a profile carry `limits`?** "This is the MinIO on watershop" and
   "this is what we will spend against it" are arguably one fact, and a
   station-wide byte budget shared by every attachment is a plausible thing to
   want. Against: limits are currently per-remote and a shared budget changes
   the enforcement model (one window, many spenders, cross-remote starvation).
   Recommend deferring until §A2 has produced real measurements.
3. **Should `url` move into the profile?** No -- the bucket is the one thing
   that genuinely differs per attachment. Noted only to record that it was
   considered.
4. **Anonymous / instance-role credentials.** Neither is used today. A profile
   kind with no credential fields at all is the natural expression if they
   ever are.

---

## 11. Summary of decisions

| # | Decision |
|---|---|
| A1 | Every credential field on a profile must be an `${env:...}` reference, validated at `mknod` time and redacted by `render` -- stricter than today's `secret_access_key`-only rule. |
| A2 | The node is a *storage profile* (connection + credentials), not an auth block, because the misplaced fields that motivated this are not credentials. Nodes at `/sys/storage/<name>`. |
| A3 | One factory per provider rather than one factory with optional fields, so that `region: auto` on AWS or `allow_http` on R2 cannot be written down. |
| A4 | Naming a profile alongside any inline connection field is a hard error, never a precedence rule. |
| A5 | Profiles resolve **once at the call site** into a `ResolvedStorage`; `to_storage_options` stays pure, so the transfer path stays synchronous and `Ship`-free. |
| A6 | `${env:...}` resolution stays at **use time, per replica**; a profile is never inlined into an attachment at attach time. |
| A7 | Inline connection fields remain supported indefinitely; rollout is binaries-first, config-second, and conversion is opportunistic. |
