---
title: Backup, recovery, and maintenance
doc_id: guide:operations
---

# Backup, recovery, and maintenance

Original source systems remain authoritative. Keep independently recoverable
source exports and test restoration before relying on any backup. A copied
data export, a replicated pond, and a recovery capsule have different
contracts; none should be the only recoverable source.

## Reproducible recovery

This offline package uses only fresh scratch directories and a `file://`
backup. Bash and `pond` are required. No cloud credentials are used.

{{ source path="/testsuite/examples/pond-recovery/run.sh" language="bash" /}}

Run `bash testsuite/examples/pond-recovery/run.sh`; the
[package](/downloads/pond-recovery.tar) includes its shared CSV inputs.
Test `214` asserts restored bytes, local `fsck`, matching content tips,
idempotent pull, a subsequent replicated update, and refusal to overwrite an
existing pond. Cross-replica `fsck` roots are **not** an identity invariant:
compare content tips and independently check each local structure.

## Backup versus import

`backup add NAME URL` attaches a whole-pond push destination (optionally
bidirectional). Successful commits can auto-push; explicit `push NAME`
provides an observable synchronization point. Failures require investigation:
do not equate committed local data with a confirmed remote publication.

`remote add NAME URL /imports/NAME` attaches another pond read-only beneath
that mount; `pull NAME` refreshes its imported snapshot. It retains the
consumer's identity. A root mirror must match identity; use `restore` for a
fresh whole-pond replica, **not** `init` followed by importing your own backup.
Detaching keeps the materialized import snapshot; `remote remove --purge`
also removes its mount. `pull --rebuild-graft NAME` atomically replaces a
graft/mount/pin; it is not permission to change source identity.

## Recovery capsules and activation

Current portable recovery uses `pondcapsule.4`. `capsule recipe publish NAME`
installs the native-format recovery recipe; `recipe inspect` verifies it.
`capsule publish NAME` publishes a snapshot. Download the complete recovery
tree and referenced data, then `capsule inspect PATH` / `verify PATH` before
import. These operate on downloaded recovery input, not a host remote factory.

`capsule import PATH --birthplace LABEL` requires an absent target, creates a
fresh identity in private sibling staging, and resumes matching staged work
on retry. It verifies the logical contract before atomic promotion. Failures
leave staging for inspection. Imported ponds remain inert: automatic factory
dispatch and remote auto-push are suppressed. Audit all restored remote/config
references and credentials before `capsule activate`; its safety preflight
must pass before enabling side effects. An imported capsule is not an ordinary
same-identity mirror restore. Capsule/service-specific procedures have separate
tests; the file-backed package above does not claim capsule or cloud execution.

## Inspection and maintenance

Start with `status`, `log --incomplete`, local `fsck`, and `tlog verify`.
`verify NAME --exact` additionally requires equality of local and remote tips,
not just compatible content. `recover` handles interrupted transaction
metadata; `rebuild-control` reconstructs a lost control table from data.
Preserve evidence before either recovery operation.

`freeze enable --reason TEXT` persistently blocks data writes and records
the protected content tip; `freeze status` inspects it and `disable` re-enables
writes. Freeze is not a backup or a guarantee against external host changes.

`maintain` checkpoints/vacuums local Delta state. Optional compaction,
control-history pruning, and series-pack consolidation are separate choices.
Begin with supported `--dry-run` inspection; pruning without a remote can make
history irrecoverable. `--collapse-versions` defaults to disabled and does not
rewrite logical series rows. Publishing consolidated packs is an explicit
`backup publish-consolidated NAME` step after verification, not ordinary push.
`backup cleanup-uploads` deletes abandoned staging after its grace period.
Emergency bucket erase is destructive and never a recovery step.

See the [generated CLI](../reference/cli/commands.md) for precise flags and
the [storage profiles](../reference/factories/storage.md) for service prerequisites.
