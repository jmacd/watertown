---
title: Storage profiles and budgets
doc_id: reference:storage
---

# Storage profiles and budgets

These are declarative dynamic nodes referenced by other commands, not
`pond run` collectors. Install using `pond mknod FACTORY /etc/NAME
--config-path FILE`; update with the same command plus `--overwrite`.
Registration/validation does not connect to a service or prove authorization.

## MinIO

`storage-minio` declares an endpoint and credential references:

```yaml
endpoint: http://127.0.0.1:9000
access_key_id: "${env:MINIO_ACCESS_KEY}"
secret_access_key: "${env:MINIO_SECRET_KEY}"
region: us-east-1
```

Use an `s3://BUCKET/PREFIX` backup URL and `storage: /etc/minio` in a
`kind: backup` apply document's `spec`. The CLI add command has no `--storage`
flag; profiles are a declarative apply surface. The endpoint must be HTTP(S); credentials must remain
`${env:NAME}` references in the raw persisted config. Literal secret values
are rejected by raw validation. Expanded credential values are validated
separately at use time. Never place real keys in examples, Git, or generated
reference. A local MinIO service plus a provisioned bucket is required for
service-level execution.

<!-- docs:reference factory:storage-minio -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | yes |
| initialize | no |
| execute | no |
| table transform | no |
| raw validation | yes |

Automatic post-commit access: `not registered`.

### StorageMinioConfig

On-disk config for a storage-minio node.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `access_key_id` | string | yes | not specified | Access key id.  Must be an ${env:...} reference (Decision A1). |
| `endpoint` | string | yes | not specified | MinIO endpoint URL, e.g. http://watershop:9000.  Required: a MinIO without an endpoint is not a MinIO. |
| `region` | string &#124; null | no | not specified | Region used for SigV4 signing.  Defaults to [DEFAULT_REGION], which [render] makes explicit so pond cat shows what is in effect. |
| `secret_access_key` | string | yes | not specified | Secret access key.  Must be an ${env:...} reference (Decision A1). |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Azure

`storage-azure` declares the account and exactly one credential shape:
`account_key`, `sas_token`, or `service_principal` with its nested fields.

```yaml
account_name: exampleaccount
account_key: "${env:AZURE_STORAGE_KEY}"
```

Use an `az://CONTAINER/PREFIX` attachment and `storage: /etc/azure` in its
declarative apply spec.
Credential strings must be environment references in raw config; missing,
mixed, or incomplete credential shapes fail. `timeout` is a duration string.
Real account/container provisioning and cloud authorization are prerequisites.
No live Azure execution is claimed by this documentation example.

<!-- docs:reference factory:storage-azure -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | yes |
| initialize | no |
| execute | no |
| table transform | no |
| raw validation | yes |

Automatic post-commit access: `not registered`.

### StorageAzureConfig

On-disk config for a storage-azure node.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `account_key` | string &#124; null | no | not specified | Shared account key.  Must be an ${env:...} reference (Decision A1). |
| `account_name` | string | yes | not specified | Storage account name, e.g. casparwater.  Required: an Azure profile without an account names nothing.  Not a credential (see module docs). |
| `sas_token` | string &#124; null | no | not specified | Shared access signature token.  Must be an ${env:...} reference. |
| `service_principal` | ServicePrincipal &#124; null | no | not specified | Azure AD application credentials.  Each field must be an ${env:...} reference. |
| `timeout` | string &#124; null | no | not specified | Per-request HTTP timeout accepted by object_store (for example 2m). Azure multipart uploads can otherwise exceed the 30-second default on a constrained uplink when several parts are in flight. |

Unknown fields: **rejected**.

### ServicePrincipal

An Azure AD service principal: the credential shape used when a pond authenticates as an application rather than with a shared secret.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `client_id` | string | yes | not specified | Application (client) id.  Must be an ${env:...} reference. |
| `client_secret` | string | yes | not specified | Client secret.  Must be an ${env:...} reference. |
| `tenant_id` | string | yes | not specified | Directory (tenant) id.  Must be an ${env:...} reference. |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Rate limit

`rate-limit` declares a consumption budget:

```yaml
unit: bytes
limit: 10 MiB/day
```

The `unit` dimension is `bytes` or `ops`. The human-readable `limit` must use
a compatible amount and period; byte units and operation units cannot be
mixed. `burst` is a capacity in the same dimension, not another rate.
Positive, finite, representable policy values are checked semantically.
Declaring a node does not globally throttle arbitrary I/O: an action must
explicitly consume the budget. Inspect consumers' behavior before treating
a budget as a hard resource-safety boundary.

<!-- docs:reference factory:rate-limit -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | yes |
| initialize | no |
| execute | no |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `not registered`.

### RateLimitConfig

On-disk config for a rate-limit node.  The unit is one string; the magnitudes are numbers (Decision L4a).

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `burst` | number &#124; null | no | not specified | Optional instantaneous allowance, in the **scale** component of unit only -- the period does not apply to a burst.  unit: MiB/day with burst: 1 permits 1 MiB to be spent faster than the smoothed rate.  Defaults to one period's worth of limit, i.e. a pure sliding window with no extra allowance. Constraints: format="double". |
| `limit` | number | yes | not specified | How many units per period are permitted.  unit: MiB/day with limit: 10 means 10 MiB per day. Constraints: format="double". |
| `unit` | string | yes | not specified | &lt;scale&gt;/&lt;period&gt;, e.g. MiB/day, iops/second, B/hour. Carries dimension, scale, and period together. |

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Inspection, state, and evidence

Use `pond list /etc` and `pond cat /etc/NAME` to inspect declared configuration;
do not publish output containing expanded secrets. `pond backup list` /
`remote list` show attachments; `status` reports replication progress.
Credential/profile updates affect later use, not already-created objects.
Budgets and replication retain state outside the YAML shape.
These samples are configuration reference, not a successful cloud restore
claim. Start with the asserted [file-backed recovery example](../../guides/operations.md)
before configuring external storage.
