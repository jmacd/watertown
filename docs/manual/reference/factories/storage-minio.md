---
title: MinIO storage profile
doc_id: factory:storage-minio
---

# MinIO storage profile

## Overview

`storage-minio` declares an S3-compatible endpoint and credential references for use by backup and replication attachments.

## Example configuration

```yaml
endpoint: http://127.0.0.1:9000
access_key_id: "${env:MINIO_ACCESS_KEY}"
secret_access_key: "${env:MINIO_SECRET_KEY}"
region: us-east-1
```

## Use and behavior

Install with `pond mknod storage-minio /etc/minio --config-path minio.yaml`. Reference it as `storage: /etc/minio` in a declarative backup apply document using an `s3://BUCKET/PREFIX` URL.

The CLI add command has no `--storage` flag. Raw credentials must remain `${env:NAME}` references; literal secrets are rejected. Service-level use requires MinIO and a provisioned bucket.

<!-- docs:reference factory:storage-minio -->
## Full configuration

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

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
