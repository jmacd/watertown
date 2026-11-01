---
title: MinIO storage profile
doc_id: factory:storage-minio
---

# MinIO storage profile

## Overview

`storage-minio` declares an S3-compatible endpoint and credential references for use by backup and replication attachments.

## Example configuration

{{ source path="/docs/examples/storage-minio/default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod storage-minio /etc/minio --config-path minio.yaml`. Reference it as `storage: /etc/minio` in a declarative backup apply document using an `s3://BUCKET/PREFIX` URL.

The CLI add command has no `--storage` flag. Raw credentials must remain `${env:NAME}` references; literal secrets are rejected. Service-level use requires MinIO and a provisioned bucket.

<!-- docs:reference factory:storage-minio -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `access_key_id` | string | yes | not specified | Access key id.  Must be an ${env:...} reference (Decision A1). |
| `endpoint` | string | yes | not specified | MinIO endpoint URL, e.g. http://watershop:9000.  Required: a MinIO without an endpoint is not a MinIO. |
| `region` | string &#124; null | no | not specified | Region used for SigV4 signing.  Defaults to [DEFAULT_REGION], which [render] makes explicit so pond cat shows what is in effect. |
| `secret_access_key` | string | yes | not specified | Secret access key.  Must be an ${env:...} reference (Decision A1). |
<!-- docs:end-reference -->
