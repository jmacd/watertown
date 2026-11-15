---
title: MinIO storage profile
doc_id: factory:storage-minio
---

# MinIO storage profile

## Overview

`storage-minio` declares an S3-compatible endpoint and credential references for use by backup and replication attachments.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod storage-minio /etc/minio --config-path minio.yaml`. Reference it as `storage: /etc/minio` in a declarative backup apply document using an `s3://BUCKET/PREFIX` URL.

The CLI add command has no `--storage` flag. Raw credentials must remain `${env:NAME}` references; literal secrets are rejected. Service-level use requires MinIO and a provisioned bucket.

<!-- docs:reference factory:storage-minio -->
<!-- docs:end-reference -->
