---
title: Azure storage profile
doc_id: factory:storage-azure
---

# Azure storage profile

## Overview

`storage-azure` declares an Azure account and exactly one credential shape for backup and replication attachments.

## Example configuration

{{ source path="default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod storage-azure /etc/azure --config-path azure.yaml`. Reference it as `storage: /etc/azure` in a declarative attachment using an `az://CONTAINER/PREFIX` URL.

Choose exactly one of `account_key`, `sas_token`, or `service_principal`. Credential fields must be environment references. Real account, container, and cloud authorization are prerequisites.

<!-- docs:reference factory:storage-azure -->
<!-- docs:end-reference -->
