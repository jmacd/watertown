---
title: Azure storage profile
doc_id: factory:storage-azure
---

# Azure storage profile

## Overview

`storage-azure` declares an Azure account and exactly one credential shape for backup and replication attachments.

## Example configuration

{{ source path="/docs/examples/storage-azure/default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod storage-azure /etc/azure --config-path azure.yaml`. Reference it as `storage: /etc/azure` in a declarative attachment using an `az://CONTAINER/PREFIX` URL.

Choose exactly one of `account_key`, `sas_token`, or `service_principal`. Credential fields must be environment references. Real account, container, and cloud authorization are prerequisites.

<!-- docs:reference factory:storage-azure -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `account_key` | string &#124; null | no | not specified | Shared account key.  Must be an ${env:...} reference (Decision A1). |
| `account_name` | string | yes | not specified | Storage account name, e.g. casparwater.  Required: an Azure profile without an account names nothing.  Not a credential (see module docs). |
| `sas_token` | string &#124; null | no | not specified | Shared access signature token.  Must be an ${env:...} reference. |
| `service_principal` | Service Principal &#124; null | no | not specified | Azure AD application credentials.  Each field must be an ${env:...} reference. |
| `timeout` | string &#124; null | no | not specified | Per-request HTTP timeout accepted by object_store (for example 2m). Azure multipart uploads can otherwise exceed the 30-second default on a constrained uplink when several parts are in flight. |

### Service Principal

An Azure AD service principal: the credential shape used when a pond authenticates as an application rather than with a shared secret.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `client_id` | string | yes | not specified | Application (client) id.  Must be an ${env:...} reference. |
| `client_secret` | string | yes | not specified | Client secret.  Must be an ${env:...} reference. |
| `tenant_id` | string | yes | not specified | Directory (tenant) id.  Must be an ${env:...} reference. |
<!-- docs:end-reference -->
