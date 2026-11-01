---
title: Email report
doc_id: factory:email-report
---

# Email report

## Overview

`email-report` renders a named sitegen report and sends it through Azure Communication Services Email.

## Example configuration

{{ source path="/docs/examples/email-report/default.yaml" language="yaml" /}}

## Use and behavior

Install with `pond mknod email-report /etc/email --config-path email.yaml`, then run `pond run /etc/email -- send`.

The `_env` fields are environment variable names, not `${env:NAME}` substitutions. Missing or empty values fail. A configured service, authorized sender and recipient, and an existing sitegen report are required. Retrying can send duplicate email.

<!-- docs:reference factory:email-report -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `access_key_env` | string | yes | not specified |  |
| `endpoint_env` | string | yes | not specified |  |
| `recipient_env` | string | yes | not specified |  |
| `reply_to` | string &#124; null | no | null |  |
| `report` | string | yes | not specified |  |
| `sender` | string | yes | not specified |  |
| `sitegen` | string | yes | not specified |  |
| `subject` | string | yes | not specified |  |
| `timeout` | string &#124; null | no | null |  |
<!-- docs:end-reference -->
