---
title: Email report
doc_id: factory:email-report
---

# Email report

## Overview

`email-report` renders a named sitegen report and sends it through Azure Communication Services Email.

## Example configuration

```yaml
sitegen: /etc/site.yaml
report: daily
endpoint_env: ACS_EMAIL_ENDPOINT
access_key_env: ACS_EMAIL_ACCESS_KEY
recipient_env: REPORT_RECIPIENT
sender: reports@example.invalid
subject: Daily report
```

## Use and behavior

Install with `pond mknod email-report /etc/email --config-path email.yaml`, then run `pond run /etc/email -- send`.

The `_env` fields are environment variable names, not `${env:NAME}` substitutions. Missing or empty values fail. A configured service, authorized sender and recipient, and an existing sitegen report are required. Retrying can send duplicate email.

<!-- docs:reference factory:email-report -->
## Full configuration

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | yes |
| execute | yes |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `ReadWrite`.

### EmailReportConfig

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

Unknown fields: **rejected**.
<!-- docs:end-reference -->

## Evidence and limitations

The sample is configuration guidance, not a claimed runnable documentation fixture. The generated schema is the deserialization contract; semantic validation, source data, platform support, credentials, and external side effects still apply. Keep original source data independently recoverable.
