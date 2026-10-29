---
title: Monitoring and email reports
doc_id: reference:reporting
---

# Monitoring and email reports

These are executable factories with different responsibilities. A successful
schema check is not a successful measurement, alarm evaluation, or mail send.
Install with `pond mknod FACTORY /etc/NAME --config-path FILE`; update using
the same command plus `--overwrite`. Inspect rendered artifacts and command
diagnostics rather than treating an installed node as evidence of delivery.

## Monitor report

`monitor-report` evaluates configured checks against a selected pond and
writes report artifacts under `output_dir`. Its checks include observed-value,
staleness, range, scored, and conditional-rate calculations. The generated
tagged/untagged alternatives below define each accepted check shape; they are
not interchangeable arbitrary SQL expressions. Source columns, windows,
alignment tolerances, thresholds, and links must agree with the source data.

Minimum shape (no checks, therefore not a useful monitor):

```yaml
pond: /path/to/pond
title: Example monitor
output_dir: /path/to/report
checks: []
```

Run with `pond run /etc/monitor -- push`. Control-reader execution supports
the designated `push` operation; it is not a general mutation API. Nonempty
checks require real source data and semantic validation. Missing/stale data
must be interpreted according to each check's contract, not as a healthy zero.
This reference does not certify an unattended alerting system.

Scalar checks identify a card with unique `id`, display `label` /
`description`, and a site-relative/absolute `href`. `source` names queryable
data; `timestamp_column` and `value_column` select its time and numeric value,
and `unit` is a display suffix. Windows/tolerances/minimum-active-time are
positive duration strings, while thresholds/bounds use the source's units.

| Shape / type | Alarm/evaluation meaning |
|---|---|
| Untagged `BelowCheckConfig` | All observed values below `threshold` over `window`; an empty observation set is not silently healthy. |
| `any-observed-below` | Any observed value below the threshold. |
| `average-range` | Average compared with ordered `lower_bound` / `upper_bound`. |
| `stale-after` | Zero non-null samples in the window is the alarm condition. |
| `stale-or-below` | Missing/stale samples or any observed value below threshold. |
| `stale-or-all-below` | No observations in `stale_window`, or all observed values below threshold in the shorter `window`. |
| `stale-or-below-any-column` | Primary-column staleness takes priority; otherwise any low sample in `threshold_columns` alarms. |
| `graded-score` | Latest score classified by strictly increasing `good_max`, `watch_max`, `concern_max`; no data or above concern is alarm. |
| `rate-while` | Accumulate positive measurement deltas while a condition predicate holds, using previous-value alignment within its tolerance; enforce `minimum_active_time` before comparing with the configured alarm. |

For `rate-while`, nested measurement/condition sources have their own time
column (`timestamp` by default). Predicates currently use equality; alignment
currently uses `previous`. Source/order/type checks and unique IDs/hrefs are
semantic validation beyond schema. Rendered reports are snapshots, not a
replacement for checking collector health or verifying alert delivery.

<!-- docs:reference factory:monitor-report -->
## Configuration reference

Generated from the Rust **deserialization** contract. This describes shape, not all semantic validation, service prerequisites, or successful execution.

| Capability | Registered |
|---|---|
| directory | no |
| file | no |
| initialize | yes |
| execute | yes |
| table transform | no |
| raw validation | no |

Automatic post-commit access: `ReadOnly`.

### MonitorConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `checks` | array of CheckConfig | yes | not specified |  |
| `output_dir` | string | yes | not specified |  |
| `pond` | string | yes | not specified |  |
| `title` | string | yes | not specified |  |

Unknown fields: **rejected**.

### Accumulation

Accepted value: "positive-deltas".

### AlarmConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `operator` | AlarmOperator | yes | not specified |  |
| `unit` | string | no | "" |  |
| `value` | number | yes | not specified | Constraints: format="double". |

Unknown fields: **rejected**.

### AlarmOperator

Accepted value: "lt".

### AlignmentConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `method` | AlignmentMethod | yes | not specified |  |
| `tolerance` | string | yes | not specified |  |

Unknown fields: **rejected**.

### AlignmentMethod

Accepted value: "previous".

### AnyObservedBelowCheckConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `description` | string | yes | not specified |  |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `threshold` | number | yes | not specified | Constraints: format="double". |
| `timestamp_column` | string | no | "timestamp" |  |
| `type` | AnyObservedBelowType | yes | not specified |  |
| `unit` | string | no | "" |  |
| `value_column` | string | yes | not specified |  |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### AnyObservedBelowType

Accepted value: "any-observed-below".

### AverageRangeCheckConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `description` | string | yes | not specified |  |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `lower_bound` | number | yes | not specified | Constraints: format="double". |
| `source` | string | yes | not specified |  |
| `timestamp_column` | string | no | "timestamp" |  |
| `type` | AverageRangeType | yes | not specified |  |
| `unit` | string | no | "" |  |
| `upper_bound` | number | yes | not specified | Constraints: format="double". |
| `value_column` | string | yes | not specified |  |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### AverageRangeType

Accepted value: "average-range".

### BelowCheckConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `description` | string | yes | not specified |  |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `threshold` | number | yes | not specified | Constraints: format="double". |
| `timestamp_column` | string | no | "timestamp" |  |
| `unit` | string | no | "" |  |
| `value_column` | string | yes | not specified |  |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### CheckConfig

### CheckConfig variant 1

Accepted value: RateWhileCheckConfig.

### CheckConfig variant 2

Accepted value: AnyObservedBelowCheckConfig.

### CheckConfig variant 3

Accepted value: AverageRangeCheckConfig.

### CheckConfig variant 4

Accepted value: BelowCheckConfig.

### CheckConfig variant 5

Accepted value: StaleAfterCheckConfig.

### CheckConfig variant 6

Accepted value: GradedScoreCheckConfig.

### CheckConfig variant 7

Accepted value: StaleOrBelowCheckConfig.

### CheckConfig variant 8

Accepted value: StaleOrAllBelowCheckConfig.

### CheckConfig variant 9

Accepted value: StaleOrBelowAnyColumnCheckConfig.

### ConditionConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `predicate` | PredicateConfig | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `timestamp` | string | no | "timestamp" |  |

Unknown fields: **rejected**.

### GradedScoreCheckConfig

Classifies the latest sample of a continuous 0.0-1.0-style score into one of four quality bands (good/watch/concern/alarm) instead of a plain pass/fail threshold, for signals like the leak-probability score where a single alarm/healthy split loses the gradient the operator cares about. good_max &lt; watch_max &lt; concern_max must hold; values above concern_max, and a missing/no-data window, both land in the worst (alarm) band -- absence of a recent score is treated as the worst case, not as "unknown", so a quiet source can't hide a real leak behind a gray card.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `concern_max` | number | yes | not specified | Constraints: format="double". |
| `description` | string | yes | not specified |  |
| `good_max` | number | yes | not specified | Constraints: format="double". |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `timestamp_column` | string | no | "timestamp" |  |
| `type` | GradedScoreType | yes | not specified |  |
| `unit` | string | no | "" |  |
| `value_column` | string | yes | not specified |  |
| `watch_max` | number | yes | not specified | Constraints: format="double". |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### GradedScoreType

Accepted value: "graded-score".

### MeasurementConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `accumulation` | Accumulation | yes | not specified |  |
| `column` | string | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `timestamp` | string | no | "timestamp" |  |

Unknown fields: **rejected**.

### PredicateConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `column` | string | yes | not specified |  |
| `operator` | PredicateOperator | yes | not specified |  |
| `value` | string | yes | not specified |  |

Unknown fields: **rejected**.

### PredicateOperator

Accepted value: "eq".

### RateWhileCheckConfig

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `alarm` | AlarmConfig | yes | not specified |  |
| `alignment` | AlignmentConfig | yes | not specified |  |
| `condition` | ConditionConfig | yes | not specified |  |
| `description` | string | yes | not specified |  |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `measurement` | MeasurementConfig | yes | not specified |  |
| `minimum_active_time` | string | yes | not specified |  |
| `type` | RateWhileType | yes | not specified |  |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### RateWhileType

Accepted value: "rate-while".

### StaleAfterCheckConfig

Alarms when a source has produced zero non-null samples within window. Unlike the other scalar checks, an empty sample set here *is* the alarm condition rather than an unknown/indeterminate state: it means the data source has stopped producing new data.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `description` | string | yes | not specified |  |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `timestamp_column` | string | no | "timestamp" |  |
| `type` | StaleAfterType | yes | not specified |  |
| `unit` | string | no | "" |  |
| `value_column` | string | yes | not specified |  |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### StaleAfterType

Accepted value: "stale-after".

### StaleOrAllBelowCheckConfig

Alarms if the source has produced zero samples at all in the longer stale_window (ingestion has stalled), or if every sample observed within the shorter window is below threshold -- combining the staleness and sustained-low-value failure modes of a single instrument (e.g. well depth) into one check instead of requiring two separate cards. Unlike stale-or-below, this uses "all observed" polarity: a single recent reading at or above threshold is enough to stay healthy.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `description` | string | yes | not specified |  |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `stale_window` | string | yes | not specified |  |
| `threshold` | number | yes | not specified | Constraints: format="double". |
| `timestamp_column` | string | no | "timestamp" |  |
| `type` | StaleOrAllBelowType | yes | not specified |  |
| `unit` | string | no | "" |  |
| `value_column` | string | yes | not specified |  |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### StaleOrAllBelowType

Accepted value: "stale-or-all-below".

### StaleOrBelowAnyColumnCheckConfig

Combines a staleness check on value_column (zero samples within window means the source has stopped reporting, same polarity as stale-after) with a threshold check spread across one or more secondary threshold_columns -- alarm if ANY sample on ANY of those columns is below threshold within the same window -- into a single check/card. Useful when a device's primary signal and some auxiliary signals (e.g. two redundant radio batteries) should be reported as one pass/fail instead of three separate cards. Staleness on the primary column is checked first and takes priority: if the device has stopped reporting at all, the secondary columns (which live on the same stalled device) aren't evaluated separately.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `description` | string | yes | not specified |  |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `threshold` | number | yes | not specified | Constraints: format="double". |
| `threshold_columns` | array of string | yes | not specified |  |
| `timestamp_column` | string | no | "timestamp" |  |
| `type` | StaleOrBelowAnyColumnType | yes | not specified |  |
| `unit` | string | no | "" |  |
| `value_column` | string | yes | not specified |  |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### StaleOrBelowAnyColumnType

Accepted value: "stale-or-below-any-column".

### StaleOrBelowCheckConfig

Alarms if the source has produced zero samples in window (stale or missing), or if any observed sample is below threshold -- combining the staleness and low-value failure modes of a single instrument (e.g. a pH probe) into one check instead of requiring two separate cards.

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `description` | string | yes | not specified |  |
| `href` | string | yes | not specified |  |
| `id` | string | yes | not specified |  |
| `label` | string | yes | not specified |  |
| `source` | string | yes | not specified |  |
| `threshold` | number | yes | not specified | Constraints: format="double". |
| `timestamp_column` | string | no | "timestamp" |  |
| `type` | StaleOrBelowType | yes | not specified |  |
| `unit` | string | no | "" |  |
| `value_column` | string | yes | not specified |  |
| `window` | string | yes | not specified |  |

Unknown fields: **rejected**.

### StaleOrBelowType

Accepted value: "stale-or-below".
<!-- docs:end-reference -->

## Email report

`email-report` renders a named [sitegen report](../sitegen/configuration.md#reports-and-subsites)
and sends it through Azure Communication Services Email:

```yaml
sitegen: /etc/site.yaml
report: daily
endpoint_env: ACS_EMAIL_ENDPOINT
access_key_env: ACS_EMAIL_ACCESS_KEY
recipient_env: REPORT_RECIPIENT
sender: reports@example.invalid
subject: Daily report
```

The sample uses placeholder addresses and requires a real authorized service,
sender, recipient, environment values, and an existing sitegen YAML/report.
Run `pond run /etc/email -- send` in a writable pond. The three `_env` fields
are environment **names**, unlike `${env:NAME}` substitutions. Values are
resolved when sending and missing/empty values fail. Sender/reply-to/recipient
addresses and timeout are validated. Static chart PNGs become inline email
attachments. Sending is an external side effect; retrying can send duplicate
mail. No live-service end-to-end coverage is claimed here.

<!-- docs:reference factory:email-report -->
## Configuration reference

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
