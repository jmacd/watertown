---
title: Hledger journal
doc_id: format:hledger
---

# Hledger journal

`hledger` reads a single accounting journal into posting rows. It is a parser,
not the hledger executable or a running-balance engine.

## URL example

```bash
# Replace the example host path with your journal.
# batch_size=8192 is the default and the only provider-specific option.
pond cat 'host+hledger:///tmp/accounts.journal?batch_size=8192' --format=table
```

Use `hledger:///accounts.journal?batch_size=8192` for pond input. Quote URLs
containing query strings. URL option values are parsed strings, not YAML;
the generated reference describes the resulting typed options.

<!-- docs:reference format:hledger -->
## Full configuration

### Settings

| Field | Accepted type | Required | Default | Description |
|---|---|---|---|---|
| `batch_size` | integer | no | 8192 | Constraints: format="uint"; minimum=0. |
<!-- docs:end-reference -->

## Output and behavior

Text and parsing are buffered; parsed posting rows are converted in batches.
Amounts use exact Decimal128(38,18); values requiring excess precision fail
rather than round. Dates use Date32. Output includes source, transaction
sequence, date, status, posting status, code, payee, description, account,
commodity, amount, `amount_inferred` (Boolean), nullable transaction/posting
comments, and discovered tag columns (nullable Utf8). Inspect with
`pond describe` for the complete current schema.

## Errors and limitations

Unsupported journal features are errors, not an instruction to execute
includes or external commands. Unknown options and zero batch size fail.
Use explicit `+gzip`, `+zstd`, or `+bzip2` layers for compressed input; this
does not make parsing constant-memory. See [shared URL rules](../concepts.md).
Parser unit tests exist; the URL example is not a runnable documentation fixture.
