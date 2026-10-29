---
title: Global CLI options
doc_id: cli:global-options
---

# Global CLI options

`--pond` overrides `POND` for pond-backed operations. Host reads use explicit
`host+` URLs and need no pond. `-d` roots host paths, not pond paths.
`--hostmount` declares supported dynamic host factory mounts; it does not
enable arbitrary pond-writing executables. `RUST_LOG` controls diagnostics.
There is no default automatic fallback from a missing pond to host mode.

<!-- docs:reference cli:global-options -->
## Command interface

```text
pond
```

Watertown - A very small data lake

| Argument / option | Accepted values | Required | Default | Description |
|---|---|---|---|---|
| -d, --directory | 1 value(s) | no | not specified | Host directory root for host+ URL operations (defaults to none; when set, host+ paths are resolved relative to this directory) |
| -h, --help | flag (no value) | no | not specified | Print help |
| --hostmount | 1 value(s) | no | not specified | Mount factory definitions onto host filesystem paths. Format: &lt;mount_path&gt;=host+&lt;factory&gt;:///&lt;config_path&gt; Example: --hostmount /reduced=host+dyndir:///reduce.yaml |
| --pond | 1 value(s) | no | not specified | Pond path override (defaults to POND env var) |
| -V, --version | flag (no value) | no | not specified | Print version |

Global options are documented in [Global options](global-options.md).
<!-- docs:end-reference -->

See [all commands](commands.md), [shared concepts](../concepts.md), and the
asserted [CSV example](../formats/csv.md). Quote shell globs/URLs. Use `--`
before factory-specific arguments when they include options such as
`build OUTPUT --strict`.
