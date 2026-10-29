# Executable manual fixtures

Each package contains the exact inputs and `run.sh` included by sitegen.
Prerequisites: Bash and a compatible `pond` on `PATH`. No services or
credentials are needed for the pilot. Run from any directory:

```bash
bash testsuite/examples/csv-query/run.sh
bash testsuite/examples/synthetic-timeseries/run.sh
```

By default scripts create a fresh `mktemp` workspace and print its path.
They never use the caller's `POND`. To retain outputs at a chosen location,
set `EXAMPLE_OUTPUT` to an **absent** directory; scripts refuse existing paths.
Inputs are read relative to the script, not the current directory. Numbered
tests run these same scripts, then assert results with `check.sh`.

The harness mounts this tree read-only at `/examples` in ordinary and Compose
containers, sets `EXAMPLES_ROOT=/examples`, and mounts repository sources
read-only at `/repo` for the pilot build. Native/host-required tests receive
absolute `EXAMPLES_ROOT` and `REPO_ROOT`; create a separate writable workspace
and never edit fixtures. Download archives are produced directly from these
directories by `make docs-build`, not maintained as duplicate inputs.

Region markers are whole comment lines, `# docs:start NAME` and
`# docs:end NAME`. Regions cannot nest or repeat. Keep prerequisites and setup
visible when including only a region.
