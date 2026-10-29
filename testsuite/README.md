# Watertown Test Framework

> See [AGENT_INSTRUCTIONS.md](AGENT_INSTRUCTIONS.md) for the full testing workflow.

The [manual authoring workflow](../docs/README.md) maps the four pilot pages
to reusable [example packages](examples/README.md) and numbered assertions.
Update migrated topics in `docs/manual/`; other topics remain in the existing
reference until individually reviewed and migrated.

## Quick Start

```bash
cd testsuite

# Build container (first time)
./build-image.sh

# Run a test by number
./run-test.sh 032

# Interactive mode
./run-test.sh --interactive
```

## Directory Structure

```
testsuite/
├── tests/              # Saved passing tests (NNN-name.sh)
├── results/            # Failure/confusion reports
├── active/             # Work-in-progress tests
├── run-test.sh         # Main test runner
├── AGENT_INSTRUCTIONS.md  # Full workflow instructions
├── WORKFLOW.md         # Iteration loop
└── BACKLOG.md          # Test queue
```

## The Loop

Every session follows:

```
RUN TEST → CLASSIFY → DISCUSS → FIX → SAVE
```

| Result | Action |
|--------|--------|
| ✅ Success | Save to `tests/`, update `docs/cli-reference.md` if needed |
| ❌ Bug | Report to user, propose proper fix |
| ❓ Docs wrong | Fix `docs/cli-reference.md` immediately |
| 🤷 UX confusing | Discuss before changing |

## Container Environment

- `pond` binary at `/usr/local/bin/pond`
- Fresh `/pond` directory each run
- `POND=/pond` environment variable preset
- S3/MinIO available via docker-compose
