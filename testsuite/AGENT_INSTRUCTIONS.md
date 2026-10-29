# Agent Instructions: Watertown Test Workflow

## The Goal

Test Watertown -> Classify results -> Fix the right layer -> Save passing tests

**Living documentation**: `docs/manual/` is authoritative for current public
interfaces. Follow `docs/README.md` and execute the exact fixtures in
`testsuite/examples/`. `docs/cli-reference.md` retains compatibility links and
historical context, not a second complete reference.

---

## THE LOOP

```
1. RUN TEST        -> ./run-test.sh 032  (or --interactive)
2. CLASSIFY        -> Success | Bug | Docs | UX | Design
3. DISCUSS         -> Present result to user, propose action
4. FIX             -> Code for bugs, docs for confusion
5. SAVE            -> Add passing test to tests/NNN-name.sh
```

---

## Running Tests

```bash
cd testsuite
./run-test.sh 032              # Run by number
./run-test.sh --interactive    # Explore manually
./run-test.sh --no-rebuild 032 # Skip image rebuild (faster iteration)
./run-test.sh --inspect 032    # Show output dir after run
```

---

## Test Conventions (MANDATORY)

Every test MUST follow this structure. The runner (`run-test.sh`) counts
`  ✓` and `  ✗` lines to produce the summary. Tests that skip this pattern
produce a misleading `=== PASSED ===` with no check count.

### Standard Test Template

```bash
#!/bin/bash
# EXPERIMENT: [Title]
# EXPECTED: [Outcome]
set -e
source check.sh    # <-- MANDATORY: shared assertion helpers

echo "=== Experiment: [Title] ==="

pond init   # or skip for hostmount tests

# ---- Setup ----

# ... create test data ...

# ---- Execute ----

# ... run the command under test ...

# ---- Verify ----

echo ""
echo "--- Verification ---"

check 'pond list / | grep -q "mydir"'  "directory exists"
check '[ -f /output/index.html ]'      "index.html generated"

check_contains  /output/index.html     "page has nav"      'class="nav"'
check_not_contains /output/page.html   "no debug output"   'DEBUG:'

check_finish   # <-- MANDATORY: prints summary, exits non-zero on failures
```

### The `check.sh` Helper

Source `check.sh` at the top of every test. It lives in `helpers/check.sh`
and is copied to `/usr/local/bin/check.sh` in the Docker image. It provides:

| Function | Purpose |
|----------|---------|
| `check 'COMMAND' "description"` | Passes if COMMAND exits 0. COMMAND is eval'd. |
| `check_contains FILE "desc" 'PATTERN'` | Passes if fixed-string PATTERN is in FILE |
| `check_not_contains FILE "desc" 'PATTERN'` | Passes if PATTERN is NOT in FILE |
| `check_finish` | Prints results summary, exits non-zero if any check failed |

Each function emits `  ✓ description` or `  ✗ description` (two leading spaces),
which is the format the runner counts.

### Rules

1. **Every assertion uses `check` / `check_contains` / `check_not_contains`.**
   Never write bare `if ! grep ...; then echo "FAIL"; exit 1; fi` blocks.

2. **Every test ends with `check_finish`.** This prints the results summary
   and exits non-zero if any check failed.

3. **`set -e` is still required** -- it catches setup failures (mkdir, pond init,
   etc.) before the verification section. The `check` functions don't trigger
   `set -e` because they handle their own exit codes internally.

4. **One concept per test.** Don't combine unrelated features.

5. **Verify with pond commands.** Always `pond list` / `pond cat` to confirm
   state, don't trust command exit codes alone.

### Why This Matters

The runner produces a one-line summary after each test:

```
=== PASSED 14/14 checks ===                        # Counted checks
=== PASSED ===                                      # No checks counted (BAD)
=== FAILED (exit: 1) -- 3/14 checks passed ===     # Shows exactly what broke
```

When checks are counted, failures are immediately visible in the summary
without having to read the full log. When they're not counted, you get a
bare `=== PASSED ===` that tells you nothing about what was actually verified.

---

## Classification

| Result | Meaning | Action |
|--------|---------|--------|
| **Success** | Works as documented | Save test, verify docs accurate |
| **BUG** | Code error | Report to user, propose proper fix |
| **DOCS** | Docs wrong/missing | Fix the authoritative `docs/manual/` page and shared fixture |
| **UX** | Works but confusing | Discuss before changing |
| **DESIGN** | Fundamental issue | Full discussion needed |

---

## Report Format

```markdown
### Test: [name/number]
**Result**: [Success/Bug/Docs/UX/Design]
**What Happened**: [brief]
**Classification**: [type] - [why]
**Proposed Action**: [specific]
**Decision Needed**: Approve / Defer / Discuss?
```

---

## Anti-Patterns

| NEVER | INSTEAD |
|-------|---------|
| Bare `if/grep/exit 1` assertions | Use `check` / `check_contains` |
| `echo "PASS: thing works"` | Use `check` (produces counted output) |
| Forget `check_finish` | Always end with `check_finish` |
| Guess CLI syntax | Read `docs/manual/reference/cli/commands.md` first |
| Skip verification | Always `pond list` / `pond cat` |
| Suppress errors | Use `set -e` |
| Test multiple things | One concept per test |
| Quick patch | Fix root cause properly |

---

## Reference Docs

- **CLI syntax**: `docs/manual/reference/cli/commands.md` (compiled reference)
- **Architecture**: `docs/watertown-overview.md`
- **Transactions**: `docs/watertown-system-patterns.md`

---

## Test Numbering

```
001-0xx: Core (init, mkdir, list, copy, cat, query)
1xx:     Synthetic factory tests (timeseries, join, pivot, reduce)
2xx:     Sitegen + complex pipelines
3xx:     Hostmount (no pond required)
4xx:     Multi-pond
5xx:     S3 / replication / remote
```

---

## Advanced Patterns

### Hostmount Tests (no pond)
```bash
#!/bin/bash
set -e
source check.sh

# No pond init -- hostmount operates on host filesystem
mkdir -p /tmp/testdir
echo "hello" > /tmp/testdir/file.txt

OUTPUT=$(pond list -d /tmp/testdir "host+file:///*" 2>&1)

check 'echo "$OUTPUT" | grep -q "file.txt"'  "host file listed"
check_finish
```

### Multi-Pond
```bash
POND=/pond1 pond init
POND=/pond2 pond init
POND=/pond1 pond mkdir /data
```

### S3/MinIO
```bash
docker-compose up -d minio
docker-compose run --rm watertown ./tests/500-*.sh
```

Environment: `MINIO_ENDPOINT=http://minio:9000`, user/pass: `minioadmin`
