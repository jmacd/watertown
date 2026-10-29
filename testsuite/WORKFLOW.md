# Watertown Test Iteration Workflow

For documented workflows, use the exact fixtures under `testsuite/examples/`
and the authoring/migration boundaries in [docs/README.md](../docs/README.md).
The numbered tests, not copied Markdown commands, assert documented results.

## ⛔ CRITICAL: NO "SIMPLE FIXES"

**NEVER propose or implement "the simplest fix" or "a quick patch."**

When you encounter a problem:
- ❌ DO NOT take shortcuts
- ❌ DO NOT apply band-aids
- ❌ DO NOT say "the simplest approach would be..."
- ❌ DO NOT hack around the issue
- ✅ DO understand the root cause fully
- ✅ DO design a proper solution that fits the architecture
- ✅ DO consider how similar problems should be handled consistently
- ✅ DO implement it correctly the first time

"Simple" fixes create technical debt, hide real problems, and waste everyone's time when they inevitably need to be redone properly. Take the time to do it right.

---

## The Loop

```
┌─────────────────────────────────────────────────────────────┐
│  1. RUN EXPERIMENT                                          │
│     - Pick highest priority item from backlog               │
│     - Execute in container                                  │
│     - Capture output                                        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  2. CLASSIFY RESULT                                         │
│     ✅ Success → Add to library, move to next               │
│     ❌ Failure → Categorize:                                │
│        - BUG: Code doesn't work as intended                 │
│        - DOCS: Documentation is wrong/missing               │
│        - UX: Interface is confusing but works               │
│        - DESIGN: Fundamental approach is wrong              │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  3. PRESENT TO USER                                         │
│     - What we tried                                         │
│     - What happened                                         │
│     - Classification + rationale                            │
│     - Proposed fix (if any)                                 │
│     - Decision needed (if any)                              │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  4. USER DECISION                                           │
│     - Approve fix → Implement                               │
│     - Reject/Defer → Add to backlog with notes              │
│     - Redirect → Change priority or approach                │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      ▼
┌─────────────────────────────────────────────────────────────┐
│  5. IMPLEMENT (if approved)                                 │
│     - Make the change (code/docs/interface)                 │
│     - Re-run test to verify                           │
│     - Update backlog                                        │
└─────────────────────┬───────────────────────────────────────┘
                      │
                      └──────────────► REPEAT
```

## Priority Levels

| Priority | Meaning |
|----------|---------|
| P0 | Blocking - can't proceed |
| P1 | High - common operation fails |
| P2 | Medium - works but confusing |
| P3 | Low - nice-to-have |

---

## Session Commands

| To do this | Say |
|------------|-----|
| Start session | "Let's iterate" or "Show me the backlog" |
| Approve | "Approved. Implement and continue." |
| Defer | "Defer. Move to next." |
| Discuss | "Let's discuss: [thoughts]" |
