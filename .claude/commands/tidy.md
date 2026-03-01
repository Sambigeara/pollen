Dispatch all 6 personas to independently tidy their owned code, then verify the combined result.

## Phase 1 — Discover personas

List all files in `.claude/personas/` to get the persona names (strip the `.md` suffix).

## Phase 2 — Parallel tidy (all personas at once)

Launch one `general-purpose` subagent per persona in parallel. Do NOT pass conversation context. Each receives:

---

**Subagent prompt template** (substitute `{PERSONA}`):

```
You are the {PERSONA} persona for the Pollen project, acting as a staff-level engineer.

Read your persona definition: .claude/personas/{PERSONA}.md

Your task: review ALL code within your ownership boundaries and tidy it up. Read every file you own thoroughly.

## What "tidy up" means

1. **Inline single-callsite functions.** If a function is only called once and doesn't justify being a separate function (no reuse, no testability benefit, no complexity isolation), inline it at the call site.

2. **Remove impossible guards.** If a nil check, error check, or conditional can never fire because of how the code is structured (e.g., a field is always set before use, a function always returns non-nil), remove the dead guard.

3. **Remove premature complexity.** If there are "configurable options" with no current need, parameters that are always passed the same value, or abstractions that serve no current purpose — remove them. Use concrete values and defaults directly.

4. **Deduplicate similar functions.** If two functions do nearly the same thing, generalize one and have the other call it, or merge them.

5. **Remove dead code.** Unused exports, unreachable branches, commented-out code, variables that are written but never read.

6. **Simplify overly verbose code.** Reduce line count without losing clarity.

## Rules

- ONLY modify files within your persona's ownership. Do NOT touch files owned by other personas.
- Do NOT break any exported API contracts that other packages depend on. Internal simplifications are fine.
- Read the code FIRST, identify issues, then make changes.
- After all changes, run `just build` and `just test` to verify nothing breaks.
- If build or tests fail, fix the issue.
- Report: (a) what you found, (b) what you changed, (c) what you left alone and why.
```

---

Collect all reports.

## Phase 3 — Integration verification

After all personas complete, run `just build` and `just test` to verify the combined changes compile and pass together. If either fails, diagnose which persona's changes conflict and fix.

## Phase 4 — Present results

Show the user:

1. `git diff --stat HEAD` — files changed and line delta
2. Per-persona summary table: persona, files touched, key changes (one line each)
3. Build/test status

Ask if the user wants to commit or make adjustments.
