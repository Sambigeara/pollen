Run the defluff review in a sub-agent, then plan fixes based on its findings.

## Step 1: Sub-agent review

Use the Task tool to launch a sub-agent (subagent_type: "general-purpose") with the following prompt. If available, pass ONLY the plan document that was used for the task. Do NOT pass any conversation context — the sub-agent should start fresh.

---

You are a staff engineer. Review the current PR (or staged diff if no PR exists) as a brutal driver of efficiency, deduplication, and safety. Your goal is to find and flag every instance of fluff, indirection, and waste.

First, determine what to review:
- Run `gh pr diff` to get the current PR diff. If that fails (no PR exists), fall back to `git diff --staged`. If that also has no output, use `git diff`.
- Use the diff to identify which files and lines changed, then read those files for full context.

### Review criteria

Apply all lessons from `.claude/CLAUDE.md` (especially the Lessons section). In addition, apply this defluff-specific check:

- **No cosmetic noise** in diffs (reformatting, reordering imports, renaming things that don't need renaming).

### Output format

For each finding:

1. **Category** (e.g. "parallel type system", "dead field", "pointless wrapper")
2. **Location** (`file:line`)
3. **Problem** (one sentence)
4. **Fix** (concrete, not vague)

Group findings by severity: correctness issues first, then structural debt, then style.

After all findings, summarize: how many switches/branches can be eliminated, net line delta, and whether the changes are wire-compatible.

### Mindset

If something feels like it's unravelling, stop and re-approach. Never run away with an implementation. Every line of code must justify its existence.

---

## Step 2: Plan fixes

Once the sub-agent returns its findings, enter plan mode. Write a plan to `tasks/todo.md` that:

1. Lists every finding from the sub-agent review
2. Groups them into logical fix batches (changes that should be made together)
3. Orders batches by dependency (correctness first, then structural, then style)
4. For each batch, describes the concrete changes to make

Present the plan for approval before making any changes.
