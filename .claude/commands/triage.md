Pull open GitHub issues, prioritise them, and offer to fix the most actionable one.

**Do NOT enter plan mode until explicitly proceeding to a fix.**

## Step 1: Fetch issues (sub-agent)

Launch a `general-purpose` sub-agent using the Task tool with the following prompt. Do NOT pass conversation context.

---

Fetch and analyse all open GitHub issues for this repository.

1. Run: `gh issue list --state open --json number,title,body,labels,comments,createdAt,updatedAt --limit 50`
2. For each issue, assess:
   - **Impact**: How severe is this? (critical / high / medium / low)
   - **Effort**: How much work to fix? (trivial / small / medium / large). To judge effort, briefly explore the codebase — find the relevant files, trace the code paths, and estimate the scope of change. A one-file, few-line fix is trivial; multi-package restructuring is large.
   - **Clarity**: Are requirements clear enough to start immediately? (yes / mostly / no)
3. Rank issues by priority using this heuristic: prefer issues that are high-impact AND low-effort AND clear. A critical bug that's trivial to fix always tops the list. A vague feature request with large scope sinks to the bottom.
4. Return a structured report:

```
## Issue Triage

### Recommended (high priority, actionable now)

For each issue (top 3-5):
- **#<number>: <title>**
  - Impact: <level> — <one sentence why>
  - Effort: <level> — <one sentence why>
  - Clarity: <yes/mostly/no>
  - Summary: <2-3 sentence description of what needs to happen>

### Backlog (lower priority or needs clarification)

For each remaining issue:
- **#<number>: <title>** — Impact: <level>, Effort: <level>, Clarity: <level>. <One sentence summary.>
```

Be honest about effort estimates — don't underestimate. If an issue is vague, say so.

---

## Step 2: Present to user

Display the sub-agent's triage report to the user verbatim.

Then use `AskUserQuestion` to ask which issue they'd like to tackle. Options should be:

- The top recommended issue (e.g. "#42: Fix certificate rotation — quickest win")
- The second recommended issue (if one exists)
- The third recommended issue (if one exists)
- "Skip — just wanted the overview"

Include the issue number and a short rationale in each option label.

## Step 3: Proceed (if selected)

If the user picks an issue:

1. Invoke the `/fix` skill with the selected issue number.

If the user picks "Skip", end the conversation naturally.
