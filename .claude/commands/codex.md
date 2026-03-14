Iterative review loop with Codex MCP. Submits the current changeset for review, addresses findings, and repeats until convergence.

`$ARGUMENTS` is the review prompt — any arbitrary task description passed to Codex (e.g., "review against the Layer 4 plan in tasks/wasm-runtime-plan.md"). If empty, default to a general code review of the staged/unstaged changes using `.claude/CLAUDE.md` as the style guide.

## Step 0: Preflight — verify Codex MCP is reachable

Call the `mcp__codex__codex` tool with a trivial probe:

```
prompt: "Reply with exactly: OK"
sandbox: "read-only"
```

If the tool call fails (connection refused, tool not found, timeout, or any error), stop immediately and tell the user:

> Codex MCP is not reachable. Start it with: `codex --model gpt-5.4 mcp-server`

Do not proceed to Step 1.

## Step 1: Stage all changes (cycle 1 only)

For the **first** Codex submission, stage all current changes so Codex can see them via `git diff --cached`:

```bash
git add -A
```

This establishes the baseline — Codex scopes its review to the staged diff.

**For cycle 2+**:
*IMPORTANT: Do NOT stage before submitting. Do NOT run `git add -A`.*. Fixes from the previous cycle must remain **unstaged** so Codex sees only the new minimal diff (via `git diff`) against its prior feedback. Codex can see both staged (`git diff --cached`, already reviewed) and unstaged (`git diff`, new fixes) changes.

## Step 2: Build the review prompt

Construct the Codex prompt from these parts:

1. **Cycle header**: `"This is review cycle N."` (start at 1, increment each round).

2. **User task**:
   - **Cycle 1**: Use the contents of `$ARGUMENTS`. If `$ARGUMENTS` is empty, use:
     > "Review the staged git changeset for correctness, style, and adherence to `.claude/CLAUDE.md`. For all points that you deem necessary to address, suggest fixes."
   - **Cycle 2+**: Shift focus to the fixes. Use:
     > "Review the unstaged changes, which are fixes addressing your previous review findings. Verify the fixes are correct and check for any new issues they introduce. Use `.claude/CLAUDE.md` for stylistic guidelines."

3. **Prior-cycle context** (cycle 2+): A concise summary of:
   - Which prior findings were **addressed** (and how).
   - Which findings were **disagreed with** (and why — include the reasoning so Codex can re-evaluate or accept).
   - Any standing disagreements carried forward from earlier cycles.

4. **Closing instruction**: Always end with:
   > "For all points that you deem necessary to address, suggest fixes that I can then pass back to Claude to address."

## Step 3: Submit to Codex

Call the `mcp__codex__codex` tool:

```
prompt: <constructed prompt from Step 2>
sandbox: "read-only"
```

## Step 4: Triage findings and fix

Now we have a response from Codex, stage the partial unstaged diff: `git add -A`, so we have a fresh unstaged set specific to the review we just received.

For each finding Codex returns:

- **Agree**: implement the fix. Run `goimports -w` on any modified Go files. If the fix is non-trivial, briefly explain what you changed and why.
- **Disagree**: record your reasoning. You must have a concrete technical argument — "I don't think so" is not sufficient. Disagreements are carried forward to the next cycle so Codex can re-evaluate.

After addressing all findings:

1. Run `just lint` — fix any failures.
2. Run `just test` — fix any failures.
3. **Do NOT stage the fixes.** Keep them unstaged so the next Codex cycle sees only the new diff.

## Step 5: Stage and loop

1. **When Codex's review comes back** (i.e., at the start of processing a new cycle's results), stage everything first: `git add -A`. This rolls the previous cycle's fixes into the staged baseline.
2. If there were findings in this cycle (whether agreed or disagreed): fix them (keeping fixes **unstaged**), verify with lint/test, then go back to **Step 2** with cycle N+1.
3. If Codex returned **zero findings**, convergence is reached. Stage everything (`git add -A`) and report:
   > "Codex review converged after N cycles. All findings addressed. `just lint` and `just test` pass."

## Guidelines

- **Maximum 8 cycles**. If you hit 8 without convergence, stop and present the remaining disagreements to the user for a decision.
- **Don't over-correct**. If a Codex suggestion would introduce unnecessary complexity or contradicts `.claude/CLAUDE.md` lessons, push back with reasoning. The goal is correctness and simplicity, not compliance for its own sake.
- **Keep context lean**. The prior-cycle summary should be 2-5 bullet points, not a full transcript. Codex doesn't need the history of every change — just what was done and what's still contested.
- **No plan mode**. This skill runs autonomously. If a finding requires significant refactoring, implement it directly and verify with lint/test.
