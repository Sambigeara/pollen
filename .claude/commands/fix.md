Autonomous issue-to-PR pipeline. Read a GitHub issue, create an isolated worktree, implement the fix, self-review via defluff, fix review findings, and open a PR — all without plan approvals.

**CRITICAL: Do NOT enter plan mode. Do NOT ask for user approval at any step. This entire pipeline runs autonomously. Use sub-agents to do heavy research and planning, then work from their clean summaries — this keeps the main context window focused on execution.**

## Phase 1: Issue intake & research (sub-agent)

1. Parse `$ARGUMENTS` as a GitHub issue number or URL. Extract the issue number.
2. Fetch issue details: `gh issue view <number> --json number,title,body,labels,comments`
3. Launch a `general-purpose` sub-agent using the Task tool with the issue content and this prompt:

---

You are investigating a GitHub issue to produce an implementation plan. Here is the issue:

**Title**: <title>
**Body**: <body>
**Comments**: <comments>

Do the following:

1. Read the issue thoroughly — including all comments. The discussion often diverges from the original description, so track how scope and requirements evolve across the conversation. The latest consensus is your source of truth for the problem and acceptance criteria.
2. Explore the codebase to understand the problem area. Use Grep, Glob, and Read as needed. Be thorough — trace call chains, read tests, understand the data model.
3. Produce a concise implementation plan with:
   - **Problem**: one-sentence summary of what is broken or missing
   - **Acceptance criteria**: what "done" looks like
   - **Files to change**: list each file and what changes are needed
   - **Order of operations**: numbered steps to implement the fix
   - **Risks**: anything that could go wrong or needs extra care

Keep the plan concrete and actionable. No vague hand-waving.

---

4. Save the sub-agent's plan — this is your implementation spec for Phase 3.

## Phase 2: Worktree setup

1. Use the `EnterWorktree` tool with name `fix-issue-<number>` to get an isolated copy of the repo.
2. Confirm the working directory changed to the worktree path.

## Phase 3: Implement the fix

Do NOT enter plan mode. Work directly from the sub-agent's plan.

1. Follow the implementation plan step by step.
2. Make every change as simple as possible — only touch what's necessary.
3. Verify correctness:
   - Run `just build`
   - Run `just test`
   - If either fails, fix the failures and re-run until green.

## Phase 4: Defluff review (sub-agent)

Launch a `general-purpose` sub-agent using the Task tool with the following prompt. Include the issue number. Do NOT pass any conversation context — the sub-agent should start fresh.

---

You are reviewing a fix for GitHub issue #<number>.

First, gather context:
1. Fetch the issue: `gh issue view <number> --json number,title,body,labels,comments`
2. Read the issue thoroughly — including all comments. The discussion often diverges from the original description, so track how scope and requirements evolve across the conversation. The latest consensus is your source of truth for what "done" looks like.

Then, read the file `.claude/commands/defluff.md` and execute **only Step 1** (the staff-engineer review). Use `git diff main...HEAD` as the diff source instead of `gh pr diff`. Review the changes against both the defluff criteria AND the issue requirements — flag anything that doesn't address the issue or introduces unnecessary scope. Return the findings exactly as specified in that file's output format.

---

## Phase 5: Fix defluff findings

If the defluff sub-agent returned no findings, skip to Phase 6.

Do NOT enter plan mode. Do NOT ask for approval.

1. Take the sub-agent's findings. These are your implementation spec — the context is already clean since the sub-agent did the analysis.
2. Group findings by dependency order: correctness first, then structural, then style.
3. Implement all fixes directly, working from the findings list.
4. Re-run build and tests:
   - `just build`
   - `just test`
   - Fix any failures until green.

## Phase 6: Commit and PR

1. Invoke the `/commit` skill (read `.claude/commands/commit.md` and follow its steps) to stage and commit the changes.
2. Push the branch: `git push -u origin HEAD`
3. Create the PR:
   ```
   gh pr create --title "<concise title>" --body "$(cat <<'EOF'
   Closes #<number>

   ## Summary
   <1-3 bullet points describing the fix>

   ## Test plan
   - [ ] Tests pass (`go test ./...`)
   - [ ] Build succeeds (`just build`)
   EOF
   )"
   ```
4. Output the PR URL.
