Convene the persona committee to collaboratively plan and implement a task.

## Phase 1 — Requirements Intake

Parse `$ARGUMENTS` to produce a clean requirements block.

- If `$ARGUMENTS` looks like a GitHub issue number (e.g. `123`) or URL (e.g. `https://github.com/.../issues/123`), fetch it with `gh issue view <number> --json title,body,labels,comments` and extract the requirements.
- Otherwise treat `$ARGUMENTS` as a plain-text task description.

Store the result as a `requirements` string for distribution to personas. Format:

```
## Requirements
<title or one-line summary>

<full description, acceptance criteria, and any relevant context>
```

## Phase 2 — Persona Assessment (parallel)

Launch all 6 persona subagents in parallel using the Task tool (`subagent_type: "general-purpose"`). Do NOT pass conversation context. Each subagent receives the same prompt template with only its persona name substituted:

---

**Subagent prompt template** (substitute `{PERSONA}` and `{REQUIREMENTS}`):

```
You are the {PERSONA} persona for the Pollen project.

Read your persona definition: .claude/personas/{PERSONA}.md

Then evaluate the following requirements:

{REQUIREMENTS}

Return your assessment in EXACTLY this format (no other output):

RELEVANT: yes | no
REASON: <one sentence why relevant or not>

INTERNAL_PLAN:
<if relevant: numbered list of concrete implementation steps within your owned packages>
<if not relevant: "n/a">

CROSS_BOUNDARY:
<if relevant: list each dependency as "NEEDS <persona>: <what you need from them>">
<if not relevant: "none">
```

---

Collect all 6 responses. Parse each into: persona name, relevant (bool), reason, internal plan, and cross-boundary dependencies.

## Phase 3 — Cross-Persona Negotiation

Identify all cross-boundary requests from Phase 2. For each unique `NEEDS <target>: <ask>`, send the ask to the target persona for evaluation.

Launch one subagent per affected target persona (`subagent_type: "general-purpose"`, no conversation context):

---

**Negotiation subagent prompt template**:

```
You are the {TARGET_PERSONA} persona for the Pollen project.

Read your persona definition: .claude/personas/{TARGET_PERSONA}.md

Another persona has requested the following from you:

FROM: {REQUESTING_PERSONA}
REQUEST: {REQUEST_DESCRIPTION}

CONTEXT (the overall task requirements):
{REQUIREMENTS}

Evaluate whether this request is compatible with your ownership, API contracts, and guarantees.

Return your response in EXACTLY this format:

VERDICT: accept | counter | reject
REASON: <one sentence>
COUNTER_PROPOSAL: <if verdict is "counter", describe your alternative; otherwise "n/a">
```

---

If any verdict is `counter`, send the counter-proposal back to the requesting persona for another round. Maximum 3 negotiation rounds total.

If after 3 rounds any request is still `reject` or unresolved `counter`, escalate to the user via `AskUserQuestion`. Present:
- The requesting persona and what they need
- The target persona and why they reject/counter
- Ask the user to decide

## Phase 4 — Consensus Plan

Synthesize all persona plans (internal plans + negotiated cross-boundary agreements) into a single unified implementation plan.

Write the plan to `tasks/committee_todo.md` using this format:

```markdown
# Committee Plan: <task title>

## Requirements
<from Phase 1>

## Participating Personas
<list relevant personas and their roles>

## Implementation Plan
- [ ] <step 1 — owner: persona>
- [ ] <step 2 — owner: persona>
...

## Cross-Boundary Agreements
<summarize any negotiated agreements>

## Verification
- [ ] `just build` passes
- [ ] `just test` passes
- [ ] <any task-specific verification>
```

Present the plan to the user for approval. If the user requests changes, revise and re-present. Do NOT proceed until the user approves.

## Phase 5 — Implementation

Launch a single `general-purpose` subagent on the current branch (no worktree, no conversation context). Pass the approved plan as its prompt:

---

**Implementation subagent prompt**:

```
You are implementing an approved plan for the Pollen project.

## Approved Plan
{CONTENTS_OF_TASKS_TODO_MD}

Implement every step in the plan. Follow these rules:
- Work on the current branch directly
- Read existing code before modifying it
- Follow patterns established in the codebase
- Do NOT add unnecessary comments, dead code, or over-engineer
- After implementation, run `just build` and `just test`
- If build or tests fail, fix the issues and re-run until both pass
- Report what you implemented and the build/test results
```

---

Collect the implementation report.

## Phase 6 — Persona Review (parallel)

Launch ALL 6 persona subagents in parallel (`subagent_type: "general-purpose"`, no conversation context). Relevant personas do a full review; irrelevant personas do a sanity check.

---

**Review subagent prompt template**:

```
You are the {PERSONA} persona for the Pollen project.

Read your persona definition: .claude/personas/{PERSONA}.md

A task has just been implemented. Your relevance to this task: {RELEVANT_OR_NOT}.

## Original Requirements
{REQUIREMENTS}

## Implementation Summary
{IMPLEMENTATION_REPORT}

{IF_RELEVANT}
Your approved plan was:
{PERSONA_INTERNAL_PLAN}

Review the implementation against your plan. Check:
1. All your planned steps were completed correctly
2. Your API contracts and guarantees are preserved
3. No regressions in your owned packages
{END_IF}

{IF_NOT_RELEVANT}
Do a sanity check for unexpected cross-cutting impacts on your owned packages. Check:
1. No unexpected changes to files you own
2. No new dependencies on your packages that bypass your API contracts
3. No changes that violate your guarantees
{END_IF}

Run `git diff HEAD~1` or inspect changed files to see what was modified.

Return your review in EXACTLY this format:

PERSONA: {PERSONA}
STATUS: pass | findings
FINDINGS:
<numbered list of issues, or "none">
```

---

Collect all 6 reviews. If ANY persona returns `findings`:

1. Triage findings into "must-fix" (correctness, dead code, dedup) and "deferred" (nice-to-have, future work). Present both lists to the user and get approval on which to fix now.
2. Send must-fix findings to the implementation subagent (resume it or launch a new one) with instructions to address each finding.
3. **MANDATORY**: After fixes are applied, re-run the FULL persona review cycle (all 6 personas, same prompts). This is not optional — every fix round MUST be followed by a review round. Do NOT skip to the final presentation after fixes.
4. Repeat fix → review cycles until all personas return `pass` or only deferred findings remain.

Maximum 3 review-fix rounds. If findings persist after 3 rounds, present the remaining issues to the user and ask how to proceed.

## Phase 7 — Final Presentation

Present the completed implementation to the user:

1. Summary of what was implemented
2. Which personas were involved and their roles
3. Build and test results
4. Any review findings that were addressed (or escalated)
5. Files changed (use `git diff --stat`)

Ask if the user wants any final adjustments before considering the task complete.
