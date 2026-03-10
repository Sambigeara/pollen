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

## Phase 2 — Persona Triage + Assessment

Read all 6 persona files directly:

- `.claude/personas/cli.md`
- `.claude/personas/orchestrator.md`
- `.claude/personas/state.md`
- `.claude/personas/transport.md`
- `.claude/personas/trust.md`
- `.claude/personas/ergonomics.md`

Evaluate the requirements against each persona's **Owns**, **Responsibilities**, and **Needs** sections. A persona is relevant if:

1. The task directly touches packages it owns, OR
2. The task requires capabilities it exposes (from its API contract), OR
3. The task impacts guarantees it defends

For the **ergonomics** persona specifically: only mark relevant if the task affects user-facing surfaces (CLI commands, error messages, help text, install scripts, status output).

Record the relevance decision and a one-sentence reason for each persona.

Then launch assessment subagents **only for relevant personas** (typically 2-3) in parallel using the Agent tool (`subagent_type: "general-purpose"`). Do NOT pass conversation context. Each subagent receives:

---

**Assessment subagent prompt template** (substitute `{PERSONA}` and `{REQUIREMENTS}`):

```
You are the {PERSONA} persona for the Pollen project.

Read your persona definition: .claude/personas/{PERSONA}.md

Then evaluate the following requirements:

{REQUIREMENTS}

Return your assessment in EXACTLY this format (no other output):

RELEVANT: yes
REASON: <one sentence why relevant>

INTERNAL_PLAN:
<numbered list of concrete implementation steps within your owned packages>

CROSS_BOUNDARY:
<list each dependency as "NEEDS <persona>: <what you need from them>">
```

---

Collect responses and parse each into: persona name, internal plan, and cross-boundary dependencies.

## Phase 3 — Cross-Persona Negotiation

Collect all cross-boundary requests from Phase 2. The orchestrator already knows each persona's API contract and guarantees from reading the persona files in Phase 2. Use this knowledge to resolve requests efficiently:

**Orchestrator-mediated resolution** (no subagent needed): If a request maps directly to a documented API in the target persona's contract (e.g., "NEEDS state: `store.Store.UpsertLocalService`"), accept it immediately — the persona files document exactly what each persona exposes and guarantees.

**Subagent-required resolution**: Launch a negotiation subagent only when:

1. The request asks the target persona to **change or extend** its API contract
2. The request could **conflict** with the target persona's documented guarantees
3. There's genuine **ambiguity** about whether the request is compatible

When multiple requesting personas need something from the **same target persona**, batch all requests into a single subagent call:

---

**Negotiation subagent prompt template** (batched):

```
You are the {TARGET_PERSONA} persona for the Pollen project.

Read your persona definition: .claude/personas/{TARGET_PERSONA}.md

The following personas have requests for you:

{FOR_EACH_REQUEST}
FROM: {REQUESTING_PERSONA}
REQUEST: {REQUEST_DESCRIPTION}
{END_FOR_EACH}

CONTEXT (the overall task requirements):
{REQUIREMENTS}

Evaluate whether each request is compatible with your ownership, API contracts, and guarantees.

Return your response in EXACTLY this format (one block per request):

REQUEST_FROM: {REQUESTING_PERSONA}
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

## Phase 6 — Persona Review (diff-gated)

Run `git diff --stat HEAD~1` and map changed files to persona ownership using these boundaries:

| Persona | Owned paths |
|---|---|
| cli | `cmd/pln/`, `pkg/workspace/` |
| orchestrator | `pkg/node/`, `pkg/tunnel/`, `pkg/server/`, `pkg/observability/` |
| state | `pkg/store/`, `pkg/config/` |
| transport | `pkg/mesh/`, `pkg/sock/`, `pkg/peer/`, `pkg/topology/`, `pkg/nat/` |
| trust | `pkg/auth/`, `pkg/perm/` |
| ergonomics | `cmd/pln/`, `scripts/`, `packaging/` |

Also map proto files: `api/public/pollen/control/` → orchestrator, `api/public/pollen/state/` → state, `api/public/pollen/mesh/` → transport, `api/public/pollen/admission/` → trust.

Launch review subagents **only for personas whose owned files were touched** in parallel (`subagent_type: "general-purpose"`, no conversation context). The **ergonomics** persona is only included if user-facing files changed (`cmd/pollen/`, `scripts/`, `packaging/`, or files containing error messages).

For personas that were relevant in Phase 2 but whose files were NOT touched by the implementation: skip review — their requirements were satisfied through cross-boundary APIs and no code in their domain changed.

---

**Review subagent prompt template**:

```
You are the {PERSONA} persona for the Pollen project.

Read your persona definition: .claude/personas/{PERSONA}.md

A task has just been implemented. Files changed in your domain:
{CHANGED_FILES_FOR_THIS_PERSONA}

## Original Requirements
{REQUIREMENTS}

## Implementation Summary
{IMPLEMENTATION_REPORT}

{IF_WAS_RELEVANT_IN_PHASE_2}
Your approved plan was:
{PERSONA_INTERNAL_PLAN}

Review the implementation against your plan. Check:
1. All your planned steps were completed correctly
2. Your API contracts and guarantees are preserved
3. No regressions in your owned packages
{END_IF}

{IF_NOT_RELEVANT_IN_PHASE_2}
Check for unexpected cross-cutting impacts on your owned packages:
1. No unexpected changes to files you own
2. No new dependencies on your packages that bypass your API contracts
3. No changes that violate your guarantees
{END_IF}

Run `git diff HEAD~1 -- {OWNED_PATHS}` to inspect changes to your files.

Return your review in EXACTLY this format:

PERSONA: {PERSONA}
STATUS: pass | findings
FINDINGS:
<numbered list of issues, or "none">
```

---

Collect reviews. If ANY persona returns `findings`:

1. Triage findings into "must-fix" (correctness, dead code, dedup) and "deferred" (nice-to-have, future work). Present both lists to the user and get approval on which to fix now.
2. Send must-fix findings to the implementation subagent (resume it or launch a new one) with instructions to address each finding.
3. **MANDATORY**: After fixes are applied, re-run review **only with personas that reported findings** (not all personas). This is targeted re-review — if a persona passed, it doesn't need to review again unless fixes touched its files.
4. If fixes touched files owned by a persona that previously passed, add that persona to the re-review round.
5. Repeat fix → targeted review cycles until all reviewing personas return `pass` or only deferred findings remain.

Maximum 3 review-fix rounds. If findings persist after 3 rounds, present the remaining issues to the user and ask how to proceed.

## Phase 7 — Final Presentation

Present the completed implementation to the user:

1. Summary of what was implemented
2. Which personas were involved and their roles
3. Build and test results
4. Any review findings that were addressed (or escalated)
5. Files changed (use `git diff --stat`)

Ask if the user wants any final adjustments before considering the task complete.
