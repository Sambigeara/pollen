## Workflow Orchestration

### 1. Plan Node Default
- Enter plan mode for ANY non-trivial task (3+ steps or architectural decisions)
- If something goes sideways, STOP and re-plan immediately - don't keep pushing
- Use plan mode for verification steps, not just building
- Write detailed specs upfront to reduce ambiguity

### 2. Subagent Strategy
- Use subagents liberally to keep main context window clean
- Offload research, exploration, and parallel analysis to subagents
- For complex problems, throw more compute at it via subagents
- One tack per subagent for focused execution

### 3. Self-Improvement Loop
- After ANY correction from the user: add the lesson to the **Lessons** section at the bottom of this file.
- If any of these lessons are useful for future review, please update the `/defluff` command also so that becomes more effective.
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review lessons at session start for relevant project

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness

### 5. Demand Elegance (Balanced)
- For non-trivial changes: pause and ask "is there a more elegant way?"
- If a fix feels hacky: "Knowing everything I know now, implement the elegant solution"
- Skip this for simple, obvious fixes - don't over-engineer
- Before presenting: audit the entire change as a whole for duplicated ideas, inconsistent abstractions, or unnecessary indirection

### 6. Autonomous Bug Fixing
- When given a bug report: just fix it. Don't ask for hand-holding
- Point at logs, errors, failing tests - then resolve them
- Zero context switching required from the user
- Go fix failing CI tests without being told how

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to `tasks/todo.md`
6. **Capture Lessons**: Add to the **Lessons** section at the bottom of this file after corrections

## Core Principles

- **Simplicity First**: Make every change as simple as possible. Only touch what's necessary. Avoid introducing bugs.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.

## Lessons

### Tooling
- Use `just generate` to regenerate protobuf code, not raw `buf generate` commands.

### Code Quality
- Don't add comments that restate what the code already says. Only comment where logic isn't self-evident.
- Don't ship dead or unused code — no dead branches for impossible conditions, no struct fields only tests read, no parameters the function ignores. Signatures and types are contracts.
- Deduplicate before shipping. If two functions build the same output from the same data, one should call the other.

### Design Patterns
- **Use typed representations over string conventions.** Don't encode structured data into string keys with prefix parsing (`"s/http"`, `"r/<pk>"`). Use typed structs with enums from the start — string conventions are fragile and create implicit coupling.
- **Unify parallel patterns immediately.** When multiple attributes need the same concept (e.g., deletion), use one consistent mechanism everywhere. After each step, ask: "have I introduced a second way of expressing the same idea?"

### Proto Message Design
- **Never build a shadow type system alongside a proto oneof.** If the proto already has a discriminator (oneof, enum), use it directly. Don't create a parallel Go enum that must stay in sync.
- **Put shared semantics at the shared level.** If every variant of a oneof carries the same field (e.g., `deleted`), that field belongs on the parent message, not duplicated across each variant.
- **Don't nest proto messages without a reason.** If the wrapper adds nothing beyond what the inner message has, inline the fields or use the inner message directly.

### Concurrency
- Don't add mutexes around write-once fields. If a field is set once before goroutines are spawned, the goroutine-spawn itself establishes happens-before — a mutex adds noise and implies the field is mutable when it isn't.
- Don't wrap simple field access in a getter that only adds a nil check for conditions that can't happen. If the field is guaranteed set by the time callers run, just use it directly.

### Performance
- Don't hand-calculate serialization sizes. Use the serialization library's own `Size()` methods — hand-counted varint bytes silently break when field numbers change.
- Batch lock acquisition on the receive side too. If you batch on send, the receive path should also take the lock once for the batch, not N times for N events.
