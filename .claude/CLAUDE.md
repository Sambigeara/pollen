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
- Write rules for yourself that prevent the same mistake
- Ruthlessly iterate on these lessons until mistake rate drops
- Review lessons at session start for relevant project

### 4. Verification Before Done
- Never mark a task complete without proving it works
- Diff behavior between main and your changes when relevant
- Ask yourself: "Would a staff engineer approve this?"
- Run tests, check logs, demonstrate correctness
- A change is only complete when `just lint` passes. Use `//nolint` on false hits (no trailing comment needed if the reason is obvious from the directive), and fix legitimate lint errors

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

### 7. Commit Message Style
- Lowercase start, imperative/descriptive verb (`simplify`, `fix`, `add`, `remove`, `harden`, `clean up`, etc.)
- Single-line title, ideally under 60 characters; add detail lines below only when context isn't clear from the title
- No trailing period, no conventional-commit prefixes, no ticket references, no Co-Authored-By trailer
- Describe what changed, not why — e.g. `simplify auth model and command surface`
- Always sign commits (`git commit -s`)

## Task Management

1. **Plan First**: Write plan to `tasks/todo.md` with checkable items
2. **Verify Plan**: Check in before starting implementation
3. **Track Progress**: Mark items complete as you go
4. **Explain Changes**: High-level summary at each step
5. **Document Results**: Add review section to `tasks/todo.md`
6. **Capture Lessons**: Add to the **Lessons** section at the bottom of this file after corrections

## Core Principles

- **ARCH_SPEC.md is gospel.** Every change must conform to the architecture spec — layer boundaries, dependency rules, interface patterns, unexported internals. Read it before proposing structural changes. Any deviation must be explicitly discussed and justified with the user before implementation. The only acceptable direction is *cleaner*: narrower interfaces, fewer cross-boundary types, less coupling. Never introduce new coupling, widen an interface, or export an internal without architectural justification. If you spot a concrete type crossing a package boundary where a narrow interface would suffice, replacing it is a priority — do it as part of the current change.
- **Simplicity First**: Make every change as simple as possible. Only touch what's necessary. Avoid introducing bugs.
- **No Laziness**: Find root causes. No temporary fixes. Senior developer standards.
- **Boyscout Rule**: Always leave the code cleaner than you found it. When defluff or review surfaces nearby debt — even if it predates the current diff — fix it, unless it opens a large can of worms.

## Lessons

### Tooling
- Use `just generate` to regenerate protobuf code, not raw `buf generate` commands.
- **After editing Go files, ALWAYS run `goimports -w <file>` to fix imports and formatting — including adding missing imports AND removing unused ones.** This is faster than manually searching for import paths and more correct than hand-editing. Never manually add or remove import lines; let `goimports` handle it.

### Config
- `config.yaml` has a header with example CLI commands (`pln serve`, `pln connect`, `pln disconnect`). When renaming or adding CLI commands that affect services/connections, update the `configHeader` constant in `pkg/config/config.go` to match.

### Code Quality
- Don't add comments that restate what the code already says. Only comment where logic isn't self-evident. This includes `nolint` directives — don't add a trailing comment that just restates the lint rule (e.g., `//nolint:forcetypeassert // always a UDPAddr`). The nolint directive is self-explanatory; only comment if the reason is genuinely non-obvious.
- Don't ship dead or unused code — no dead branches for impossible conditions, no nil/zero guards on values that provably can't be nil/zero, no struct fields only tests read, no parameters the function ignores. These guards are actively misleading: they imply the guarded state is reachable when it isn't. Signatures and types are contracts.
- Deduplicate before shipping. If two functions build the same output from the same data, one should call the other.
- Every switch on a type or enum must be exhaustive. Dead default branches that can't fire are fluff — remove them.
- No naked returns in functions with named return values unless the function is trivially short.

### Error Handling
- **Standard library only for errors.** `errors.New` for sentinels, `fmt.Errorf("%w")` for wrapping, `errors.Is`/`errors.As` for checking. No external error libraries.
- **Map errors to gRPC status codes in service handlers.** Use `status.Error(codes.X, "user-facing message")` and log the detailed error separately with `zap.Error(err)`. Don't leak internal details to callers.

### Design Patterns
- **Use typed representations over string conventions.** Don't encode structured data into string keys with prefix parsing (`"s/http"`, `"r/<pk>"`). Use typed structs with enums from the start — string conventions are fragile and create implicit coupling.
- **Unify parallel patterns immediately.** When multiple attributes need the same concept (e.g., deletion), use one consistent mechanism everywhere. After each step, ask: "have I introduced a second way of expressing the same idea?"
- **Clean package APIs.** Each package should expose a clean API. Internal struct types, lock details, and implementation choices must not leak across package boundaries.
- **Narrow interfaces at every boundary.** If a concrete type (e.g., `transport.Stream`) crosses a package boundary but the consumer only needs `io.ReadWriteCloser`, replace it with the narrow interface. This is not optional cleanup — it's a priority whenever encountered, even mid-task.
- **Noop implementations for optional features.** When a feature can be disabled (metrics, tracing), implement the same interface with no-ops rather than scattering nil checks. Initialize to no-op, wire the real implementation later via setter.

### Proto Message Design
- **Never build a shadow type system alongside a proto oneof.** If the proto already has a discriminator (oneof, enum), use it directly. Don't create a parallel Go enum that must stay in sync.
- **Put shared semantics at the shared level.** If every variant of a oneof carries the same field (e.g., `deleted`), that field belongs on the parent message, not duplicated across each variant.
- **Don't nest proto messages without a reason.** If the wrapper adds nothing beyond what the inner message has, inline the fields or use the inner message directly.

### Concurrency
- Don't add mutexes around write-once fields. If a field is set once before goroutines are spawned, the goroutine-spawn itself establishes happens-before — a mutex adds noise and implies the field is mutable when it isn't.
- Don't wrap simple field access in a getter that only adds a nil check for conditions that can't happen. If the field is guaranteed set by the time callers run, just use it directly.
- Every `for { select { ... } }` loop must have a `case <-ctx.Done()` branch. No exceptions.
- **No fire-and-forget goroutines.** Every goroutine must be tracked by a `WaitGroup` (or equivalent) and use a cancellable context. Bare `go func()` with `context.Background()` creates goroutines that outlive shutdown and panic on closed channels. Use `wg.Go()` with the service's context.
- Use `sync.Once` to guard channel closes when multiple code paths could trigger shutdown. Double-close panics.
- Shutdown ordering matters: stop accepting → drain active work → flush observability → close stores. Each layer waits for the previous.

### Performance
- Don't hand-calculate serialization sizes. Use the serialization library's own `Size()` methods — hand-counted varint bytes silently break when field numbers change.
- Batch lock acquisition on the receive side too. If you batch on send, the receive path should also take the lock once for the batch, not N times for N events.

### Workflow
- When `CLAUDE_CODE_EXPERIMENTAL_AGENT_TEAMS` is enabled, use agent teams (TeamCreate) for multi-package coordination work, not plain subagents. The user expects agent teams when they've enabled the feature. Subagents are fine for research/analysis but not for cross-package execution.

### Testing
- Use `require.Equal`, `require.Len`, `require.NoError`, etc. from `github.com/stretchr/testify/require` instead of manual `t.Fatalf` with format strings. Testify assertions are more readable and give better failure output.
- Test helpers must match production constructors. If a test helper builds a struct that a production constructor also builds, they must produce equivalent state. Divergence means tests exercise impossible configurations.
- Prefer `t.Cleanup()` over `defer` for test resource teardown — it works correctly with subtests and `t.Fatal`.
- Use `require.Eventually` for async assertions (peer connections, state convergence). Don't `time.Sleep` then assert.
- Use test harness structs with factory methods (like `meshHarness`, `clusterAuth`) to keep test setup readable and reusable across subtests.
- **Test harnesses must exercise the production code path.** If a harness method (e.g. `supervisor.SeedWorkload`) adds logic beyond what the production entry point does (e.g. `control.SeedWorkload → placement.Seed`), the production path is probably missing that logic. Harness shortcuts that bypass gRPC/API layers mask integration bugs.
