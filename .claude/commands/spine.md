Produce a reading map for a given component, subsystem, or concept — the high-influence "spine" functions whose downstream calls reveal how the mechanism works end-to-end.

`ARGUMENTS` identifies the target: a package path (`pkg/placement`), a subsystem name (`workload orchestration`), a concept (`SLO autoscale`, `invocation path`), or a specific mechanism (`gate sizing`, `claim decisions`).

## Goal

Help the user build a mental model of the code by reading a small, curated set of functions in an order that makes sense. Optimise for **upstream, centralised functions whose call tree covers the mechanism**, not for comprehensive coverage.

## Steps

1. **Locate candidate entry points.** Search for:
   - Public package API methods (exported functions on service/manager structs)
   - Periodic loops / reconcile tick functions
   - Message-dispatch / stream handlers
   - Construction / lifecycle functions (`New*`, `Start`, `Run`)
   - Whatever the user's argument specifically names

2. **Rank by influence.** A spine function is one where:
   - Most of the mechanism's logic hangs off its call tree
   - Reading it forces you to encounter the next layer of abstraction
   - It's the natural entry point from the layer above (CLI, gRPC, timer)

   Prefer 2–4 spine functions total. More than 4 and the map stops being a map.

3. **Trace one level down per spine.** For each spine, list what it calls in order, annotated with what each call does in one clause. Use `file:line` for every reference so the user can click through.

4. **Surface supporting helpers.** A short second list of functions that concentrate important logic but aren't on the main call path — pure scoring functions, tick loops, gate/lock primitives. 3–5 max.

5. **End with a suggested tracing order.** A numbered 3–5 step walk that builds understanding incrementally — usually "make something happen → watch it propagate → observe the feedback loop". Frame each step as a concrete action the reader performs while reading.

## Output format

Follow this exact shape:

```
<N> spine functions cover most of <target>. Read them in order; each calls the next layer you need to understand.

### 1. `<func name>` — `<file>:<line>`

<One sentence on role>. <What it does per call/tick>:

- `<callee>` — `<file>:<line>`: <one clause>
- `<callee>` — `<file>:<line>`: <one clause>
- ...

<Optional: one sentence on why it's THE spine>

### 2. `<func name>` — `<file>:<line>`

<Same pattern>

### 3. ...

### Supporting functions worth reading

- **`<func name>`** — `<file>:<line>`. <One sentence.>
- ...

### Suggested tracing order

1. <Concrete action>: `<func>` → `<func>` → (<side effect>)
2. <Next action>: `<func>` → `<func>`
3. ...

<One closing sentence pointing out the interesting loop/invariant the reader should look for.>
```

## Rules

- **Code first, narrative minimal.** The output is a navigation aid, not an explanation. If a function's role is self-evident from its name, don't explain it.
- **Every function reference carries `file:line`.** Non-negotiable — the whole point is clicking through.
- **No speculation about intent.** Describe what the code does, not what the author "probably wanted".
- **Don't restate the code.** If the reader will see `gate.acquire` in the spine's body, don't re-describe gate mechanics — just name it and move on.
- **Skip functions where there's nothing to say.** A spine with only one meaningful callee isn't a spine; promote that callee directly.
- **Push back if the target is ambiguous.** If `ARGUMENTS` could mean several things ("routing" in a mesh could be tunnel routing, P2C, DHT), ask which one before producing a map.

## When not to produce a map

- The target is a single function — just read it together.
- The target is a one-file helper package — no spine, just a table of contents.
- The target spans unrelated subsystems — ask the user to narrow it.
