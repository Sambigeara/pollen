Review the current PR (or staged diff if no PR exists) as a brutal driver of efficiency, deduplication, and safety. Your goal is to find and flag every instance of fluff, indirection, and waste.

## Review criteria

Apply these checks ruthlessly. Every violation gets called out with file, line, and a concrete fix.

### Type systems
- **No parallel type systems.** If a proto already defines a oneof or enum, code must switch on the proto type directly. Handwritten type-tag enums that shadow proto types are instant flags.
- **Finite/strict switches only.** Every switch on a type or enum must be exhaustive. Dead default branches are fluff.

### Deduplication
- **No duplicated fields.** If the same semantic (e.g. `deleted`) appears in N messages when it belongs on a wrapper, flag it.
- **No duplicated logic.** If two functions build the same data structure with different ceremony, one must delegate to the other.

### Indirection and nesting
- **No pointless wrappers.** `A { B { fields } }` when `A { fields }` suffices is instant debt.
- **No unnecessary guard clauses.** If a function is only called in a context where a condition is already true, don't re-check it.

### Dead code
- **No dead fields.** Struct fields that no production code reads must be removed.
- **No dead parameters.** Function parameters that are never used must be removed. Signatures are contracts; they must not lie.
- **No dead branches.** Code paths that can never execute (e.g. an else branch in a function only called when `!deleted`) must be removed.

### Component boundaries
- **No leaking internals.** Each package should expose a clean API. Internal struct types, lock details, and implementation choices must not leak.
- **Batch where possible.** If callers always loop over a method that acquires a lock, provide a batch variant that acquires once.

### Constants and magic numbers
- **No magic constants.** Hardcoded sizes, overheads, and offsets must be computed or derived from the source of truth (e.g. proto serialization).

### Style
- **No naked returns** in functions with named return values unless the function is trivially short.
- **No cosmetic noise** in diffs (reformatting, reordering imports, renaming things that don't need renaming).

## Output format

For each finding:

1. **Category** (e.g. "parallel type system", "dead field", "pointless wrapper")
2. **Location** (`file:line`)
3. **Problem** (one sentence)
4. **Fix** (concrete, not vague)

Group findings by severity: correctness issues first, then structural debt, then style.

After all findings, summarize: how many switches/branches can be eliminated, net line delta, and whether the changes are wire-compatible.

## Mindset

If something feels like it's unravelling, stop and re-approach. Never run away with an implementation. Every line of code must justify its existence.
