# pkg/workload

## Responsibilities
- Manages WASM workload lifecycle (seed/compile/invoke/unseed)
- Orchestrates artifact storage and compilation
- Provides workload execution context and result marshaling
- Implements prefix resolution for hash lookups

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `Manager` | type | Workload lifecycle manager |
| `New` | func | Constructor |
| `Status` | type | Workload status enum |
| `StatusRunning` | const | Workload running state |
| `Summary` | type | Workload snapshot (Hash, Status, CompiledAt) |
| `(*Manager).Seed` | method | Store WASM and compile; return hash |
| `(*Manager).SeedFromCAS` | method | Compile from CAS artifact |
| `(*Manager).IsRunning` | method | Check if workload compiled |
| `(*Manager).Call` | method | Invoke function on compiled workload |
| `(*Manager).Unseed` | method | Remove workload and drop module |
| `(*Manager).ResolvePrefix` | method | Resolve hash prefix; return full hash |
| `(*Manager).List` | method | Return all workloads as summaries |
| `(*Manager).Close` | method | Drop all compiled modules |
| `ErrAlreadyRunning` | var | Workload already running |
| `ErrNotRunning` | var | Workload not running |
| `ErrAmbiguousPrefix` | var | Hash prefix ambiguous |
| `ErrStore` | var | CAS storage error |
| `ErrCompile` | var | Compilation error |

## Dependencies (internal)

| Package | What crosses the boundary |
|---------|--------------------------|
| pkg/cas | `Store`, `Put`, `Get` |
| pkg/wasm | `Runtime`, `PluginConfig`, `Compile`, `Call`, `DropCompiled`, `ErrModuleMissing` |

## Consumed by
- pkg/node (uses: `Manager`, `New`, `Seed`, `SeedFromCAS`, `IsRunning`, `Call`, `Unseed`, `List`, `Close`, `Summary`, `ErrNotRunning`, `ErrCompile`, `ErrAlreadyRunning`)
- pkg/scheduler (uses: `ErrNotRunning`; via interface: `SeedFromCAS`, `Unseed`, `IsRunning`)
