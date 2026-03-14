# pkg/wasm

## Responsibilities
- WASM plugin runtime backed by Extism and wazero
- Compiles and caches modules by content hash
- Manages plugin instantiation with configurable resource limits and host functions
- Probes runtime config to detect mmap(PROT_EXEC) restrictions

## Consumer API

| Export | Kind | Description |
|--------|------|-------------|
| `ErrModuleMissing` | var | Sentinel error when compiled module not cached |
| `PluginConfig` | type | Per-workload resource limits (memory, timeout) |
| `Runtime` | type | Extism-backed WASM runtime with module cache |
| `NewRuntime` | func | Create runtime with host functions and concurrency limit |
| `(*Runtime).Compile` | method | Compile and cache WASM bytes by hash |
| `(*Runtime).Call` | method | Invoke function on cached module instance |
| `(*Runtime).DropCompiled` | method | Remove cached compiled module |
| `(*Runtime).Close` | method | Close all compiled plugins |
| `InvocationRouter` | interface | Inter-workload RPC through mesh (`RouteCall` method) |
| `NewHostFunctions` | func | Create host functions (pollen_log, pollen_call) |

## Dependencies (internal)

None — leaf package.

## Consumed by
- pkg/node (uses: `Runtime`, `NewRuntime`, `NewHostFunctions`, `PluginConfig`, `InvocationRouter`)
- pkg/scheduler (uses: `PluginConfig`)
- pkg/workload (uses: `Runtime`, `PluginConfig`)
