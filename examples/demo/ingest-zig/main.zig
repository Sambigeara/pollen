// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// ingest is the first seed in the firehose chain. It timestamps the inbound
// payload and tail-calls terminal via pln://seed/terminal/handle, so the
// host releases this instance before routing the next hop.

const std = @import("std");
const pdk = @import("extism-pdk");

// Pollen host functions live under extism:host/user. The PDK only declares
// the extism:host/env imports it ships with, so we declare these directly.
extern "extism:host/user" fn pollen_log(level: u64, msg_offset: u64) void;

// WASI clock_time_get for timestamping. wazero exposes WASI to all guests,
// so this resolves at instantiation time even though we target freestanding.
extern "wasi_snapshot_preview1" fn clock_time_get(
    clock_id: u32,
    precision: u64,
    out_time: *u64,
) u32;

const log_level_info: u64 = 1;

fn unixSeconds() i64 {
    var ns: u64 = 0;
    // CLOCK_REALTIME = 0; precision=0 is "best effort".
    _ = clock_time_get(0, 0, &ns);
    return @intCast(ns / std.time.ns_per_s);
}

export fn handle() i32 {
    // Per-invocation arena: every allocation is freed when the call returns,
    // and nothing crosses invocations, so concurrent calls share no state.
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const plugin = pdk.Plugin.init(allocator);
    const input = plugin.getInput() catch {
        plugin.output("{\"error\":\"failed to read input\"}");
        return 1;
    };
    const payload = std.fmt.allocPrint(
        allocator,
        "{{\"ts\":{d},\"data\":\"{s}\"}}",
        .{ unixSeconds(), input },
    ) catch {
        plugin.output("{\"error\":\"failed to build payload\"}");
        return 1;
    };

    const encoder = std.base64.standard.Encoder;
    const encoded = allocator.alloc(u8, encoder.calcSize(payload.len)) catch {
        plugin.output("{\"error\":\"failed to encode payload\"}");
        return 1;
    };
    _ = encoder.encode(encoded, payload);

    const marker = std.fmt.allocPrint(
        allocator,
        "{{\"kind\":\"tail_call\",\"uri\":\"pln://seed/terminal/handle\",\"input\":\"{s}\"}}",
        .{encoded},
    ) catch {
        plugin.output("{\"error\":\"failed to build tail call marker\"}");
        return 1;
    };

    plugin.output(marker);

    const log_mem = plugin.allocateBytes("ingest: tail-calling terminal");
    pollen_log(log_level_info, log_mem.offset);
    return 0;
}
