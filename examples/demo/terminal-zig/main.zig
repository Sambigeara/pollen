// Copyright 2026 Sam Lock
// SPDX-License-Identifier: Apache-2.0

// terminal is the tail seed in the firehose chain. It hashes the input
// payload and forwards a tick to pln://service/sink, which runs on the
// operator's local node and renders a live RPS display.

const std = @import("std");
const pdk = @import("extism-pdk");

extern "extism:host/user" fn pollen_log(level: u64, msg_offset: u64) void;
extern "extism:host/user" fn pollen_request(uri_offset: u64, input_offset: u64) u64;

const log_level_info: u64 = 1;
const log_level_error: u64 = 3;

fn logStr(plugin: pdk.Plugin, level: u64, msg: []const u8) void {
    const mem = plugin.allocateBytes(msg);
    pollen_log(level, mem.offset);
}

export fn handle() i32 {
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const plugin = pdk.Plugin.init(allocator);

    const input = plugin.getInput() catch {
        plugin.output("{\"error\":\"failed to read input\"}");
        return 1;
    };

    var digest: [std.crypto.hash.sha2.Sha256.digest_length]u8 = undefined;
    std.crypto.hash.sha2.Sha256.hash(input, &digest, .{});
    const hex = std.fmt.bytesToHex(digest, .lower);

    // Empty-body POST keeps the wire payload minimal so the chain measures
    // the mesh + sink hot path rather than per-request body throughput.
    const req = "POST / HTTP/1.1\r\nHost: localhost\r\nConnection: close\r\nContent-Length: 0\r\n\r\n";

    const uri_mem = plugin.allocateBytes("pln://service/sink");
    const input_mem = plugin.allocateBytes(req);
    const out_offset = pollen_request(uri_mem.offset, input_mem.offset);

    if (out_offset == 0) {
        logStr(plugin, log_level_error, "terminal: sink request failed");
        plugin.output("{\"error\":\"sink request failed\"}");
        return 1;
    }

    const out_mem = plugin.findMemory(out_offset);
    const out_buf = out_mem.loadAlloc(allocator) catch {
        plugin.output("{\"error\":\"failed to read response\"}");
        return 1;
    };

    if (std.mem.indexOf(u8, out_buf, "200 OK") == null) {
        logStr(plugin, log_level_error, "terminal: sink returned non-200");
        plugin.output("{\"error\":\"sink returned error\"}");
        return 1;
    }

    const out_payload = std.fmt.allocPrint(
        allocator,
        "{{\"hash\":\"{s}\",\"sink\":true}}",
        .{&hex},
    ) catch {
        plugin.output("{\"error\":\"failed to build response\"}");
        return 1;
    };

    logStr(plugin, log_level_info, "terminal: forwarded to sink");
    plugin.output(out_payload);
    return 0;
}
