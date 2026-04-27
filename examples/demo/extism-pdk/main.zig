const std = @import("std");
const extism = @import("ffi.zig");
const Memory = @import("Memory.zig");
pub const http = @import("http.zig");

pub const LogLevel = enum { Trace, Debug, Info, Warn, Error };

pub fn Json(comptime T: type) type {
    return struct {
        parsed: std.json.Parsed(T),
        slice: []const u8,

        pub fn value(self: @This()) T {
            return self.parsed.value;
        }

        pub fn deinit(self: @This()) void {
            self.parsed.deinit();
        }
    };
}

pub const Plugin = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) Plugin {
        return Plugin{
            .allocator = allocator,
        };
    }

    // IMPORTANT: It's the caller's responsibility to free the returned slice
    pub fn getInput(self: Plugin) ![]u8 {
        const len = extism.input_length();
        var buf = try self.allocator.alloc(u8, @intCast(len));
        errdefer self.allocator.free(buf);
        var i: usize = 0;
        while (i < len) {
            if (len - i < 8) {
                buf[i] = extism.input_load_u8(@as(u64, i));
                i += 1;
                continue;
            }

            const x = extism.input_load_u64(@as(u64, i));
            std.mem.writeInt(u64, buf[i..][0..8], x, std.builtin.Endian.little);
            i += 8;
        }
        return buf;
    }

    // IMPORTANT: It's the caller's responsibility to free the returned struct
    pub fn getJsonOpt(self: Plugin, comptime T: type, options: std.json.ParseOptions) !Json(T) {
        const bytes = try self.getInput();
        const out = try std.json.parseFromSlice(T, self.allocator, bytes, options);
        const FromJson = Json(T);
        const input = FromJson{ .parsed = out, .slice = bytes };
        return input;
    }

    pub fn getJson(self: Plugin, comptime T: type) !T {
        const bytes = try self.getInput();
        const out = try std.json.parseFromSlice(T, self.allocator, bytes, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
        return out.value;
    }

    pub fn allocate(self: Plugin, length: usize) Memory {
        _ = self; // to make the interface consistent

        const c_len = @as(u64, length);
        const offset = extism.alloc(c_len);

        return Memory.init(offset, c_len);
    }

    pub fn allocateBytes(self: Plugin, data: []const u8) Memory {
        _ = self; // to make the interface consistent
        const c_len = @as(u64, data.len);
        const offset = extism.alloc(c_len);
        const mem = Memory.init(offset, c_len);
        mem.store(data);
        return mem;
    }

    pub fn findMemory(self: Plugin, offset: u64) Memory {
        _ = self; // to make the interface consistent

        const length = extism.length(offset);
        return Memory.init(offset, length);
    }

    pub fn outputMemory(self: Plugin, mem: Memory) void {
        _ = self; // to make the interface consistent
        extism.output_set(mem.offset, mem.length);
    }

    pub fn output(self: Plugin, data: []const u8) void {
        _ = self; // to make the interface consistent
        const c_len = @as(u64, data.len);
        const offset = extism.alloc(c_len);
        const memory = Memory.init(offset, c_len);
        memory.store(data);
        extism.output_set(offset, c_len);
    }

    pub fn outputJson(self: Plugin, T: anytype, options: std.json.StringifyOptions) !void {
        const out = try std.json.stringifyAlloc(self.allocator, T, options);
        self.output(out);
    }

    pub fn setErrorMemory(self: Plugin, mem: Memory) void {
        _ = self; // to make the interface consistent
        extism.error_set(mem.offset);
    }

    pub fn setError(self: Plugin, data: []const u8) void {
        _ = self; // to make the interface consistent
        const c_len = @as(u64, data.len);
        const offset = extism.alloc(c_len);
        const memory = Memory.init(offset, c_len);
        memory.store(data);
        extism.error_set(offset);
    }

    pub fn setErrorFmt(self: Plugin, comptime fmt: []const u8, args: anytype) !void {
        const data = try std.fmt.allocPrint(self.allocator, fmt, args);
        self.setError(data);
    }

    /// IMPORTANT: it's the caller's responsibility to free the returned string
    pub fn getConfig(self: Plugin, key: []const u8) !?[]u8 {
        const key_mem = self.allocateBytes(key);
        const offset = extism.config_get(key_mem.offset);
        const c_len = extism.length(offset);
        if (offset == 0 or c_len == 0) {
            return null;
        }
        const memory = Memory.init(offset, c_len);
        defer memory.free();
        const value = try memory.loadAlloc(self.allocator);
        return value;
    }

    pub fn logMemory(self: Plugin, level: LogLevel, memory: Memory) void {
        _ = self; // to make the interface consistent

        switch (level) {
            .Trace => extism.log_trace(memory.offset),
            .Debug => extism.log_debug(memory.offset),
            .Info => extism.log_info(memory.offset),
            .Warn => extism.log_warn(memory.offset),
            .Error => extism.log_error(memory.offset),
        }
    }

    pub fn log(self: Plugin, level: LogLevel, data: []const u8) void {
        if (loggingEnabled(level)) {
            const mem = self.allocateBytes(data);
            self.logMemory(level, mem);
        }
    }

    pub fn logFmt(self: Plugin, level: LogLevel, comptime fmt: []const u8, args: anytype) !void {
        if (loggingEnabled(level)) {
            const data = try std.fmt.allocPrint(self.allocator, fmt, args);
            self.log(level, data);
        }
    }

    /// IMPORTANT: it's the caller's responsibility to free the returned string
    pub fn getVar(self: Plugin, key: []const u8) !?[]u8 {
        const key_mem = self.allocateBytes(key);
        const offset = extism.var_get(key_mem.offset);
        const c_len = extism.length(offset);
        if (offset == 0 or c_len == 0) {
            return null;
        }
        const memory = Memory.init(offset, c_len);
        defer memory.free();
        const value = try memory.loadAlloc(self.allocator);
        return value;
    }

    pub fn getVarInt(self: Plugin, comptime T: type, key: []const u8) !?T {
        const result = try self.getVar(key);

        if (result) |buf| {
            return std.mem.readPackedInt(T, buf, 0, .little);
        }

        return null;
    }

    pub fn setVar(self: Plugin, key: []const u8, value: []const u8) void {
        const key_mem = self.allocateBytes(key);
        const val_mem = self.allocateBytes(value);
        extism.var_set(key_mem.offset, val_mem.offset);
    }

    pub fn setVarInt(self: Plugin, comptime T: type, key: []const u8, value: T) !void {
        const buffer = try self.allocator.alloc(u8, @sizeOf(T));
        defer self.allocator.free(buffer);
        std.mem.writePackedInt(T, buffer, 0, value, .little);

        self.setVar(key, buffer);
    }

    pub fn removeVar(self: Plugin, key: []const u8) void {
        const mem = self.allocateBytes(key);
        extism.var_set(mem.offset, 0);
    }

    pub fn request(self: Plugin, http_request: http.HttpRequest, body: ?[]const u8) !http.HttpResponse {
        const json = try std.json.stringifyAlloc(self.allocator, http_request, .{ .emit_null_optional_fields = false });
        defer self.allocator.free(json);
        const req = self.allocateBytes(json);
        const req_body = b: {
            if (body) |bdy| {
                break :b self.allocateBytes(bdy);
            } else {
                break :b self.allocate(0);
            }
        };
        const offset = extism.http_request(req.offset, req_body.offset);
        const length = extism.length_unsafe(offset);
        const status: u16 = @intCast(extism.http_status_code());

        const headersOffset = extism.http_headers();
        const headersLength = extism.length_unsafe(headersOffset);
        const headersMem = Memory.init(headersOffset, headersLength);

        const mem = Memory.init(offset, length);
        return http.HttpResponse{
            .memory = mem,
            .status = status,
            .responseHeaders = headersMem,
        };
    }
};

fn loggingEnabled(level: LogLevel) bool {
    const currentLevelInt = extism.get_log_level();
    if (currentLevelInt == std.math.maxInt(i32)) {
        return false;
    }

    const levelInt = @intFromEnum(level);
    if (levelInt >= currentLevelInt) {
        return true;
    }

    return false;
}
