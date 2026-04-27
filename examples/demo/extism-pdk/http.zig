const std = @import("std");
const Memory = @import("Memory.zig");
const extism = @import("ffi.zig");

pub const Headers = struct {
    allocator: std.mem.Allocator,
    raw: []const u8,
    internal: std.json.ArrayHashMap([]const u8),

    /// Get a value (if it exists) from the Headers map at the provided name.
    /// NOTE: this may be a multi-value header, and will be a comma-separated list.
    pub fn get(self: Headers, name: []const u8) ?std.http.Header {
        const val = self.internal.map.get(name);
        if (val) |v| {
            return std.http.Header{
                .name = name,
                .value = v,
            };
        } else {
            return null;
        }
    }

    /// Access the internal data to iterate over or mutate as needed.
    pub fn getInternal(self: Headers) std.json.ArrayHashMap([]const u8) {
        return self.internal;
    }

    /// Check if the Headers is empty.
    pub fn isEmpty(self: Headers) bool {
        return self.internal.map.entries.len == 0;
    }

    /// Check if a header exists in the Headers.
    pub fn contains(self: Headers, key: []const u8) bool {
        return self.internal.map.contains(key);
    }

    pub fn deinit(self: *Headers) void {
        self.allocator.free(self.raw);
        self.internal.deinit(self.allocator);
    }
};

pub const HttpResponse = struct {
    memory: Memory,
    status: u16,
    responseHeaders: Memory,

    /// IMPORTANT: it's the caller's responsibility to free the returned string
    pub fn body(self: HttpResponse, allocator: std.mem.Allocator) ![]u8 {
        const buf = try allocator.alloc(u8, @intCast(self.memory.length));
        errdefer allocator.free(buf);
        self.memory.load(buf);
        return buf;
    }

    pub fn deinit(self: HttpResponse) void {
        self.memory.free();
        self.responseHeaders.free();
    }

    pub fn statusCode(self: HttpResponse) u16 {
        return self.status;
    }

    /// IMPORTANT: it's the caller's responsibility to `deinit` the Headers if returned.
    pub fn headers(self: HttpResponse, allocator: std.mem.Allocator) !Headers {
        const data = try self.responseHeaders.loadAlloc(allocator);
        errdefer allocator.free(data);

        const j = try std.json.parseFromSlice(std.json.ArrayHashMap([]const u8), allocator, data, .{ .allocate = .alloc_always, .ignore_unknown_fields = true });
        defer j.deinit();

        return Headers{
            .allocator = allocator,
            .raw = data,
            .internal = j.value,
        };
    }
};

pub const HttpRequest = struct {
    url: []const u8,
    method: []const u8,
    headers: std.json.ArrayHashMap([]const u8),

    pub fn init(method: []const u8, url: []const u8) HttpRequest {
        return HttpRequest{
            .method = method,
            .url = url,
            .headers = std.json.ArrayHashMap([]const u8){},
        };
    }

    pub fn deinit(self: *HttpRequest, allocator: std.mem.Allocator) void {
        self.headers.deinit(allocator);
    }

    pub fn setHeader(self: *HttpRequest, allocator: std.mem.Allocator, key: []const u8, value: []const u8) !void {
        try self.headers.map.put(allocator, key, value);
    }
};
