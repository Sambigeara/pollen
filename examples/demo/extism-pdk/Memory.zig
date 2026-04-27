const std = @import("std");
const extism = @import("ffi.zig");

const Self = @This();

offset: u64,
length: u64,

pub fn init(offset: u64, length: u64) Self {
    return .{
        .offset = offset,
        .length = length,
    };
}

pub fn load(self: Self, buf: []u8) void {
    const len = buf.len;

    var i: usize = 0;
    while (i < len) {
        if (len - i < 8) {
            buf[i] = extism.load_u8(self.offset + @as(u64, i));
            i += 1;
            continue;
        }

        const x = extism.load_u64(self.offset + @as(u64, i));
        std.mem.writeInt(u64, buf[i..][0..8], x, std.builtin.Endian.little);
        i += 8;
    }
}

pub fn loadAlloc(self: Self, allocator: std.mem.Allocator) ![]u8 {
    const out = try allocator.alloc(u8, @intCast(self.length));
    self.load(out);
    return out;
}

pub fn store(self: Self, buf: []const u8) void {
    const len = buf.len;
    var i: usize = 0;
    while (i < len) {
        if (len - i < 8) {
            extism.store_u8(self.offset + @as(u64, i), buf[i]);
            i += 1;
            continue;
        }
        const data = std.mem.readInt(u64, buf[i..][0..8], std.builtin.Endian.little);
        extism.store_u64(self.offset + @as(u64, i), data);
        i += 8;
    }
}

pub fn free(self: Self) void {
    extism.free(self.offset);
}
