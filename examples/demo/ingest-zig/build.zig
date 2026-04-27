const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.resolveTargetQuery(.{
        .cpu_arch = .wasm32,
        .os_tag = .freestanding,
    });
    const optimize: std.builtin.OptimizeMode = .ReleaseSmall;

    const pdk = b.createModule(.{
        .root_source_file = b.path("../extism-pdk/main.zig"),
    });

    const exe = b.addExecutable(.{
        .name = "ingest",
        .root_module = b.createModule(.{
            .root_source_file = b.path("main.zig"),
            .target = target,
            .optimize = optimize,
            .imports = &.{.{ .name = "extism-pdk", .module = pdk }},
        }),
    });
    exe.entry = .disabled;
    exe.rdynamic = true;
    // Default shadow stack is 1 MiB and inflates the initial memory section.
    // 64 KiB is plenty for our small synchronous JSON munging.
    exe.stack_size = 64 * 1024;

    b.installArtifact(exe);
}
