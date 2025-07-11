const std = @import("std");

pub fn build(b: *std.Build) void {
    const target = b.standardTargetOptions(.{});
    const optimize = b.standardOptimizeOption(.{});

    // Host executable
    const host_exe = b.addExecutable(.{
        .name = "vsock-proxy-host",
        .root_source_file = b.path("host.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Enclave executable
    const enclave_exe = b.addExecutable(.{
        .name = "vsock-proxy-enclave",
        .root_source_file = b.path("enclave.zig"),
        .target = target,
        .optimize = optimize,
    });

    // Install steps
    b.installArtifact(host_exe);
    b.installArtifact(enclave_exe);

    // Run commands
    const run_host = b.addRunArtifact(host_exe);
    run_host.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_host.addArgs(args);
    }

    const run_host_step = b.step("run-host", "Run the host proxy");
    run_host_step.dependOn(&run_host.step);

    const run_enclave = b.addRunArtifact(enclave_exe);
    run_enclave.step.dependOn(b.getInstallStep());
    if (b.args) |args| {
        run_enclave.addArgs(args);
    }

    const run_enclave_step = b.step("run-enclave", "Run the enclave proxy");
    run_enclave_step.dependOn(&run_enclave.step);
}
