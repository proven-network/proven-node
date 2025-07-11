const std = @import("std");
const linux = std.os.linux;
const IO_Uring = linux.IoUring;

// Protocol constants
const DEFAULT_VSOCK_PORT: u32 = 5000;
const BUFFER_SIZE: usize = 65536;
const AF_VSOCK = 40;

// TUN constants
const TUNSETIFF = 0x400454ca;

// io_uring configuration
const QUEUE_DEPTH: u16 = 256;

// Operation types for io_uring user data
const OpType = enum(u8) {
    read_tun = 1,
    write_vsock = 2,
    read_vsock = 3,
    write_tun = 4,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var tun_addr: []const u8 = "10.0.2.1";
    var tun_mask: u8 = 24;
    var vsock_port: u32 = DEFAULT_VSOCK_PORT;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--tun-addr") and i + 1 < args.len) {
            tun_addr = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--tun-mask") and i + 1 < args.len) {
            tun_mask = try std.fmt.parseInt(u8, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--vsock-port") and i + 1 < args.len) {
            vsock_port = try std.fmt.parseInt(u32, args[i + 1], 10);
            i += 1;
        }
    }

    std.log.info("Starting vsock proxy host listening on port {} with TUN {s}/{}", .{ vsock_port, tun_addr, tun_mask });

    // Create TUN interface
    const tun_fd = try createTunInterface(allocator, tun_addr, tun_mask);
    defer std.posix.close(tun_fd);

    // Create VSOCK listener
    const listen_fd = try createVsockListener(vsock_port);
    defer std.posix.close(listen_fd);

    // Accept connections
    while (true) {
        const vsock_fd = try std.posix.accept(listen_fd, null, null, std.posix.SOCK.CLOEXEC);

        // Handle in separate thread
        const thread = try std.Thread.spawn(.{}, handleConnection, .{
            allocator,
            tun_fd,
            vsock_fd,
        });
        thread.detach();
    }
}

fn handleConnection(
    allocator: std.mem.Allocator,
    tun_fd: std.posix.fd_t,
    vsock_fd: std.posix.socket_t,
) !void {
    defer std.posix.close(vsock_fd);

    // Set socket options for performance
    const buf_size: c_int = 1024 * 1024; // 1MB
    _ = std.posix.setsockopt(vsock_fd, std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, &std.mem.toBytes(buf_size)) catch {};
    _ = std.posix.setsockopt(vsock_fd, std.posix.SOL.SOCKET, std.posix.SO.RCVBUF, &std.mem.toBytes(buf_size)) catch {};

    try forwardPacketsIoUring(allocator, tun_fd, vsock_fd);
}

fn forwardPacketsIoUring(
    allocator: std.mem.Allocator,
    tun_fd: std.posix.fd_t,
    vsock_fd: std.posix.socket_t,
) !void {
    // Initialize io_uring
    var ring = try IO_Uring.init(QUEUE_DEPTH, 0);
    defer ring.deinit();

    // Allocate buffers
    var tun_buf = try allocator.alloc(u8, BUFFER_SIZE);
    defer allocator.free(tun_buf);

    var vsock_buf = try allocator.alloc(u8, BUFFER_SIZE);
    defer allocator.free(vsock_buf);

    // Submit initial reads
    _ = try ring.read(makeUserData(@intFromEnum(OpType.read_tun), 0), tun_fd, .{ .buffer = tun_buf }, 0);
    _ = try ring.read(makeUserData(@intFromEnum(OpType.read_vsock), 0), vsock_fd, .{ .buffer = vsock_buf }, 0);

    // Main io_uring event loop
    while (true) {
        _ = try ring.submit();

        const cqe = try ring.copy_cqe();

        const op_type = @as(OpType, @enumFromInt(@as(u8, @truncate(cqe.user_data >> 56))));

        if (cqe.res < 0) {
            std.log.err("io_uring operation {} failed: res={}", .{ op_type, cqe.res });
            continue;
        }

        switch (op_type) {
            .read_tun => {
                if (cqe.res == 0) {
                    // Submit next TUN read
                    _ = try ring.read(makeUserData(@intFromEnum(OpType.read_tun), 0), tun_fd, .{ .buffer = tun_buf }, 0);
                    continue;
                }

                const n = @as(usize, @intCast(cqe.res));

                // Forward TUN packet to VSOCK
                _ = try std.posix.write(vsock_fd, tun_buf[0..n]);

                // Submit next TUN read
                _ = try ring.read(makeUserData(@intFromEnum(OpType.read_tun), 0), tun_fd, .{ .buffer = tun_buf }, 0);
            },

            .read_vsock => {
                if (cqe.res == 0) return; // Connection closed

                const n = @as(usize, @intCast(cqe.res));

                // Forward data to TUN
                _ = try std.posix.write(tun_fd, vsock_buf[0..n]);

                // Submit next VSOCK read
                _ = try ring.read(makeUserData(@intFromEnum(OpType.read_vsock), 0), vsock_fd, .{ .buffer = vsock_buf }, 0);
            },

            .write_tun, .write_vsock => {
                // Write completed, continue
            },
        }
    }
}

fn createTunInterface(allocator: std.mem.Allocator, ip_addr: []const u8, mask: u8) !std.posix.fd_t {
    // Open TUN device
    const tun_fd = try std.posix.open("/dev/net/tun", .{ .ACCMODE = .RDWR }, 0);
    errdefer std.posix.close(tun_fd);

    // Configure interface
    var ifr: linux.ifreq = undefined;
    @memset(@as([*]u8, @ptrCast(&ifr))[0..@sizeOf(linux.ifreq)], 0);

    const name = "tun1";
    @memcpy(ifr.ifrn.name[0..name.len], name);
    // IFF_TUN = 0x0001, IFF_NO_PI = 0x1000
    const flags: u16 = 0x0001 | 0x1000;
    ifr.ifru.flags = @bitCast(flags);

    _ = std.os.linux.ioctl(tun_fd, TUNSETIFF, @intFromPtr(&ifr));

    // Bring interface up and set IP address
    try configureInterface(allocator, name, ip_addr, mask);

    return tun_fd;
}

fn configureInterface(allocator: std.mem.Allocator, name: []const u8, ip_addr: []const u8, mask: u8) !void {
    // Construct ip command
    const ip_cmd = try std.fmt.allocPrint(allocator, "ip addr add {s}/{} dev {s}", .{ ip_addr, mask, name });
    defer allocator.free(ip_cmd);

    const up_cmd = try std.fmt.allocPrint(allocator, "ip link set {s} up", .{name});
    defer allocator.free(up_cmd);

    const qdisc_cmd = try std.fmt.allocPrint(allocator, "tc qdisc replace dev {s} root fq", .{name});
    defer allocator.free(qdisc_cmd);

    // Execute commands
    var ip_proc = std.process.Child.init(&.{ "sh", "-c", ip_cmd }, allocator);
    _ = try ip_proc.spawnAndWait();

    var up_proc = std.process.Child.init(&.{ "sh", "-c", up_cmd }, allocator);
    _ = try up_proc.spawnAndWait();

    // Set up qdisc for optimal performance
    var qdisc_proc = std.process.Child.init(&.{ "sh", "-c", qdisc_cmd }, allocator);
    _ = qdisc_proc.spawnAndWait() catch |err| {
        std.log.warn("Failed to set qdisc: {}, continuing with default", .{err});
    };

    std.log.info("Configured TUN interface {s} with IP {s}/{}", .{ name, ip_addr, mask });
}

fn createVsockListener(port: u32) !std.posix.socket_t {
    const listen_fd = try std.posix.socket(AF_VSOCK, std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC, 0);
    errdefer std.posix.close(listen_fd);

    var addr = std.mem.zeroes(linux.sockaddr.vm);
    addr.family = AF_VSOCK;
    addr.port = port;
    addr.cid = linux.VMADDR_CID_HOST; // Listen on host CID

    try std.posix.bind(listen_fd, @ptrCast(&addr), @sizeOf(@TypeOf(addr)));
    try std.posix.listen(listen_fd, 128);

    return listen_fd;
}

fn makeUserData(op_type: u8, conn_idx: usize) u64 {
    return (@as(u64, op_type) << 56) | @as(u64, conn_idx);
}