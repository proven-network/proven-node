const std = @import("std");
const linux = std.os.linux;
const IO_Uring = linux.IoUring;

// Protocol constants
const DEFAULT_VSOCK_PORT: u32 = 5000;
const BUFFER_SIZE: usize = 65536;
const AF_VSOCK = 40;
const VMADDR_CID_HOST = 3; // AWS Nitro Enclaves use CID 3 for host

// TUN interface constants
const TUNSETIFF = 0x400454ca;

// io_uring configuration
const QUEUE_DEPTH: u16 = 256;

// Operation types for io_uring user data
const OpType = enum(u8) {
    read_vsock = 1,
    write_tun = 2,
    read_tun = 3,
    write_vsock = 4,
};

pub fn main() !void {
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    // Parse arguments
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var vsock_port: u32 = DEFAULT_VSOCK_PORT;
    var tun_addr: []const u8 = "10.0.1.1";
    var tun_mask: u8 = 24;

    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--vsock-port") and i + 1 < args.len) {
            vsock_port = try std.fmt.parseInt(u32, args[i + 1], 10);
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--tun-addr") and i + 1 < args.len) {
            tun_addr = args[i + 1];
            i += 1;
        } else if (std.mem.eql(u8, args[i], "--tun-mask") and i + 1 < args.len) {
            tun_mask = try std.fmt.parseInt(u8, args[i + 1], 10);
            i += 1;
        }
    }

    std.log.info("Starting vsock proxy enclave connecting to host port {} with TUN {s}/{}", .{ vsock_port, tun_addr, tun_mask });

    // Create TUN interface
    const tun_fd = try createTunInterface(allocator, tun_addr, tun_mask);
    defer std.posix.close(tun_fd);

    // Connect to VSOCK on host
    const vsock_fd = try std.posix.socket(AF_VSOCK, std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC, 0);
    defer std.posix.close(vsock_fd);

    var vsock_addr = std.mem.zeroes(linux.sockaddr.vm);
    vsock_addr.family = AF_VSOCK;
    vsock_addr.cid = VMADDR_CID_HOST;
    vsock_addr.port = vsock_port;

    try std.posix.connect(vsock_fd, @ptrCast(&vsock_addr), @sizeOf(@TypeOf(vsock_addr)));
    
    std.log.info("Connected to host on vsock port {}", .{vsock_port});

    // Set socket options for performance
    const buf_size: c_int = 1024 * 1024; // 1MB
    _ = std.posix.setsockopt(vsock_fd, std.posix.SOL.SOCKET, std.posix.SO.SNDBUF, &std.mem.toBytes(buf_size)) catch {};
    _ = std.posix.setsockopt(vsock_fd, std.posix.SOL.SOCKET, std.posix.SO.RCVBUF, &std.mem.toBytes(buf_size)) catch {};

    // Forward packets
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
    var vsock_read_buf = try allocator.alloc(u8, BUFFER_SIZE);
    defer allocator.free(vsock_read_buf);

    var tun_read_buf = try allocator.alloc(u8, BUFFER_SIZE);
    defer allocator.free(tun_read_buf);

    // Submit initial read operations
    _ = try ring.read(makeUserData(@intFromEnum(OpType.read_vsock), 0), vsock_fd, .{ .buffer = vsock_read_buf }, 0);
    _ = try ring.read(makeUserData(@intFromEnum(OpType.read_tun), 0), tun_fd, .{ .buffer = tun_read_buf }, 0);

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
            .read_vsock => {
                if (cqe.res == 0) return; // Connection closed

                const n = @as(usize, @intCast(cqe.res));

                // Forward data to TUN
                _ = try std.posix.write(tun_fd, vsock_read_buf[0..n]);

                // Submit next VSOCK read
                _ = try ring.read(makeUserData(@intFromEnum(OpType.read_vsock), 0), vsock_fd, .{ .buffer = vsock_read_buf }, 0);
            },

            .read_tun => {
                if (cqe.res == 0) {
                    // Submit next TUN read
                    _ = try ring.read(makeUserData(@intFromEnum(OpType.read_tun), 0), tun_fd, .{ .buffer = tun_read_buf }, 0);
                    continue;
                }

                const n = @as(usize, @intCast(cqe.res));

                // Forward TUN packet to VSOCK
                _ = try std.posix.write(vsock_fd, tun_read_buf[0..n]);

                // Submit next TUN read
                _ = try ring.read(makeUserData(@intFromEnum(OpType.read_tun), 0), tun_fd, .{ .buffer = tun_read_buf }, 0);
            },

            .write_vsock, .write_tun => {
                // Write completed, continue
            },
        }
    }
}

fn makeUserData(op_type: u8, conn_idx: usize) u64 {
    return (@as(u64, op_type) << 56) | @as(u64, conn_idx);
}

fn createTunInterface(allocator: std.mem.Allocator, ip_addr: []const u8, mask: u8) !std.posix.fd_t {
    // Open TUN device
    const tun_fd = try std.posix.open("/dev/net/tun", .{ .ACCMODE = .RDWR }, 0);
    errdefer std.posix.close(tun_fd);

    // Configure interface
    var ifr: linux.ifreq = undefined;
    @memset(@as([*]u8, @ptrCast(&ifr))[0..@sizeOf(linux.ifreq)], 0);

    const name = "tun0";
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
    // Add IP address
    const ip_cmd = try std.fmt.allocPrint(allocator, "ip addr add {s}/{} dev {s}", .{ ip_addr, mask, name });
    defer allocator.free(ip_cmd);

    var ip_proc = std.process.Child.init(&.{ "sh", "-c", ip_cmd }, allocator);
    _ = try ip_proc.spawnAndWait();

    // Bring interface up
    const up_cmd = try std.fmt.allocPrint(allocator, "ip link set {s} up", .{name});
    defer allocator.free(up_cmd);

    var up_proc = std.process.Child.init(&.{ "sh", "-c", up_cmd }, allocator);
    _ = try up_proc.spawnAndWait();

    // Set up qdisc for optimal performance
    const qdisc_cmd = try std.fmt.allocPrint(allocator, "tc qdisc replace dev {s} root fq", .{name});
    defer allocator.free(qdisc_cmd);

    var qdisc_proc = std.process.Child.init(&.{ "sh", "-c", qdisc_cmd }, allocator);
    _ = try qdisc_proc.spawnAndWait();

    std.log.info("Configured TUN interface {s} with IP {s}/{}", .{ name, ip_addr, mask });
}
