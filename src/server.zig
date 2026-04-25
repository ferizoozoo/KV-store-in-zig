// TODO: Refactor for cross-platform networking and portability
// - Add unit tests for connection handling on both OSes

const std = @import("std");
const builtin = @import("builtin");
const store = @import("store.zig");
const parse_request = @import("parse.zig").parse_request;

var shutdown_flag: ?*std.atomic.Value(bool) = null;

pub const DBServer = struct {
    port: u16,
    host: []const u8,
    address: std.net.Address,
    store: *store.KVStore,
    shutting_down: std.atomic.Value(bool),
    active_connections: std.Thread.WaitGroup,

    pub fn init(port: u16, host: []const u8) !DBServer {
        const allocator = std.heap.page_allocator;
        const address = try std.net.Address.parseIp4(host, port);
        const kvStore = try store.KVStore.new(allocator);

        std.debug.print("Initialized DBServer on {s}:{d}\n", .{ host, port });

        return DBServer{
            .port = port,
            .host = host,
            .address = address,
            .store = kvStore,
            .shutting_down = std.atomic.Value(bool).init(false),
            .active_connections = .{},
        };
    }

    pub fn start(self: *DBServer) !void {
        self.installShutdownHandler();

        var server = try self.address.listen(
            .{
                .reuse_address = true,
                .force_nonblocking = true,
            },
        );
        defer server.deinit();

        std.debug.print("Server listening on {s}:{d}\n", .{ self.host, self.port });

        while (true) {
            if (self.shutting_down.load(.acquire)) break;

            var connection = server.accept() catch |err| switch (err) {
                error.WouldBlock => {
                    std.Thread.sleep(100 * std.time.ns_per_ms);
                    continue;
                },
                error.ConnectionAborted => continue,
                else => return err,
            };

            self.active_connections.start();
            const thread = std.Thread.spawn(.{}, handle_connection, .{ self, connection }) catch |err| {
                self.active_connections.finish();
                connection.stream.close();
                std.debug.print("Failed to spawn connection handler thread: {any}\n", .{err});
                return err;
            };
            thread.detach();
        }

        std.debug.print("Shutdown requested. Waiting for active connections to finish...\n", .{});
        self.active_connections.wait();

        self.store.flush() catch |err| {
            std.debug.print("Failed to flush store during shutdown: {any}\n", .{err});
            return err;
        };

        std.debug.print("Server shutdown complete\n", .{});
    }

    fn handle_connection(self: *DBServer, connection: std.net.Server.Connection) void {
        defer self.active_connections.finish();

        var client = connection;
        defer client.stream.close();

        std.debug.print("Accepted connection from {any}\n", .{client.address});

        var buffer: [256]u8 = undefined;

        while (true) {
            const bytes_read = client.stream.read(&buffer) catch |err| {
                std.debug.print("Connection read failed: {any}\n", .{err});
                return;
            };

            if (bytes_read == 0) {
                std.debug.print("Connection closed by client\n", .{});
                return;
            }

            std.debug.print("Received {d} bytes\n", .{bytes_read});

            parse_request(self.store, buffer[0..bytes_read]) catch |err| {
                std.debug.print("Failed to parse request: {any}\n", .{err});
            };
        }
    }

    fn installShutdownHandler(self: *DBServer) void {
        shutdown_flag = &self.shutting_down;

        if (builtin.os.tag == .windows) {
            return;
        }

        const act: std.posix.Sigaction = .{
            .handler = .{ .handler = onSignal },
            .mask = std.posix.sigemptyset(),
            .flags = 0,
        };

        std.posix.sigaction(std.posix.SIG.INT, &act, null);
        std.posix.sigaction(std.posix.SIG.TERM, &act, null);
    }

    fn onSignal(_: i32) callconv(.c) void {
        if (shutdown_flag) |flag| {
            flag.store(true, .release);
        }
    }
};
