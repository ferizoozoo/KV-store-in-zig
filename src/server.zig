// TODO: Refactor for cross-platform networking and portability
// - Add unit tests for connection handling on both OSes

const std = @import("std");
const builtin = @import("builtin");
const store = @import("store.zig");
const Logger = @import("logger.zig").Logger;
const parse_request = @import("parse.zig").parse_request;
const wal = @import("wal.zig");

var shutdown_flag: ?*std.atomic.Value(bool) = null;

pub const DBServer = struct {
    port: u16,
    host: []const u8,
    address: std.net.Address,
    store: *store.KVStore,
    shutting_down: std.atomic.Value(bool),
    active_connections: std.Thread.WaitGroup,

    pub fn init(allocator: std.mem.Allocator, port: u16, host: []const u8, s: *store.KVStore, use_wal: bool) !*DBServer {
        const self = try allocator.create(DBServer);
        self.* = .{
            .port = port,
            .host = host,
            .address = try std.net.Address.parseIp4(host, port),
            .store = s,
            .shutting_down = std.atomic.Value(bool).init(false),
            .active_connections = .{},
        };

        if (use_wal) {
            std.debug.print("Replaying WAL on startup...\n", .{});
            var log_buffer: [256]u8 = undefined;
            const infoMessage = try std.fmt.bufPrint(&log_buffer, "Replaying WAL on startup", .{});
            try self.store.logger.logWithType(.Info, infoMessage);

            wal.replay(self.store) catch |err| {
                std.debug.print("Error replaying WAL: {any}\n", .{err});
                var buffer: [256]u8 = undefined;
                const errMessage = try std.fmt.bufPrint(&buffer, "Failed to replay WAL on startup: {any}", .{err});
                try self.store.logger.logWithType(.Error, errMessage);
                return err;
            };

            std.debug.print("WAL replay complete. Starting server...\n", .{});
            const infoMessage2 = try std.fmt.bufPrint(&log_buffer, "WAL replay complete. Starting server...", .{});
            try self.store.logger.logWithType(.Info, infoMessage2);
        }

        return self;
    }

    pub fn deinit(self: *DBServer, allocator: std.mem.Allocator) void {
        allocator.destroy(self);
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
                error.ConnectionAborted => {
                    try self.store.logger.logWithType(.Warning, "Connection aborted by client");
                    continue;
                },
                else => {
                    var buffer: [256]u8 = undefined;
                    const errMessage = try std.fmt.bufPrint(&buffer, "Failed to accept connection: {any}", .{err});
                    try self.store.logger.logWithType(.Error, errMessage);
                    continue;
                },
            };

            self.active_connections.start();
            const thread = std.Thread.spawn(.{}, handle_connection, .{ self, connection }) catch |err| {
                self.active_connections.finish();
                connection.stream.close();
                var buffer: [256]u8 = undefined;
                const errMessage = try std.fmt.bufPrint(&buffer, "Failed to spawn connection handler thread: {any}", .{err});
                try self.store.logger.logWithType(.Error, errMessage);
                return err;
            };
            thread.detach();
        }

        try self.store.logger.logWithType(.Info, "Shutdown requested. Waiting for active connections to finish...");
        self.active_connections.wait();

        self.store.flush() catch |err| {
            var buffer: [256]u8 = undefined;
            const errMessage = try std.fmt.bufPrint(&buffer, "Failed to flush store during shutdown: {any}", .{err});
            try self.store.logger.logWithType(.Error, errMessage);
            return err;
        };

        try self.store.logger.logWithType(.Info, "Server shutdown complete");
    }

    fn handle_connection(self: *DBServer, connection: std.net.Server.Connection) !void {
        defer self.active_connections.finish();

        var client = connection;
        defer client.stream.close();

        var read_buf: [512]u8 = undefined;
        var log_buf: [256]u8 = undefined;
        const infoMessage = try std.fmt.bufPrint(&log_buf, "Handling new connection from {f}", .{client.address});
        try self.store.logger.logWithType(.Info, infoMessage);

        while (true) {
            const bytes_read = client.stream.read(&read_buf) catch |err| {
                const errMessage = try std.fmt.bufPrint(&log_buf, "Connection read failed: {any}", .{err});
                try self.store.logger.logWithType(.Error, errMessage);
                return;
            };

            if (bytes_read == 0) {
                try self.store.logger.logWithType(.Info, "Connection closed by client");
                return;
            }

            const infoMessage2 = try std.fmt.bufPrint(&log_buf, "Received {d} bytes from {f}", .{ bytes_read, client.address });
            try self.store.logger.logWithType(.Info, infoMessage2);

            parse_request(self.store, read_buf[0..bytes_read]) catch |err| {
                const errMessage = try std.fmt.bufPrint(&log_buf, "Failed to parse request: {any}", .{err});
                try self.store.logger.logWithType(.Error, errMessage);
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
