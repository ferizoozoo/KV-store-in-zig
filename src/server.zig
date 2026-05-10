const std = @import("std");
const builtin = @import("builtin");
const store = @import("store.zig");
const Logger = @import("logger.zig").Logger;
const wal = @import("wal.zig");

var shutdown_flag: ?*std.atomic.Value(bool) = null;

pub const DBServer = struct {
    port: u16,
    host: []const u8,
    address: std.net.Address,
    store: *store.KVStore,
    shutting_down: std.atomic.Value(bool),
    active_connections: std.Thread.WaitGroup,

    pub fn init(port: u16, host: []const u8, s: *store.KVStore, use_wal: bool) !*DBServer {
        const self = try s.allocator.create(DBServer);
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
            try self.store.logger.logWithType(.Info, "Replaying WAL on startup");

            s.replay_wal_entries() catch |err| {
                std.debug.print("Error replaying WAL: {any}\n", .{err});
                try self.store.logger.logWithParameters(.Error, "Failed to replay WAL on startup: {any}", .{err});
                return err;
            };

            std.debug.print("WAL replay complete. Starting server...\n", .{});
            try self.store.logger.logWithType(.Info, "WAL replay complete. Starting server...");
        }

        return self;
    }

    pub fn deinit(self: *DBServer) void {
        self.store.allocator.destroy(self);
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

        try self.store.logger.logWithParameters(.Info, "Server listening on {s}:{d}", .{ self.host, self.port });

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
                    try self.store.logger.logWithType(.Error, "Failed to accept connection");
                    continue;
                },
            };

            self.active_connections.start();
            const thread = std.Thread.spawn(.{}, handle_connection, .{ self, connection }) catch |err| {
                self.active_connections.finish();
                connection.stream.close();
                try self.store.logger.logWithParameters(.Error, "Failed to spawn connection handler thread: {any}", .{err});
                return err;
            };
            thread.detach();
        }

        try self.store.logger.logWithType(.Info, "Shutdown requested. Waiting for active connections to finish...");
        self.active_connections.wait();

        self.store.flush() catch |err| {
            try self.store.logger.logWithParameters(.Error, "Failed to flush store during shutdown: {any}", .{err});
            return err;
        };

        try self.store.logger.logWithType(.Info, "Server shutdown complete");
    }

    fn handle_connection(self: *DBServer, connection: std.net.Server.Connection) !void {
        defer self.active_connections.finish();

        var client = connection;
        defer client.stream.close();

        var read_buf: [512]u8 = undefined;
        try self.store.logger.logWithParameters(.Info, "Handling new connection from {f}", .{client.address});

        while (true) {
            const bytes_read = client.stream.read(&read_buf) catch |err| {
                try self.store.logger.logWithParameters(.Info, "Closing connection due to read error: {any}", .{err});
                return;
            };

            if (bytes_read == 0) {
                try self.store.logger.logWithType(.Info, "Connection closed by client");
                return;
            }

            try self.store.logger.logWithParameters(.Info, "Received request: {s}", .{read_buf[0..bytes_read]});

            self.store.rp.parse_request(self.store, read_buf[0..bytes_read]) catch |err| {
                try self.store.logger.logWithParameters(.Error, "Failed to parse request: {any}", .{err});
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
