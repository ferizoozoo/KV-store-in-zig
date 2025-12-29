// TODO: this needs definitely a refactor

const std = @import("std");
const store = @import("store.zig");

const requestType = enum {
    GET,
    SET,
};

pub const DBServer = struct {
    port: u16,
    host: []const u8,
    address: std.net.Address,
    store: store.KVStore,

    pub fn init(port: u16, host: []const u8) !DBServer {
        const address = try std.net.Address.parseIp4(host, port);
        const kvStore = store.KVStore.new(std.heap.page_allocator);

        return DBServer{
            .port = port,
            .host = host,
            .address = address,
            .store = kvStore,
        };
    }

    pub fn start(self: *DBServer) !void {
        var server = try self.address.listen(
            .{
                .reuse_address = true,
            },
        );
        defer server.deinit();

        std.debug.print("Server listening on {s}:{d}\n", .{ self.host, self.port });
        try self.handle_connection(&server);
    }

    fn handle_connection(self: *DBServer, server: *std.net.Server) !void {
        while (true) {
            const connection = server.accept() catch |err| {
                std.debug.print("Error accepting connection: {any}\n", .{err});
                continue;
            };
            defer connection.stream.close();

            std.debug.print("Accepted connection\n", .{});

            var buffer: [256]u8 = undefined;

            // On Windows, we need to use recv() instead of ReadFile()
            // TODO: check if we're on Windows or linux
            const bytes_read_result = std.os.windows.ws2_32.recv(
                connection.stream.handle,
                @as([*]u8, @ptrCast(&buffer)),
                buffer.len,
                0,
            );

            if (bytes_read_result == std.os.windows.ws2_32.SOCKET_ERROR) {
                const err_code = std.os.windows.GetLastError();
                std.debug.print("recv() error: {d}\n", .{err_code});
                continue;
            }

            const bytes_read = @as(usize, @intCast(bytes_read_result));

            if (bytes_read == 0) {
                std.debug.print("Connection closed by client\n", .{});
                continue;
            }

            std.debug.print("Received {d} bytes\n", .{bytes_read});

            if (bytes_read > 0) {
                // if get request, read, if set request, write
                try self.parse_request(buffer[0..bytes_read]);
            }
        }
    }

    fn parse_request(self: *DBServer, request: []const u8) !void {
        if (request.len >= 3 and std.mem.eql(u8, request[0..3], "GET")) {
            try self.parse_get_request(request);
        } else if (request.len >= 3 and std.mem.eql(u8, request[0..3], "SET")) {
            try self.parse_set_request(request);
        }
    }

    fn parse_set_request(self: *DBServer, request: []const u8) !void {
        var parts = std.mem.splitScalar(u8, request, ' ');
        const key = parts.next() orelse return;
        const value = parts.next() orelse return;

        const k = try store.Key.init(key, std.heap.page_allocator);
        const v = try store.Value.init(std.heap.page_allocator);

        try self.store.insert(k, v);
        std.debug.print("Storing key: {s} with value: {s}\n", .{ key, value });
    }

    fn parse_get_request(self: *DBServer, request: []const u8) !void {
        var parts = std.mem.splitScalar(u8, request, ' ');
        const key = parts.next() orelse return;

        const k = try store.Key.init(key, std.heap.page_allocator);

        const value = try self.store.get(k, std.heap.page_allocator);
        std.debug.print("Received GET request for key: {s}, value: {any}\n", .{ key, value });
    }
};
