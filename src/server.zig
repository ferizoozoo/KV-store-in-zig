// TODO: this needs definitely a refactor

const std = @import("std");
const store = @import("store.zig");
const parse_request = @import("parse.zig").parse_request;

const requestType = enum {
    GET,
    SET,
};

pub const DBServer = struct {
    port: u16,
    host: []const u8,
    address: std.net.Address,
    store: *store.KVStore,

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
        try handle_connection(self.store, &server);
    }

    fn handle_connection(s: *store.KVStore, server: *std.net.Server) !void {
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
                try parse_request(s, buffer[0..bytes_read]);
            }
        }
    }
};
