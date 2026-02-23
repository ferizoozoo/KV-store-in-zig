// TODO: Refactor for cross-platform networking and portability
// - Replace platform-specific socket calls with `stream.read`/`stream.write`
// - Avoid `std.os.windows` references so code builds on Linux and Windows
// - Add unit tests for connection handling on both OSes

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

            // Use the stream API which is cross-platform (works on Linux & Windows)
            const bytes_read = connection.stream.read(&buffer) catch |err| {
                std.debug.print("Read error: {any}\n", .{err});
                continue;
            };

            if (bytes_read == 0) {
                std.debug.print("Connection closed by client\n", .{});
                continue;
            }

            std.debug.print("Received {d} bytes\n", .{bytes_read});

            if (bytes_read > 0) {
                try parse_request(s, buffer[0..bytes_read]);
            }
        }
    }
};
