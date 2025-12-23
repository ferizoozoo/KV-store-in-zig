const std = @import("std");

pub const DBServer = struct {
    port: u16,
    host: []const u8,
    address: std.net.Address,

    pub fn init(port: u16, host: []const u8) !DBServer {
        const address = try std.net.Address.parseIp4(host, port);
        return DBServer{
            .port = port,
            .host = host,
            .address = address,
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
                parse_request(buffer[0..bytes_read]);
            }
        }
    }

    fn parse_request(request: []const u8) void {
        if (request.len >= 3 and std.mem.eql(u8, request[0..3], "GET")) {
            // Handle GET request
            const key_start = 4; // Skip "GET "
            var key_end = request.len;
            // Trim trailing whitespace
            while (key_end > key_start and (request[key_end - 1] == ' ' or request[key_end - 1] == '\n' or request[key_end - 1] == '\r')) {
                key_end -= 1;
            }
            if (key_end > key_start) {
                const key = request[key_start..key_end];
                std.debug.print("Received GET request for key: {s}\n", .{key});
            }
        } else if (request.len >= 3 and std.mem.eql(u8, request[0..3], "SET")) {
            // Handle SET request
            std.debug.print("Received SET request\n", .{});
        }
    }
};
