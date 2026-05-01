const std = @import("std");
const server = @import("server.zig");
const Logger = @import("logger.zig").Logger;

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const logger = try Logger.init(allocator, "server.log");
    defer logger.deinit(allocator);
    const db_server = try server.DBServer.init(allocator, 12345, "127.0.0.1", logger);
    defer db_server.deinit(allocator);
    try db_server.start();
}
