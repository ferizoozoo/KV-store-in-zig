const std = @import("std");
const server = @import("server.zig");
const Logger = @import("logger.zig").Logger;
const store = @import("store.zig");

pub fn main() !void {
    const allocator = std.heap.page_allocator;

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    var use_wal = false;
    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "-wal")) use_wal = true;
    }

    const logger = try Logger.init(allocator, "server.log");
    defer logger.deinit(allocator);

    const s = try store.KVStore.new(allocator, logger);
    defer s.clear();

    const db_server = try server.DBServer.init(allocator, 12345, "127.0.0.1", s, use_wal);
    defer db_server.deinit(allocator);

    try db_server.start();
}
