const std = @import("std");
const server = @import("server.zig");
const Logger = @import("logger.zig").Logger;
const store = @import("store.zig");
const RequestParser = @import("parser.zig").RequestParser;

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

    const request_parser = RequestParser.new();

    const s = try store.KVStore.new(allocator, logger, request_parser);
    defer s.clear();

    const db_server = try server.DBServer.init(12345, "127.0.0.1", s, use_wal);
    defer db_server.deinit();

    try db_server.start();
}
