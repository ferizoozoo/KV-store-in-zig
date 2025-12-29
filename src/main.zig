const std = @import("std");
const store = @import("store.zig");
const server = @import("server.zig");

pub fn main() !void {
    var db_server = try server.DBServer.init(12345, "127.0.0.1");
    try db_server.start();
}
