const std = @import("std");
const store = @import("store.zig");
const operations = @import("operations.zig").Operation;

pub const WAL_PATH = "wal.log";

pub fn write_entry(operation: operations, key: []const u8, val: ?[]const u8) !void {
    const file = try std.fs.cwd().createFile(WAL_PATH, .{ .truncate = false });
    defer file.close();

    try file.seekFromEnd(0);

    var buf: [100]u8 = undefined;

    const entry = try switch (operation) {
        .Insert => std.fmt.bufPrint(buf[0..], "INSERT {s} {s}\n", .{ key, val orelse "" }),
        .Delete => std.fmt.bufPrint(buf[0..], "DELETE {s}\n", .{key}),
        else => unreachable,
    };

    try file.writeAll(entry);
}

pub fn read_entries(allocator: std.mem.Allocator) !std.ArrayList([]const u8) {
    var entries: std.ArrayList([]const u8) = .empty;
    errdefer {
        for (entries.items) |item| allocator.free(item);
        entries.deinit(allocator);
    }

    const file = try std.fs.cwd().openFile(WAL_PATH, .{ .mode = .read_only });
    defer file.close();

    var buffer: [1024]u8 = undefined;
    var reader = file.reader(&buffer);
    while (try reader.interface.takeDelimiter('\n')) |line| {
        const trimmed = std.mem.trimRight(u8, line, "\n");
        if (trimmed.len > 0) {
            const owned = try allocator.dupe(u8, trimmed);
            try entries.append(allocator, owned);
        }
    }

    return entries;
}
