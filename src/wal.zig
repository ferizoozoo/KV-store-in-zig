const std = @import("std");
const store = @import("store.zig");
const operations = @import("operations.zig").Operation;

const WAL_PATH = "wal.log";

const allocator = std.heap.page_allocator;

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

fn read_entries() !std.ArrayList([]const u8) {
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

pub fn replay(s: *store.KVStore) !void {
    var entries = try read_entries();
    defer {
        for (entries.items) |item| allocator.free(item);
        entries.deinit(allocator);
    }

    for (entries.items) |entry| {

        // Process each entry (e.g., apply to in-memory state)
        var buffer: [256]u8 = undefined;
        const infoMessage = try std.fmt.bufPrint(&buffer, "Replaying entry: {s}", .{entry});
        try s.logger.logWithType(.Info, infoMessage);

        var parts = std.mem.splitScalar(u8, entry, ' ');

        const opStr = parts.next() orelse "";

        const op = operations.fromString(opStr) orelse continue;
        switch (op) {
            operations.Insert => try s.insert(parts.next() orelse "", parts.next() orelse ""),
            operations.Delete => try s.remove(parts.next() orelse ""),
            // operations.Update => try s.update(parts[1], parts[2]),
            else => {
                const errMessage = try std.fmt.bufPrint(&buffer, "Unknown operation in WAL entry: {s}", .{entry});
                try s.logger.logWithType(.Error, errMessage);
            },
        }
        try std.fs.cwd().deleteFile(WAL_PATH);
    }
}
