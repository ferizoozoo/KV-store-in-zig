const std = @import("std");
const store = @import("store.zig");
const operations = @import("operations.zig").Operation;

const WAL_PATH = "wal.log";

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
    var entries = std.ArrayList([]const u8).init(std.heap.page_allocator);
    defer entries.deinit();

    const file = try std.fs.cwd().openFile(WAL_PATH, .{ .read = true });
    defer file.close();

    var reader = std.io.BufferedReader(file.reader());
    while (true) {
        const line = try reader.readUntilDelimiterOrEof('\n');
        if (line.len == 0) break; // EOF
        try entries.append(line);
    }

    return entries;
}

pub fn replay(s: *store.KVStore) !void {
    const entries = try read_entries();
    for (entries.items) |entry| {
        // Process each entry (e.g., apply to in-memory state)
        std.debug.print("Replaying entry: {s}\n", .{entry});
        const parts = std.mem.split(entry, " ");
        if (parts.len != 2) {
            std.debug.print("Invalid WAL entry: {s}\n", .{entry});
            continue;
        }
        switch (parts[0]) {
            operations.Insert => try s.insert(parts[1], parts[2]),
            operations.Delete => s.remove(parts[1]),
            // operations.Update => try s.update(parts[1], parts[2]),
            else => std.debug.print("Unknown operation in WAL entry: {s}\n", .{entry}),
        }
        try std.fs.cwd().deleteFile(WAL_PATH);
    }
}
