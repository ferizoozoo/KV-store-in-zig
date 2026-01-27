const std = @import("std");
const PWriteError = std.fs.File.PWriteError;
const PReadError = std.fs.File.PReadError;
const Value = @import("store.zig").Value;
const KVStore = @import("store.zig").KVStore;

const filename = "data.db";

pub fn get_record(offset: u64, length: usize, allocator: std.mem.Allocator) ![]const u8 {
    const file = try std.fs.cwd().openFile(filename, .{ .mode = .read_only });
    defer file.close();

    var buffer = try std.ArrayList(u8).initCapacity(allocator, length);
    defer buffer.deinit(allocator);

    _ = try file.pread(buffer.items, offset);
    return buffer.items;
}

pub fn flush(store: KVStore) !void {
    var file: std.fs.File = undefined;
    const fileExists = try std.fs.cwd().access(filename, .{ .read = true });
    if (!fileExists) {
        file = try std.fs.cwd().createFile(filename, .{ .truncate = true });
    }
    file = try std.fs.cwd().openFile(filename, .{
        .mode = .read_write,
    });
    defer file.close();

    try file.seekFromEnd(0);

    for (store.table.items()) |entry| {
        const key = entry.key;
        const value = entry.value;
        try file.writeAll(key);
        try file.writeAll(" ");
        try file.writeAll(value);
        try file.writeAll("\n");
    }
}
