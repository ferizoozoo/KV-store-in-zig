const std = @import("std");
const PWriteError = std.fs.File.PWriteError;
const PReadError = std.fs.File.PReadError;
const Value = @import("store.zig").Value;
const KVStore = @import("store.zig").KVStore;

const filename = "data.db";

pub fn get_record(offset: usize, length: usize, allocator: std.mem.Allocator) ![]const u8 {
    const file = try std.fs.cwd().openFile(filename, .{ .mode = .read_only });
    defer file.close();

    var buffer = try std.ArrayList(u8).initCapacity(allocator, length);
    defer buffer.deinit(allocator);

    _ = try file.pread(buffer.items, offset);
    return buffer.items;
}

pub fn flush(buffer: *std.StringArrayHashMap([]const u8), main_index: *std.StringArrayHashMap(usize)) !void {
    var file: std.fs.File = try std.fs.cwd().createFile(filename, .{});
    defer file.close();

    try file.seekFromEnd(0);

    var bufIt = buffer.iterator();

    while (bufIt.next()) |entry| {
        const key = entry.key_ptr.*;
        const value = entry.value_ptr.*;
        try file.writeAll(key);
        try file.writeAll(" ");
        try file.writeAll(value);
        try file.writeAll("\n");

        const offset = @as(usize, try file.getEndPos()) - (key.len + 1 + value.len + 1);
        try main_index.*.put(key, offset);
    }
}

pub fn snapshot() void {
    // NOTE: read all records and turn them into a log file (naive implementation, not good)
    // TODO: refactoring by just pass an allocator into the snapshot() function
    // TODO: the transaction should be atomic
    var gpa = std.heap.GeneralPurposeAllocator(.{}){};
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const destination_file = try std.fmt.allocPrint(allocator, "{}.snap", .{
        .{std.time.timestamp()},
    });
    defer allocator.free(destination_file);

    std.fs.cwd().copyFile(filename, "snapshots", destination_file) catch {
        std.debug.print("Failed to create snapshot: {s}\n", .{destination_file});
    };
}
