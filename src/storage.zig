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

    std.debug.print("Flushing {d} entries to disk...\n", .{buffer.count()});
    try file.seekFromEnd(0);

    var bufIt = buffer.iterator();

    while (bufIt.next()) |entry| {
        std.debug.print("Flushing entry: key='{s}', value='{s}'\n", .{ entry.key_ptr.*, entry.value_ptr.* });
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

// TODO: this should be in a separate file, but we need to avoid circular dependencies for now
pub fn snapshot() !void {
    const source_file_path = "data.db";
    const destination_file_path = try std.fmt.allocPrint(std.heap.page_allocator, "{}.snap", .{
        std.time.timestamp(),
    });
    defer std.heap.page_allocator.free(destination_file_path);

    const cwd = std.fs.cwd();
    // ensure the snapshots directory exists (ignore error if it already does)
    cwd.makeDir("snapshots") catch |err| {
        if (err == error.PathAlreadyExists) {
            // This is fine! The folder is already there.
        } else {
            // Something actually went wrong (e.g., PermissionDenied)
            std.debug.print("Failed to create snapshots directory: {}\n", .{err});
            return err;
        }
    };

    var snapshots_dir = try cwd.openDir("snapshots", .{});
    defer snapshots_dir.close();

    cwd.copyFile(source_file_path, snapshots_dir, destination_file_path, .{}) catch |err| {
        if (err == error.FileNotFound) {
            std.debug.print("No data file to snapshot yet (data.db not found)\n", .{});
        } else {
            std.debug.print("Failed to create snapshot: {s} (error: {})\n", .{ destination_file_path, err });
        }
    };
    std.debug.print("Snapshot request completed\n", .{});
}
