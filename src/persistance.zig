const std = @import("std");
const PWriteError = std.fs.File.PWriteError;
const PReadError = std.fs.File.PReadError;
const Value = @import("store.zig").Value;

const filename = "data.db";

pub fn save_record(key: []const u8, value: []const u8) !void {
    const file = try std.fs.cwd().createFile(filename, .{
        .truncate = false,
        .read = true,
    });
    defer file.close();

    try file.seekFromEnd(0);

    var buffer: [64]u8 = [_]u8{0} ** 64;
    var fbs = std.io.fixedBufferStream(&buffer);
    var writer = fbs.writer();

    try writer.writeAll(key);
    try writer.writeAll(" ");
    try writer.writeAll(value);
    try writer.writeAll("\n"); // Add a newline so records are separable
}

pub fn get_record(offset: u64, length: usize, allocator: std.mem.Allocator) !Value {
    const file = try std.fs.cwd().openFile(filename, .{ .mode = .read_only });
    defer file.close();

    var buffer = try std.ArrayList(u8).initCapacity(allocator, length);
    defer buffer.deinit(allocator);

    _ = try file.pread(buffer.items, offset);
    return Value{ .val = buffer };
}
