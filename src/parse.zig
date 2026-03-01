const std = @import("std");
const store = @import("store.zig");

pub fn parse_request(s: *store.KVStore, request: []const u8) !void {
    if (request.len <= 2) {
        return;
    }

    const first_three = request[0..3];

    if (std.mem.eql(u8, first_three, "GET")) {
        try parse_get_request(s, request);
    } else if (std.mem.eql(u8, first_three, "SET")) {
        try parse_set_request(s, request);
    } else if (std.mem.eql(u8, first_three, "SNP")) {
        try parse_snapshot_request(s);
    } else {
        std.debug.print("Unknown command: {s}\n", .{request});
    }
}

fn parse_set_request(s: *store.KVStore, request: []const u8) !void {
    var parts = std.mem.splitScalar(u8, request, ' ');
    _ = parts.next(); // skip "SET"
    const key_value = parts.next() orelse return;
    var key_value_parts = std.mem.splitScalar(u8, key_value, '=');
    const k = key_value_parts.next() orelse return;
    const v = key_value_parts.next() orelse return;

    const key = std.mem.trim(u8, k, "\n\r\t");
    const value = std.mem.trim(u8, v, "\n\r\t");

    try s.insert(key, value);
    std.debug.print("Storing key: {s} with value: {s}\n", .{ key, value });
}

fn parse_get_request(s: *store.KVStore, request: []const u8) !void {
    var parts = std.mem.splitScalar(u8, request, ' ');
    _ = parts.next(); // skip "GET"
    const key_value = parts.next() orelse return;
    var key_value_parts = std.mem.splitScalar(u8, key_value, '=');
    const key = key_value_parts.next() orelse return;
    const k = std.mem.trim(u8, key, "\n\r\t");

    const value = try s.get(k, std.heap.page_allocator);
    if (value == null) {
        std.debug.print("Key: {s} not found\n", .{k});
        return;
    }

    const v = std.mem.trim(u8, value.?, "\n\r\t");

    std.debug.print("Received GET request for key: {s}, value: {s}\n", .{ k, v });
}

fn parse_snapshot_request(s: *store.KVStore) !void {
    // Flush any pending data first
    try s.flush();
    std.debug.print("Flushed active buffer\n", .{});

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
