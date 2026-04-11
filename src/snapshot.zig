const std = @import("std");

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
