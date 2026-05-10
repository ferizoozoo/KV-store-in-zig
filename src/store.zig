const std = @import("std");
const sha1 = @import("std").crypto.hash.Sha1;
const wal = @import("wal.zig");
const operations = @import("operations.zig").Operation;
const Mutex = std.Thread.Mutex;
const Logger = @import("logger.zig").Logger;
const RequestParser = @import("parser.zig").RequestParser;

const LENGTH = 1024;
const FILENAME = "data.db";

pub const HashTableError = error{
    KeyNotFound,
    TableFull,
    InvalidKey,
};

pub const KVStore = struct {
    allocator: std.mem.Allocator,
    main_index: std.StringArrayHashMap(usize),
    active_buffer: std.StringArrayHashMap([]const u8),
    logger: *Logger,
    rp: RequestParser,
    mu: Mutex,

    pub fn new(allocator: std.mem.Allocator, logger: *Logger, requestParser: RequestParser) !*KVStore {
        const self = try allocator.create(KVStore);

        self.* = .{ .mu = Mutex{}, .main_index = std.StringArrayHashMap(usize).init(allocator), .active_buffer = std.StringArrayHashMap([]const u8).init(allocator), .logger = logger, .allocator = allocator, .rp = requestParser };
        return self;
    }

    pub fn insert(self: *KVStore, key: []const u8, value: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.active_buffer.capacity() == self.active_buffer.count() + 2 and self.active_buffer.capacity() != 1) {
            try self.logger.logWithType(.Info, "Active buffer full, flushing to disk...");
            self.flush() catch |err| {
                return err;
            };
        }

        try self.logger.logWithParameters(.Info, "Capacity: {d}, Count: {d}", .{ self.active_buffer.capacity(), self.active_buffer.count() });

        // NOTE: Duplicating key and value to ensure they are owned by the store
        const k = try self.allocator.dupe(u8, key);
        const v = try self.allocator.dupe(u8, value);

        try wal.write_entry(operations.Insert, k, v);
        try self.active_buffer.put(k, v);
    }

    pub fn get(self: *KVStore, key: []const u8) !?[]const u8 {
        self.mu.lock();
        defer self.mu.unlock();

        var value = self.active_buffer.get(key);
        if (value == null) {
            const offset = self.main_index.get(key) orelse return null;
            value = try get_record(offset, LENGTH, self.allocator);
        }
        return value;
    }

    pub fn remove(self: *KVStore, key: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        _ = self.main_index.orderedRemove(key);
        _ = self.active_buffer.orderedRemove(key);
        // TODO: think about if the record should be removed from disk or not
        try wal.write_entry(operations.Delete, key, null);
    }

    pub fn clear(self: *KVStore) void {
        self.mu.lock();
        defer self.mu.unlock();

        self.main_index.clearAndFree();
        self.active_buffer.clearAndFree();
    }

    pub fn snapshot(self: *KVStore) !void {
        const source_file_path = "data.db";
        const destination_file_path = try std.fmt.allocPrint(self.allocator, "{}.snap", .{
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

    pub fn replay_wal_entries(self: *KVStore) !void {
        var entries = try wal.read_entries(self.allocator);
        defer {
            for (entries.items) |item| self.allocator.free(item);
            entries.deinit(self.allocator);
        }

        for (entries.items) |entry| {

            // Process each entry (e.g., apply to in-memory state)
            try self.logger.logWithParameters(.Info, "Replaying entry: {s}", .{entry});

            var parts = std.mem.splitScalar(u8, entry, ' ');

            const opStr = parts.next() orelse "";

            const op = operations.fromString(opStr) orelse continue;
            switch (op) {
                operations.Insert => try self.insert(parts.next() orelse "", parts.next() orelse ""),
                operations.Delete => try self.remove(parts.next() orelse ""),
                // operations.Update => try s.update(parts[1], parts[2]),
                else => {
                    try self.logger.logWithParameters(.Error, "Unknown operation in WAL entry: {s}", .{entry});
                },
            }
            try std.fs.cwd().deleteFile(wal.WAL_PATH);
        }
    }

    pub fn flush(self: *KVStore) !void {
        self.mu.lock();
        defer self.mu.unlock();

        var file: std.fs.File = try std.fs.cwd().createFile(FILENAME, .{ .truncate = false });
        defer file.close();

        std.debug.print("Flushing {d} entries to disk...\n", .{self.active_buffer.count()});
        try file.seekFromEnd(0);

        var bufIt = self.active_buffer.iterator();

        while (bufIt.next()) |entry| {
            std.debug.print("Flushing entry: key='{s}', value='{s}'\n", .{ entry.key_ptr.*, entry.value_ptr.* });
            const key = entry.key_ptr.*;
            const value = entry.value_ptr.*;
            try file.writeAll(key);
            try file.writeAll(" ");
            try file.writeAll(value);
            try file.writeAll("\n");

            const offset = @as(usize, try file.getEndPos()) - (key.len + 1 + value.len + 1);
            try self.main_index.put(key, offset);
        }
    }

    fn get_record(offset: usize, length: usize, allocator: std.mem.Allocator) ![]const u8 {
        const file = try std.fs.cwd().openFile(FILENAME, .{ .mode = .read_only });
        defer file.close();

        var buffer = try std.ArrayList(u8).initCapacity(allocator, length);
        defer buffer.deinit(allocator);

        _ = try file.pread(buffer.items, offset);
        return buffer.items;
    }
};
