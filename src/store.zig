const std = @import("std");
const sha1 = @import("std").crypto.hash.Sha1;
const storage = @import("storage.zig");
const wal = @import("wal.zig");
const operations = @import("operations.zig").Operation;

const length = 1024; // TODO: determine length based on value size

pub const HashTableError = error{
    KeyNotFound,
    TableFull,
    InvalidKey,
};

pub const KVStore = struct {
    size: usize = 256,
    main_index: std.StringArrayHashMap(usize),
    active_buffer: std.StringArrayHashMap([]const u8),

    pub fn new(allocator: std.mem.Allocator) !*KVStore {
        const self = try allocator.create(KVStore);

        self.* = .{
            .main_index = std.StringArrayHashMap(usize).init(allocator),
            .active_buffer = std.StringArrayHashMap([]const u8).init(allocator),
            .size = self.size,
        };
        return self;
    }

    pub fn insert(self: *KVStore, key: []const u8, value: []const u8) !void {
        if (self.active_buffer.capacity() == self.active_buffer.count() + 2 and self.active_buffer.capacity() != 1) {
            std.debug.print("Active buffer full, flushing to disk...\n", .{});
            self.flush() catch |err| {
                return err;
            };
        }
        std.debug.print("Capacity: {d}, Count: {d}\n", .{ self.active_buffer.capacity(), self.active_buffer.count() });
        // NOTE: Duplicating key and value to ensure they are owned by the store
        const k = try self.active_buffer.allocator.dupe(u8, key);
        const v = try self.active_buffer.allocator.dupe(u8, value);

        try wal.write_entry(operations.Insert, k, v);
        try self.active_buffer.put(k, v);
    }

    pub fn get(self: *KVStore, key: []const u8, allocator: std.mem.Allocator) !?[]const u8 {
        var value = self.active_buffer.get(key);
        if (value == null) {
            const offset = self.main_index.get(key) orelse return null;
            value = try storage.get_record(offset, length, allocator);
        }
        return value;
    }

    pub fn remove(self: *KVStore, key: []const u8) !void {
        _ = self.main_index.remove(key);
        _ = self.active_buffer.remove(key);
        try wal.write_entry(operations.Delete, key, null);
    }

    pub fn clear(self: *KVStore) void {
        self.main_index.clearAndFree();
        self.active_buffer.clearAndFree();
    }

    pub fn flush(self: *KVStore) !void {
        try storage.flush(&self.active_buffer, &self.main_index);
    }
};
