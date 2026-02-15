const std = @import("std");
const sha1 = @import("std").crypto.hash.Sha1;
const persistance = @import("persistance.zig");

const length = 1024; // TODO: determine length based on value size

pub const HashTableError = error{
    KeyNotFound,
    TableFull,
    InvalidKey,
};

pub const KVStore = struct {
    size: usize = 256,
    capacity: usize = 16,
    main_index: std.StringArrayHashMap(usize),
    active_buffer: std.StringArrayHashMap([]const u8),

    pub fn new(allocator: std.mem.Allocator) !*KVStore {
        const self = try allocator.create(KVStore);

        self.* = .{
            .main_index = std.StringArrayHashMap(usize).init(allocator),
            .active_buffer = std.StringArrayHashMap([]const u8).init(allocator),
        };
        return self;
    }

    pub fn insert(self: *KVStore, key: []const u8, value: []const u8) !void {
        if (self.active_buffer.capacity() == self.active_buffer.count()) {
            self.flush() catch |err| {
                return err;
            };
        }

        // NOTE: Duplicating key and value to ensure they are owned by the store
        const k = try self.active_buffer.allocator.dupe(u8, key);
        const v = try self.active_buffer.allocator.dupe(u8, value);

        // TODO: WAL must be done here before changing the data structure
        try self.active_buffer.put(k, v);
    }

    pub fn get(self: *KVStore, key: []const u8, allocator: std.mem.Allocator) !?[]const u8 {
        var value = self.active_buffer.get(key);
        if (value == null) {
            const offset = self.main_index.get(key) orelse return null;
            value = try persistance.get_record(offset, length, allocator);
        }
        return value;
    }

    pub fn remove(self: *KVStore, key: []const u8) void {
        _ = self.main_index.remove(key);
        _ = self.active_buffer.remove(key);
    }

    pub fn clear(self: *KVStore) void {
        self.main_index.clearAndFree();
        self.active_buffer.clearAndFree();
    }

    pub fn flush(self: *KVStore) !void {
        try persistance.flush(&self.active_buffer, &self.main_index);
    }
};
