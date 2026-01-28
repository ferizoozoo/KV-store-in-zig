const std = @import("std");
const sha1 = @import("std").crypto.hash.Sha1;
const persistance = @import("persistance.zig");

const length = 1024; // TODO: determine length based on value size

pub const HashTableError = error{
    KeyNotFound,
    TableFull,
    InvalidKey,
};

// TODO: The active_buffer can be used for batching writes before flushing to main_index
// TODO: The main_index should use pointers to values stored in persistance layer
pub const KVStore = struct {
    size: usize = 256,
    capacity: usize = 16,
    main_index: std.StringArrayHashMap(usize),
    active_buffer: std.StringArrayHashMap([]const u8),

    pub fn new(allocator: std.mem.Allocator) !*KVStore {
        const self = try allocator.create(KVStore);
        const table = std.StringArrayHashMap(usize).init(allocator);
        const buffer = std.StringArrayHashMap([]const u8).init(allocator);

        self.* = .{
            .main_index = table,
            .active_buffer = buffer,
        };
        return self;
    }

    pub fn insert(self: *KVStore, key: []const u8, value: []const u8) !void {
        // NOTE: Duplicating key and value to ensure they are owned by the store
        const k = try self.active_buffer.allocator.dupe(u8, key);
        const v = try self.active_buffer.allocator.dupe(u8, value);
        try self.active_buffer.put(k, v);
    }

    pub fn get(self: *KVStore, key: []const u8, allocator: std.mem.Allocator) !?[]const u8 {
        var value = self.active_buffer.get(key);
        if (value == null) {
            const offset = switch (self.main_index.get(key)) {
                null => return null,
                else => |o| o,
            };
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
        try persistance.flush(self.active_buffer);
    }
};
