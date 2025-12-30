const std = @import("std");
const sha1 = @import("std").crypto.hash.Sha1;
const persistance = @import("persistance.zig");

const offset = 0; // TODO: determine offset based on key or some mapping
const length = 1024; // TODO: determine length based on value size

// just a wrapper around a hash map for simplicity
// TODO: needs some form of persistence
pub const KVStore = struct {
    size: usize = 256,
    capacity: usize = 16,
    table: std.StringArrayHashMap([]const u8),

    pub fn new(allocator: std.mem.Allocator) !*KVStore {
        const self = try allocator.create(KVStore);
        const table = std.StringArrayHashMap([]const u8).init(allocator);

        self.* = .{
            .table = table,
        };
        return self;
    }

    pub fn insert(self: *KVStore, key: []const u8, value: []const u8) !void {
        // NOTE: Duplicating key and value to ensure they are owned by the store
        const k = try self.table.allocator.dupe(u8, key);
        const v = try self.table.allocator.dupe(u8, value);
        try self.table.put(k, v);
        _ = try persistance.save_record(k, v);
    }

    pub fn get(self: *KVStore, key: []const u8, allocator: std.mem.Allocator) !?[]const u8 {
        var value = self.table.get(key);
        if (value == null) {
            value = try persistance.get_record(offset, length, allocator);
        }
        return value;
    }

    pub fn remove(self: *KVStore, key: []const u8) void {
        _ = self.table.remove(key);
    }

    pub fn clear(self: *KVStore) void {
        self.table.clearAndFree();
    }
};

pub const HashTableError = error{
    KeyNotFound,
    TableFull,
    InvalidKey,
};
