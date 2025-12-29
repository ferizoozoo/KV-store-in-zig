const std = @import("std");
const sha1 = @import("std").crypto.hash.Sha1;
const persistance = @import("persistance.zig");

const offset = 0; // TODO: determine offset based on key or some mapping
const length = 1024; // TODO: determine length based on value size

pub const Key = struct {
    val: []const u8,
    ttl: u64 = 10,

    pub fn init(new_key: []const u8, allocator: std.mem.Allocator) !*Key {
        const owned_val = try allocator.dupe(u8, new_key);
        const self = try allocator.create(Key);
        self.* = Key{ .val = owned_val, .ttl = 10 };
        return self;
    }

    pub fn deinit(self: *Key, allocator: std.mem.Allocator) void {
        allocator.free(self.val);
    }

    pub fn get(self: *Key) []const u8 {
        return self.val;
    }
};

pub const Value = struct {
    // fixed size value for simplicity
    // TODO: this shouldn't be fixed size in a real implementation
    // NOTE: use ArrayList of u8 to allow different types of data
    val: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) !*Value {
        const list = try std.ArrayList(u8).initCapacity(allocator, 64);
        const self = try allocator.create(Value);
        self.* = Value{ .val = list };
        return self;
    }

    pub fn set_integer(self: *Value, int_val: u64) void {
        self.val.clearRetainingCapacity();
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, int_val, .little);
        self.val.appendSlice(&bytes) catch unreachable;
    }

    pub fn get_integer(self: *const Value) u64 {
        if (self.val.items.len < 8) return 0;
        return std.mem.readInt(u64, self.val.items[0..8], .little);
    }

    pub fn set_float(self: *Value, float_val: f64) void {
        self.val.clearRetainingCapacity();
        const bits: u64 = @bitCast(float_val);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, bits, .little);
        self.val.appendSlice(&bytes) catch unreachable;
    }

    pub fn get_float(self: *const Value) f64 {
        if (self.val.items.len < 8) return 0.0;
        const bits = std.mem.readInt(u64, self.val.items[0..8], .little);
        return @bitCast(bits);
    }

    pub fn set_bytes(self: *Value, byte_val: [8]u8) void {
        self.val = byte_val;
    }

    pub fn get_bytes(self: *const Value) [64]u8 {
        return self.val;
    }

    pub fn set_string(self: *Value, allocator: std.mem.Allocator, str_val: []const u8) !void {
        if (str_val.len == 0) {
            return;
        }
        var str_val_list = try std.ArrayList(u8).initCapacity(allocator, 64);
        _ = try str_val_list.appendSlice(allocator, str_val);
        self.val = str_val_list;
    }

    pub fn get_string(self: *const Value) []const u8 {
        return self.val.items;
    }
};

pub const KeyContext = struct {
    pub fn hash(self: KeyContext, key: Key) u64 {
        _ = self;
        var hasher = std.hash.Wyhash.init(0);
        std.hash.autoHashStrat(&hasher, key, .Deep);
        return hasher.final();
    }

    pub fn eql(self: KeyContext, a: Key, b: Key) bool {
        _ = self;
        return a.ttl == b.ttl and std.mem.eql(u8, a.val, b.val);
    }
};

// just a wrapper around a hash map for simplicity
// TODO: needs some form of persistence
pub const KVStore = struct {
    size: usize = 1024,
    capacity: usize = 16,

    table: std.HashMap(Key, Value, KeyContext, std.hash_map.default_max_load_percentage),

    pub fn new(allocator: std.mem.Allocator) KVStore {
        return .{
            .table = std.HashMap(Key, Value, KeyContext, std.hash_map.default_max_load_percentage).init(allocator),
        };
    }

    pub fn insert(self: *KVStore, key: *Key, value: *Value) !void {
        try self.table.put(key.*, value.*);
        _ = try persistance.save_record(key.val, value.val.items);
    }

    pub fn get(self: *KVStore, key: *Key, allocator: std.mem.Allocator) !?Value {
        var value = self.table.get(key.*);
        if (value == null) {
            value = try persistance.get_record(offset, length, allocator);
        }

        return value;
    }

    pub fn remove(self: *KVStore, key: Key) void {
        _ = self.table.remove(key.val);
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
