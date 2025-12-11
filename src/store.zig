const std = @import("std");
const sha1 = @import("std").crypto.hash.Sha1;

pub const Key = struct {
    val: [20]u8 = undefined,
    ttl: u64 = 10,

    pub fn set(new_key: []const u8) Key {
        var val_out: [20]u8 = [_]u8{0} ** 20;
        sha1.hash(new_key, &val_out, .{});
        return Key{ .val = val_out };
    }
};

pub const Value = struct {
    // fixed size value for simplicity
    // TODO: this shouldn't be fixed size in a real implementation
    // NOTE: use ArrayList of u8 to allow different types of data
    val: [64]u8,

    pub fn init() Value {
        return Value{ .val = [_]u8{0} ** 64 };
    }

    pub fn set_integer(self: *Value, int_val: u64) void {
        self.val = @as([64]u8, int_val);
    }

    pub fn get_integer(self: *const Value) u64 {
        return @as(u64, self.val);
    }

    pub fn set_float(self: *Value, float_val: f64) void {
        self.val = @as([64]u8, float_val);
    }

    pub fn get_float(self: *const Value) f64 {
        return @as(f64, self.val);
    }

    pub fn set_bytes(self: *Value, byte_val: [8]u8) void {
        self.val = byte_val;
    }

    pub fn get_bytes(self: *const Value) [64]u8 {
        return self.val;
    }

    pub fn set_string(self: *Value, str_val: []const u8) void {
        const len = @min(str_val.len, 64);
        for (0..len) |i| {
            self.val[i] = str_val[i];
        }
        for (len..64) |i| {
            self.val[i] = 0;
        }
    }

    pub fn get_string(self: *const Value) []const u8 {
        return self.val[0..];
    }
};

// just a wrapper around a hash map for simplicity
// TODO: needs some form of persistence
pub const KVStore = struct {
    size: usize = 1024,
    capacity: usize = 16,

    table: std.AutoHashMap(Key, Value),

    pub fn new(allocator: std.mem.Allocator) KVStore {
        const table = std.AutoHashMap(Key, Value).init(allocator);
        return KVStore{
            .table = table,
        };
    }

    pub fn insert(self: *KVStore, key: Key, value: Value) !void {
        try self.table.put(key, value);

        // save the table to disk or persistent storage here if needed
    }

    pub fn get(self: *KVStore, key: Key) ?Value {
        return self.table.get(key);
    }

    pub fn remove(self: *KVStore, key: Key) void {
        const hashed_key = Key.set(key);
        for (self.table.items()) |item| {
            if (item.key.val == hashed_key.val) {
                self.table.remove(item.key) catch {};
            }
        }
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
