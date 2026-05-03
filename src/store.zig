const std = @import("std");
const sha1 = @import("std").crypto.hash.Sha1;
const storage = @import("storage.zig");
const wal = @import("wal.zig");
const operations = @import("operations.zig").Operation;
const Mutex = std.Thread.Mutex;
const Logger = @import("logger.zig").Logger;

const length = 1024; // TODO: determine length based on value size

pub const HashTableError = error{
    KeyNotFound,
    TableFull,
    InvalidKey,
};

// TODO: maybe we can have the allocator as a field in the KVStore struct and use it across the codebase instead of passing it around
pub const KVStore = struct {
    main_index: std.StringArrayHashMap(usize),
    active_buffer: std.StringArrayHashMap([]const u8),
    logger: *Logger,
    mu: Mutex,

    pub fn new(allocator: std.mem.Allocator, logger: *Logger) !*KVStore {
        const self = try allocator.create(KVStore);

        self.* = .{
            .mu = Mutex{},
            .main_index = std.StringArrayHashMap(usize).init(allocator),
            .active_buffer = std.StringArrayHashMap([]const u8).init(allocator),
            .logger = logger,
        };
        return self;
    }

    pub fn insert(self: *KVStore, key: []const u8, value: []const u8) !void {
        self.mu.lock();
        defer self.mu.unlock();

        if (self.active_buffer.capacity() == self.active_buffer.count() + 2 and self.active_buffer.capacity() != 1) {
            std.debug.print("Active buffer full, flushing to disk...\n", .{});
            self.flushLocked() catch |err| {
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
        self.mu.lock();
        defer self.mu.unlock();

        var value = self.active_buffer.get(key);
        if (value == null) {
            const offset = self.main_index.get(key) orelse return null;
            value = try storage.get_record(offset, length, allocator);
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

    pub fn flush(self: *KVStore) !void {
        self.mu.lock();
        defer self.mu.unlock();

        try self.flushLocked();
    }

    fn flushLocked(self: *KVStore) !void {
        try storage.flush(&self.active_buffer, &self.main_index);
    }
};
