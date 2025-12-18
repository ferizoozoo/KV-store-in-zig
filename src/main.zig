const std = @import("std");
const store = @import("store.zig");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var kv_store = store.KVStore.new(allocator);
    const key_string = "example_key";
    const key = try store.Key.init(key_string, allocator);
    var value = try store.Value.init(allocator);
    try value.set_string(allocator, "example_value");
    try kv_store.insert(key, value.*);
    const retrieved_value = try kv_store.get(key, allocator);
    if (retrieved_value) |val| {
        std.debug.print("Retrieved value: {s}\n", .{@as([]const u8, val.get_string())});
    }
}
