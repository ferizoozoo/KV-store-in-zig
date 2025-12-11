const std = @import("std");
const store = @import("store.zig");

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    var kv_store = store.KVStore.new(allocator);
    const key = store.Key.set("example_key");
    var value = store.Value.init();
    value.set_string("example_value");
    try kv_store.insert(key, value);
    const retrieved_value = kv_store.get(key);
    if (retrieved_value) |val| {
        std.debug.print("Retrieved value: {s}\n", .{@as([]const u8, val.get_string())});
    }
}
