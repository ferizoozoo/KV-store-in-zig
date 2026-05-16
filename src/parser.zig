const std = @import("std");
const store = @import("store.zig");
const DBRecord = @import("record.zig").DBRecord;

pub const RequestParser = struct {
    pub fn new() RequestParser {
        return RequestParser{};
    }

    // TODO: maybe the parsers should wrap the data (key, value) into a record
    pub fn parse_request(self: RequestParser, s: *store.KVStore, request: []const u8) !void {
        _ = self;

        if (request.len <= 2) {
            return;
        }

        const first_three = request[0..3];
        std.debug.print("Received request: {s}\n", .{request});

        // NOTE: add request protocols here (like GET, SET, ...)
        if (std.mem.eql(u8, first_three, "GET")) {
            try parse_get_request(s, request);
        } else if (std.mem.eql(u8, first_three, "SET")) {
            try parse_set_request(s, request);
        } else if (std.mem.eql(u8, first_three, "SNP")) {
            try parse_snapshot_request(s);
        } else {
            std.debug.print("Unknown command: {s}\n", .{request});
        }
    }

    fn parse_set_request(s: *store.KVStore, request: []const u8) !void {
        const record = try DBRecord.serialize(request);
        try s.insert(record);
        std.debug.print("Storing key: {s} with value: {s}\n", .{ record.key, record.value });
    }

    fn parse_get_request(s: *store.KVStore, request: []const u8) !void {
        // TODO: maybe a DBRequest struct is better to have here
        const record = try DBRecord.serialize(request);

        const result = try s.get(record.key);
        if (result == null) {
            std.debug.print("Key: {s} not found\n", .{record.key});
            return;
        }

        std.debug.print("Received GET request for key: {s}, value: {s}\n", .{ result.?.key, result.?.value });
    }

    fn parse_delete_request(s: *store.KVStore, request: []const u8) !void {
        const record = try DBRecord.serialize(request);
        try s.remove(record.key);

        std.debug.print("Received DEL request for key: {s}", .{record.key});
    }

    fn parse_snapshot_request(s: *store.KVStore) !void {
        // Flush any pending data first
        try s.flush();
        std.debug.print("Flushed active buffer\n", .{});
        try s.logger.logWithType(.Info, "Flushed active buffer before snapshot");

        // Create a snapshot of the current state
        try s.snapshot();
        std.debug.print("Created snapshot\n", .{});
        try s.logger.logWithType(.Info, "Created snapshot");
    }
};
