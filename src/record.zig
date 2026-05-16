const std = @import("std");

pub const DBRecord = struct {
    key: []const u8,
    value: []const u8,
    isDead: bool,
    createdAt: ?[]const u8,

    pub fn init(key: []const u8, value: []const u8) DBRecord {
        return DBRecord{ .key = key, .value = value, .isDead = false, .createdAt = null };
    }

    pub fn serialize(recordStr: []const u8) !DBRecord {
        var it1 = std.mem.splitScalar(u8, recordStr, ' ');
        _ = it1.next();
        const body = it1.next();
        var it2 = std.mem.splitScalar(u8, body.?, '=');
        const key = it2.next() orelse body.?;
        const value = std.mem.trimRight(u8, it2.next() orelse "", "\r\n");

        return DBRecord{ .key = key, .value = value, .isDead = false, .createdAt = "" };
    }

    pub fn deserialize(self: DBRecord, allocator: std.mem.Allocator) ![]u8 {
        const formatted = try std.fmt.allocPrint(allocator, "{s}={s} {s}", .{ self.key, self.value, self.isDead });
        return formatted;
    }

    pub fn setKey(self: *DBRecord, key: []const u8) void {
        self.key = key;
    }

    pub fn setValue(self: *DBRecord, value: []const u8) void {
        self.value = value;
    }
};
