const std = @import("std");

pub const Operation = enum {
    Insert,
    Delete,
    Update,

    pub fn toString(self: Operation) []const u8 {
        return switch (self) {
            .Insert => "INSERT",
            .Delete => "DELETE",
            .Update => "UPDATE",
        };
    }

    pub fn fromString(value: []const u8) ?Operation {
        if (std.mem.eql(u8, value, "INSERT")) return .Insert;
        if (std.mem.eql(u8, value, "DELETE")) return .Delete;
        if (std.mem.eql(u8, value, "UPDATE")) return .Update;
        return null;
    }
};
