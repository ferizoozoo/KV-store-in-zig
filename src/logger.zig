const std = @import("std");

pub const LogType = enum {
    Info,
    Warning,
    Error,
};

pub const Logger = struct {
    file: std.fs.File,
    filename: []const u8,
    mutex: std.Thread.Mutex,

    pub fn init(allocator: std.mem.Allocator, filename: []const u8) !*Logger {
        const logger = try allocator.create(Logger);
        logger.file = try std.fs.cwd().createFile(filename, .{ .truncate = false });
        logger.filename = filename;
        logger.mutex = .{};
        return logger;
    }

    pub fn deinit(self: *Logger, allocator: std.mem.Allocator) void {
        self.file.close();
        allocator.destroy(self);
    }

    pub fn log(self: *Logger, message: []const u8) !void {
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.file.seekFromEnd(0);
        try self.file.writeAll(message);
        try self.file.writeAll("\n");
    }

    pub fn logWithType(self: *Logger, logType: LogType, message: []const u8) !void {
        const prefix: []const u8 = switch (logType) {
            .Info => "[INFO] ",
            .Warning => "[WARNING] ",
            .Error => "[ERROR] ",
        };
        self.mutex.lock();
        defer self.mutex.unlock();
        try self.file.seekFromEnd(0);
        try self.file.writeAll(prefix);
        try self.file.writeAll(message);
        try self.file.writeAll("\n");
    }
};
