const std = @import("std");
const mvcc_mod = @import("../database/mvcc.zig");

pub const StorageError = error{
    OutOfMemory,
    Io,
    InvalidLog,
};

pub fn TxLog(comptime RowType: type, comptime TxIdType: type) type {
    return struct {
        const Self = @This();

        arena: std.heap.ArenaAllocator,
        records: []const mvcc_mod.LogRecord(RowType, TxIdType),
        owns_arena: bool = true,

        pub fn initEmpty(allocator: std.mem.Allocator) Self {
            return .{
                .arena = std.heap.ArenaAllocator.init(allocator),
                .records = &.{},
                .owns_arena = true,
            };
        }

        pub fn deinit(self: *Self) void {
            if (self.owns_arena) self.arena.deinit();
        }

        pub fn releaseArena(self: *Self) std.heap.ArenaAllocator {
            self.owns_arena = false;
            return self.arena;
        }
    };
}

pub fn assertStorage(comptime T: type) void {
    comptime {
        if (!@hasDecl(T, "appendLogRecord")) {
            @compileError(@typeName(T) ++ " must declare appendLogRecord(self: *T, record: anytype) !void");
        }

        if (!@hasDecl(T, "readTxLog")) {
            @compileError(@typeName(T) ++ " must declare readTxLog(self: *T, allocator: std.mem.Allocator, comptime RowType: type, comptime TxIdType: type) !TxLog(RowType, TxIdType)");
        }

        if (!@hasDecl(T, "deinit")) {
            @compileError(@typeName(T) ++ " must declare deinit(self: *T, allocator: std.mem.Allocator) void");
        }
    }
}

pub const NullStorage = struct {
    pub fn init() NullStorage {
        return .{};
    }

    pub fn deinit(self: *NullStorage, allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }

    pub fn appendLogRecord(self: *NullStorage, record: anytype) !void {
        _ = self;
        _ = record;
    }

    pub fn readTxLog(
        self: *NullStorage,
        allocator: std.mem.Allocator,
        comptime RowType: type,
        comptime TxIdType: type,
    ) !TxLog(RowType, TxIdType) {
        _ = self;
        return TxLog(RowType, TxIdType).initEmpty(allocator);
    }
};

pub const JsonFileStorage = struct {
    allocator: std.mem.Allocator,
    path: []u8,

    pub fn init(allocator: std.mem.Allocator, path: []const u8) StorageError!JsonFileStorage {
        return .{
            .allocator = allocator,
            .path = allocator.dupe(u8, path) catch return error.OutOfMemory,
        };
    }

    pub fn deinit(self: *JsonFileStorage, allocator: std.mem.Allocator) void {
        _ = allocator;
        self.allocator.free(self.path);
    }

    pub fn appendLogRecord(self: *JsonFileStorage, record: anytype) StorageError!void {
        var file = openForAppend(self.path) catch |err| return mapIoError(err);
        defer file.close();

        file.seekFromEnd(0) catch |err| return mapIoError(err);

        var json_buffer: std.io.Writer.Allocating = .init(self.allocator);
        defer json_buffer.deinit();

        std.json.Stringify.value(record, .{}, &json_buffer.writer) catch |err| switch (err) {
            error.WriteFailed => return error.OutOfMemory,
        };

        file.writeAll(json_buffer.written()) catch |err| return mapIoError(err);
        file.writeAll("\n") catch |err| return mapIoError(err);
    }

    pub fn readTxLog(
        self: *JsonFileStorage,
        allocator: std.mem.Allocator,
        comptime RowType: type,
        comptime TxIdType: type,
    ) StorageError!TxLog(RowType, TxIdType) {
        const Result = TxLog(RowType, TxIdType);
        const Record = mvcc_mod.LogRecord(RowType, TxIdType);

        var result = Result.initEmpty(allocator);
        errdefer result.deinit();

        var file = openPath(self.path, .{}) catch |err| switch (err) {
            error.FileNotFound => return result,
            else => return mapIoError(err),
        };
        defer file.close();

        const bytes = file.readToEndAlloc(result.arena.allocator(), std.math.maxInt(usize)) catch |err| {
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.Io,
            };
        };

        var records = try std.ArrayList(Record).initCapacity(result.arena.allocator(), 0);

        var lines = std.mem.splitScalar(u8, bytes, '\n');
        while (lines.next()) |line| {
            const trimmed = std.mem.trim(u8, line, " \t\r");
            if (trimmed.len == 0) continue;

            const record = std.json.parseFromSliceLeaky(Record, result.arena.allocator(), trimmed, .{
                .ignore_unknown_fields = true,
            }) catch return error.InvalidLog;

            try records.append(result.arena.allocator(), record);
        }

        result.records = try records.toOwnedSlice(result.arena.allocator());
        return result;
    }
};

fn openForAppend(path: []const u8) std.fs.File.OpenError!std.fs.File {
    return openPath(path, .{ .mode = .write_only }) catch |err| switch (err) {
        error.FileNotFound => createPath(path, .{ .truncate = false }),
        else => err,
    };
}

fn openPath(path: []const u8, flags: std.fs.File.OpenFlags) std.fs.File.OpenError!std.fs.File {
    if (std.fs.path.isAbsolute(path)) {
        return std.fs.openFileAbsolute(path, flags);
    }
    return std.fs.cwd().openFile(path, flags);
}

fn createPath(path: []const u8, flags: std.fs.File.CreateFlags) std.fs.File.OpenError!std.fs.File {
    if (std.fs.path.isAbsolute(path)) {
        return std.fs.createFileAbsolute(path, flags);
    }
    return std.fs.cwd().createFile(path, flags);
}

fn mapIoError(_: anytype) StorageError {
    return error.Io;
}
