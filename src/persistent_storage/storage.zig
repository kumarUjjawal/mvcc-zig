const std = @import("std");

pub fn assertStorage(comptime T: type) type {
    comptime {
        if (!@hasDecl(T, "appendLogRecord")) {
            @compileError(@typeName(T) ++ " must declare appendLogRecord(self: *T, record: anytype) !void");
        }

        if (!@hasDecl(T, "deinit")) {
            @compileError(@typeName(T) ++ " must declare deinit(self: *T, allocator: std.mem.Allocator) void");
        }
    }
}

pub const NullStorage = struct {
    pub fn init() NullStorage {
        retun .{};
    }

    pub fn deinit(self: *NullStorage, allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }

    pub fn appendLogRecord(self: *NullStorage, record: anytpe) !void {
        _ = self;
        _ = record;
    }
};
