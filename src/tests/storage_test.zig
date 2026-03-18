const std = @import("std");
const testing = std.testing;
const storage_mod = @import("../persistent_storage/storage.zig");

test "NullStorage: appendLogRecord accepts arbitrary record" {
    var storage = storage_mod.NullStorage.init();
    defer storage.deinit(testing.allocator);

    const rec = .{
        .tx_id = @as(u64, 7),
        .commit_ts = @as(u64, 99),
        .rows = &[_]u8{ 1, 2, 3 },
    };

    try storage.appendLogRecord(rec);
}

test "assertStorage accepts a valid storage type" {
    const MockStorage = struct {
        pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
            _ = self;
            _ = allocator;
        }

        pub fn appendLogRecord(self: *@This(), record: anytype) !void {
            _ = self;
            _ = record;
        }
    };

    storage_mod.assertStorage(MockStorage);
}
