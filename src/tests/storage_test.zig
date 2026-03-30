const std = @import("std");
const testing = std.testing;
const mvcc_mod = @import("../database/mvcc.zig");
const storage_mod = @import("../persistent_storage/storage.zig");

const Row = struct {
    id: u64,
    value: []const u8,
};

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

test "NullStorage: readTxLog returns an empty log" {
    var storage = storage_mod.NullStorage.init();
    defer storage.deinit(testing.allocator);

    var tx_log = try storage.readTxLog(testing.allocator, Row, u64);
    defer tx_log.deinit();

    try testing.expectEqual(@as(usize, 0), tx_log.records.len);
}

test "JsonFileStorage: appendLogRecord and readTxLog round-trip committed versions" {
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(dir_path);

    const path = try std.fs.path.join(testing.allocator, &.{ dir_path, "tx.log" });
    defer testing.allocator.free(path);

    var storage = try storage_mod.JsonFileStorage.init(testing.allocator, path);
    defer storage.deinit(testing.allocator);

    const Version = mvcc_mod.RowVersion(Row, u64);
    const Record = mvcc_mod.LogRecord(Row, u64);

    const first_versions = [_]Version{
        .{
            .begin = .{ .timestamp = 10 },
            .end = null,
            .row = .{ .id = 1, .value = "alpha" },
        },
    };
    const second_versions = [_]Version{
        .{
            .begin = .{ .timestamp = 10 },
            .end = .{ .timestamp = 11 },
            .row = .{ .id = 1, .value = "alpha" },
        },
        .{
            .begin = .{ .timestamp = 10 },
            .end = null,
            .row = .{ .id = 2, .value = "beta" },
        },
    };

    try storage.appendLogRecord(Record{
        .tx_timestamp = 10,
        .row_versions = first_versions[0..],
    });
    try storage.appendLogRecord(Record{
        .tx_timestamp = 11,
        .row_versions = second_versions[0..],
    });

    var tx_log = try storage.readTxLog(testing.allocator, Row, u64);
    defer tx_log.deinit();

    try testing.expectEqual(@as(usize, 2), tx_log.records.len);
    try testing.expectEqual(@as(u64, 10), tx_log.records[0].tx_timestamp);
    try testing.expectEqual(@as(usize, 1), tx_log.records[0].row_versions.len);
    try testing.expectEqual(@as(u64, 11), tx_log.records[1].tx_timestamp);
    try testing.expectEqual(@as(usize, 2), tx_log.records[1].row_versions.len);
    try testing.expectEqual(@as(u64, 2), tx_log.records[1].row_versions[1].row.id);
    try testing.expectEqualStrings("beta", tx_log.records[1].row_versions[1].row.value);
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

        pub fn readTxLog(
            self: *@This(),
            allocator: std.mem.Allocator,
            comptime RowType: type,
            comptime TxIdType: type,
        ) !storage_mod.TxLog(RowType, TxIdType) {
            _ = self;
            return storage_mod.TxLog(RowType, TxIdType).initEmpty(allocator);
        }
    };

    storage_mod.assertStorage(MockStorage);
}
