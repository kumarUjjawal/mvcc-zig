const std = @import("std");
const testing = std.testing;

const db_mod = @import("../database/database.zig");

const Row = struct {
    id: u64,
    value: []const u8,
};

const MockClock = struct {
    next: u64 = 10,

    pub fn getTimestamp(self: *@This()) u64 {
        const v = self.next;
        self.next += 1;
        return v;
    }

    pub fn reset(self: *@This(), ts: u64) void {
        self.next = ts;
    }
};

const MockStorage = struct {
    append_calls: usize = 0,
    last_tx_id: u64 = 0,
    last_commit_ts: u64 = 0,
    last_row_count: usize = 0,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }

    pub fn appendLogRecord(self: *@This(), record: anytype) !void {
        self.append_calls += 1;
        self.last_tx_id = record.tx_id;
        self.last_commit_ts = record.commit_ts;
        self.last_row_count = record.rows.len;
    }
};

test "Database: beginTx yields unique ids starting at 1" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);

    var db = DB.init(testing.allocator, .{}, .{});
    defer db.deinit();

    const t1 = try db.beginTx();
    const t2 = try db.beginTx();
    const t3 = try db.beginTx();

    try testing.expectEqual(@as(u64, 1), t1);
    try testing.expectEqual(@as(u64, 2), t2);
    try testing.expectEqual(@as(u64, 3), t3);
}

test "Database: nextTimestamp delegates to clock" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);

    var db = DB.init(testing.allocator, .{ .next = 50 }, .{});
    defer db.deinit();

    try testing.expectEqual(@as(u64, 50), db.nextTimestamp());
    try testing.expectEqual(@as(u64, 51), db.nextTimestamp());

    db.resetClock(100);
    try testing.expectEqual(@as(u64, 100), db.nextTimestamp());
}

test "Database: appendCommittedWrites writes tx id, commit ts, and rows to storage" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);

    var db = DB.init(testing.allocator, .{ .next = 500 }, .{});
    defer db.deinit();

    const rows = [_]Row{
        .{ .id = 1, .value = "a" },
        .{ .id = 2, .value = "b" },
    };

    const commit_ts = try db.appendCommittedWrites(12, rows[0..]);

    try testing.expectEqual(@as(u64, 500), commit_ts);
    try testing.expectEqual(@as(usize, 1), db.storage.append_calls);
    try testing.expectEqual(@as(u64, 12), db.storage.last_tx_id);
    try testing.expectEqual(@as(u64, 500), db.storage.last_commit_ts);
    try testing.expectEqual(@as(usize, 2), db.storage.last_row_count);
}

test "Database: latestVisibleVersion resolves newest visible version" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{}, .{});
    defer db.deinit();

    try db.insertVersion(1, 10, 20, .{ .id = 1, .value = "v1" });
    try db.insertVersion(1, 20, null, .{ .id = 1, .value = "v2" });

    const at_15 = db.latestVisibleVersion(1, 15) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("v1", at_15.row.value);

    const at_25 = db.latestVisibleVersion(1, 25) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("v2", at_25.row.value);
}

test "Database: closeLatestVersion sets tombstone boundary" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{}, .{});
    defer db.deinit();

    try db.insertVersion(7, 5, null, .{ .id = 7, .value = "alive" });
    try testing.expect(try db.closeLatestVersion(7, 9));

    try testing.expect(db.latestVisibleVersion(7, 8) != null);
    try testing.expect(db.latestVisibleVersion(7, 9) == null);
}

test "Database: beginTx inserts tx metadata and read uses tx begin timestamp" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 100 }, .{});
    defer db.deinit();

    const tx_id = try db.beginTx();
    try db.insertVersion(1, 90, 110, .{ .id = 1, .value = "old" });
    try db.insertVersion(1, 110, null, .{ .id = 1, .value = "new" });

    const row = (try db.read(tx_id, 1)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("old", row.value);
}
