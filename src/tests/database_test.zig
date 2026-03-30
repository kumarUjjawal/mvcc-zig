const std = @import("std");
const testing = std.testing;

const db_mod = @import("../database/database.zig");
const storage_mod = @import("../persistent_storage/storage.zig");

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
    last_tx_timestamp: u64 = 0,
    last_row_version_count: usize = 0,
    last_first_row_id: u64 = 0,
    last_first_begin_ts: u64 = 0,
    last_first_end_ts: ?u64 = null,

    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }

    pub fn appendLogRecord(self: *@This(), record: anytype) !void {
        self.append_calls += 1;
        self.last_tx_timestamp = record.tx_timestamp;
        self.last_row_version_count = record.row_versions.len;
        self.last_first_row_id = 0;
        self.last_first_begin_ts = 0;
        self.last_first_end_ts = null;

        if (record.row_versions.len != 0) {
            const first = record.row_versions[0];
            self.last_first_row_id = first.row.id;
            self.last_first_begin_ts = switch (first.begin) {
                .timestamp => |ts| ts,
                .tx_id => 0,
            };
            self.last_first_end_ts = if (first.end) |end| switch (end) {
                .timestamp => |ts| ts,
                .tx_id => null,
            } else null;
        }
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

test "Database: commit logs committed row versions to storage" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);

    var db = DB.init(testing.allocator, .{ .next = 500 }, .{});
    defer db.deinit();

    const tx = try db.beginTx();
    try db.insert(tx, 1, .{ .id = 1, .value = "a" });
    const commit_ts = try db.commitTx(tx);

    try testing.expectEqual(@as(u64, 501), commit_ts);
    try testing.expectEqual(@as(usize, 1), db.storage.append_calls);
    try testing.expectEqual(@as(u64, 501), db.storage.last_tx_timestamp);
    try testing.expectEqual(@as(usize, 1), db.storage.last_row_version_count);
    try testing.expectEqual(@as(u64, 1), db.storage.last_first_row_id);
    try testing.expectEqual(@as(u64, 500), db.storage.last_first_begin_ts);
    try testing.expect(db.storage.last_first_end_ts == null);
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

test "Database: insert creates tx-scoped version and commit materializes timestamps" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 200 }, .{});
    defer db.deinit();

    const tx_id = try db.beginTx(); // begin_ts = 200
    try db.insert(tx_id, 10, .{ .id = 10, .value = "hello" });

    const own_row = (try db.read(tx_id, 10)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("hello", own_row.value);

    const commit_ts = try db.commitTx(tx_id);
    try testing.expectEqual(@as(u64, 201), commit_ts);

    const tx2 = try db.beginTx(); // begin_ts = 202
    const row = (try db.read(tx2, 10)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("hello", row.value);
}

test "Database: delete marks visible version end and hides at end timestamp" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 300 }, .{});
    defer db.deinit();

    try db.insertVersion(5, 250, null, .{ .id = 5, .value = "alive" });

    const tx_id = try db.beginTx(); // begin_ts = 300
    try testing.expect(try db.delete(tx_id, 5));
    _ = try db.commitTx(tx_id); // commit_ts = 301

    const tx_after_end = try db.beginTx(); // begin_ts = 302
    try testing.expect((try db.read(tx_after_end, 5)) == null);
}

test "Database: rollback removes uncommitted inserted versions" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 700 }, .{});
    defer db.deinit();

    const tx1 = try db.beginTx();
    try db.insert(tx1, 44, .{ .id = 44, .value = "temp" });
    try db.rollbackTx(tx1);

    const tx2 = try db.beginTx();
    try testing.expect((try db.read(tx2, 44)) == null);
}

test "Database: rollback clears uncommitted tombstones" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 800 }, .{});
    defer db.deinit();

    try db.insertVersion(55, 600, null, .{ .id = 55, .value = "keep" });

    const tx1 = try db.beginTx();
    try testing.expect(try db.delete(tx1, 55));
    try db.rollbackTx(tx1);

    const tx2 = try db.beginTx();
    const row = (try db.read(tx2, 55)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("keep", row.value);
}

test "Database: update returns false when target row does not exist" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 900 }, .{});
    defer db.deinit();

    const tx = try db.beginTx();
    try testing.expect(!(try db.update(tx, 999, .{ .id = 999, .value = "x" })));
}

test "Database: update replaces existing row and is visible in same tx" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 1000 }, .{});
    defer db.deinit();

    try db.insertVersion(70, 800, null, .{ .id = 70, .value = "old" });

    const tx = try db.beginTx();
    try testing.expect(try db.update(tx, 70, .{ .id = 70, .value = "new" }));

    const own = (try db.read(tx, 70)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("new", own.value);
}

test "Database: upsert inserts when row is missing" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 1100 }, .{});
    defer db.deinit();

    const tx1 = try db.beginTx();
    try db.upsert(tx1, 80, .{ .id = 80, .value = "v1" });
    _ = try db.commitTx(tx1);

    const tx2 = try db.beginTx();
    const row = (try db.read(tx2, 80)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("v1", row.value);
}

test "Database: upsert overwrites existing committed row" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 1200 }, .{});
    defer db.deinit();

    const tx1 = try db.beginTx();
    try db.insert(tx1, 81, .{ .id = 81, .value = "v1" });
    _ = try db.commitTx(tx1);

    const tx2 = try db.beginTx();
    try db.upsert(tx2, 81, .{ .id = 81, .value = "v2" });

    const own = (try db.read(tx2, 81)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("v2", own.value);

    _ = try db.commitTx(tx2);

    const tx3 = try db.beginTx();
    const row = (try db.read(tx3, 81)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("v2", row.value);
}

test "Database: terminateTx removes committed transaction from active map" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 1300 }, .{});
    defer db.deinit();

    const tx = try db.beginTx();
    _ = try db.commitTx(tx);
    try db.terminateTx(tx);

    try testing.expectError(db_mod.DbError.NoSuchTransaction, db.read(tx, 1));
}

test "Database: terminateTx removes aborted transaction from active map" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 1400 }, .{});
    defer db.deinit();

    const tx = try db.beginTx();
    try db.rollbackTx(tx);
    try db.terminateTx(tx);

    try testing.expectError(db_mod.DbError.NoSuchTransaction, db.read(tx, 1));
}

test "Database: terminateTx rejects active transaction" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 1500 }, .{});
    defer db.deinit();

    const tx = try db.beginTx();
    try testing.expectError(db_mod.DbError.InvalidStateTransition, db.terminateTx(tx));
}

test "Database: dropUnusedRowVersions removes closed historical version" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 100 }, .{});
    defer db.deinit();

    try db.insertVersion(1, 10, 20, .{ .id = 1, .value = "v1" });
    try db.insertVersion(1, 20, 30, .{ .id = 1, .value = "v2" });
    try db.insertVersion(1, 30, null, .{ .id = 1, .value = "v3" });

    const removed = try db.dropUnusedRowVersions();
    try testing.expectEqual(@as(usize, 2), removed);

    const tx = try db.beginTx();
    const row = (try db.read(tx, 1)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("v3", row.value);
}

test "Database: dropUnusedRowVersions keeps versions needed by active tx snapshot" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 15 }, .{});
    defer db.deinit();

    try db.insertVersion(2, 10, 20, .{ .id = 2, .value = "old" });
    try db.insertVersion(2, 20, null, .{ .id = 2, .value = "new" });

    const old_tx = try db.beginTx(); // begin_ts = 15, must still see "old"
    try testing.expectEqual(@as(usize, 0), try db.dropUnusedRowVersions());
    try testing.expectEqualStrings("old", (try db.read(old_tx, 2)).?.value);

    db.resetClock(25);
    const new_tx = try db.beginTx();
    const row = (try db.read(new_tx, 2)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("new", row.value);
}

test "Database: commit rejects write-write conflicts" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 2000 }, .{});
    defer db.deinit();

    const tx1 = try db.beginTx();
    const tx2 = try db.beginTx();

    try db.insert(tx1, 99, .{ .id = 99, .value = "t1" });
    try db.insert(tx2, 99, .{ .id = 99, .value = "t2" });

    _ = try db.commitTx(tx1);
    try testing.expectError(db_mod.DbError.SerializationConflict, db.commitTx(tx2));

    const reader = try db.beginTx();
    const row = (try db.read(reader, 99)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("t1", row.value);
}

test "Database: commit rejects read-write conflicts" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 3000 }, .{});
    defer db.deinit();

    try db.insertVersion(100, 10, null, .{ .id = 100, .value = "base" });

    const tx1 = try db.beginTx();
    const before = (try db.read(tx1, 100)) orelse return error.TestUnexpectedResult;
    try testing.expectEqualStrings("base", before.value);

    const tx2 = try db.beginTx();
    try testing.expect(try db.update(tx2, 100, .{ .id = 100, .value = "new" }));
    _ = try db.commitTx(tx2);

    try testing.expectError(db_mod.DbError.SerializationConflict, db.commitTx(tx1));
}

test "Database: commit allows non-overlapping writes" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 4000 }, .{});
    defer db.deinit();

    const tx1 = try db.beginTx();
    const tx2 = try db.beginTx();

    try db.insert(tx1, 501, .{ .id = 501, .value = "a" });
    try db.insert(tx2, 502, .{ .id = 502, .value = "b" });

    _ = try db.commitTx(tx1);
    _ = try db.commitTx(tx2);

    const reader = try db.beginTx();
    try testing.expectEqualStrings("a", (try db.read(reader, 501)).?.value);
    try testing.expectEqualStrings("b", (try db.read(reader, 502)).?.value);
}

test "Database: delete can remove own uncommitted insert" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    var db = DB.init(testing.allocator, .{ .next = 5000 }, .{});
    defer db.deinit();

    const tx = try db.beginTx();
    try db.insert(tx, 777, .{ .id = 777, .value = "temp" });
    try testing.expect(try db.delete(tx, 777));
    try testing.expect((try db.read(tx, 777)) == null);

    _ = try db.commitTx(tx);

    const reader = try db.beginTx();
    try testing.expect((try db.read(reader, 777)) == null);
}

test "Database: recover replays committed rows and resets the clock" {
    const DB = db_mod.Database(Row, MockClock, storage_mod.JsonFileStorage);

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(dir_path);

    const path = try std.fs.path.join(testing.allocator, &.{ dir_path, "recover.log" });
    defer testing.allocator.free(path);

    {
        const storage = try storage_mod.JsonFileStorage.init(testing.allocator, path);
        var db = DB.init(testing.allocator, .{ .next = 100 }, storage);
        defer db.deinit();

        const tx1 = try db.beginTx();
        try db.insert(tx1, 1, .{ .id = 1, .value = "alpha" });
        _ = try db.commitTx(tx1);

        const tx2 = try db.beginTx();
        try db.insert(tx2, 2, .{ .id = 2, .value = "beta" });
        _ = try db.commitTx(tx2);

        const tx3 = try db.beginTx();
        try db.insert(tx3, 999, .{ .id = 999, .value = "rolled-back" });
        try db.rollbackTx(tx3);
    }

    {
        const storage = try storage_mod.JsonFileStorage.init(testing.allocator, path);
        var db = DB.init(testing.allocator, .{}, storage);
        defer db.deinit();

        try db.recover();

        try testing.expectEqual(@as(u64, 103), db.nextTimestamp());

        const tx = try db.beginTx();
        try testing.expectEqualStrings("alpha", (try db.read(tx, 1)).?.value);
        try testing.expectEqualStrings("beta", (try db.read(tx, 2)).?.value);
        try testing.expect((try db.read(tx, 999)) == null);
    }
}

test "Database: recover replays updates and deletes from the tx log" {
    const DB = db_mod.Database(Row, MockClock, storage_mod.JsonFileStorage);

    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const dir_path = try tmp.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(dir_path);

    const path = try std.fs.path.join(testing.allocator, &.{ dir_path, "recover-updates.log" });
    defer testing.allocator.free(path);

    {
        const storage = try storage_mod.JsonFileStorage.init(testing.allocator, path);
        var db = DB.init(testing.allocator, .{ .next = 200 }, storage);
        defer db.deinit();

        const seed = try db.beginTx();
        try db.insert(seed, 7, .{ .id = 7, .value = "old" });
        try db.insert(seed, 8, .{ .id = 8, .value = "keep" });
        _ = try db.commitTx(seed);

        const mutate = try db.beginTx();
        try testing.expect(try db.update(mutate, 7, .{ .id = 7, .value = "new" }));
        try testing.expect(try db.delete(mutate, 8));
        _ = try db.commitTx(mutate);
    }

    {
        const storage = try storage_mod.JsonFileStorage.init(testing.allocator, path);
        var db = DB.init(testing.allocator, .{}, storage);
        defer db.deinit();

        try db.recover();

        const tx = try db.beginTx();
        try testing.expectEqualStrings("new", (try db.read(tx, 7)).?.value);
        try testing.expect((try db.read(tx, 8)) == null);
    }
}
