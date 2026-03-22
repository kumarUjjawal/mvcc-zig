const std = @import("std");
const testing = std.testing;

const db_mod = @import("../database/database.zig");
const cursor_mod = @import("../cursor.zig");

const Row = struct {
    id: u64,
    value: []const u8,
};

const MockClock = struct {
    next: u64 = 0,

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
    pub fn deinit(self: *@This(), allocator: std.mem.Allocator) void {
        _ = self;
        _ = allocator;
    }

    pub fn appendLogRecord(self: *@This(), record: anytype) !void {
        _ = self;
        _ = record;
    }
};

test "ScanCursor iterates visible rows in sorted row-id order" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    const Cursor = cursor_mod.ScanCursor(Row, MockClock, MockStorage);

    var db = DB.init(testing.allocator, .{ .next = 100 }, .{});
    defer db.deinit();

    try db.insertVersion(2, 10, null, .{ .id = 2, .value = "b" });
    try db.insertVersion(1, 10, null, .{ .id = 1, .value = "a" });

    const tx = try db.beginTx();

    var cursor = try Cursor.init(testing.allocator, &db, tx);
    defer cursor.deinit();

    try testing.expect(!cursor.isEmpty());

    try testing.expectEqual(@as(u64, 1), cursor.currentRowId().?);
    try testing.expectEqualStrings("a", (try cursor.currentRow()).?.value);

    try testing.expect(cursor.forward());
    try testing.expectEqual(@as(u64, 2), cursor.currentRowId().?);
    try testing.expectEqualStrings("b", (try cursor.currentRow()).?.value);

    try testing.expect(!cursor.forward());
}

test "ScanCursor is empty when no row is visible" {
    const DB = db_mod.Database(Row, MockClock, MockStorage);
    const Cursor = cursor_mod.ScanCursor(Row, MockClock, MockStorage);

    var db = DB.init(testing.allocator, .{ .next = 500 }, .{});
    defer db.deinit();

    try db.insertVersion(7, 10, 20, .{ .id = 7, .value = "gone" });
    const tx = try db.beginTx();

    var cursor = try Cursor.init(testing.allocator, &db, tx);
    defer cursor.deinit();

    try testing.expect(cursor.isEmpty());
    try testing.expect(cursor.currentRowId() == null);
    try testing.expect((try cursor.currentRow()) == null);
}
