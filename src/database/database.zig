const std = @import("std");
const mvcc = @import("mvcc.zig");
const clock_mod = @import("../clock.zig");
const storage_mod = @import("../persistent_storage/storage.zig");

pub const TxId = u64;
pub const RowId = u64;

pub fn Database(comptime RowType: type, comptime ClockType: type, comptime StorageType: type) type {
    clock_mod.assertLogicalClock(ClockType);
    storage_mod.assertStorage(StorageType);

    const Version = mvcc.RowVersion(RowType, TxId);
    const VersionList = std.ArrayList(Version);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        clock: ClockType,
        storage: StorageType,
        tx_ids: std.atomic.Value(u64),
        versions: std.AutoHashMap(RowId, VersionList),

        pub fn init(
            allocator: std.mem.Allocator,
            clock: ClockType,
            storage: StorageType,
        ) Self {
            return .{
                .allocator = allocator,
                .clock = clock,
                .storage = storage,
                .tx_ids = std.atomic.Value(u64).init(1),
                .versions = std.AutoHashMap(RowId, VersionList).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.versions.valueIterator();
            while (it.next()) |list| list.deinit(self.allocator);
            self.versions.deinit();
            self.storage.deinit(self.allocator);
        }

        pub fn nextTxId(self: *Self) TxId {
            return self.tx_ids.fetchAdd(1, .seq_cst);
        }

        pub fn beginTx(self: *Self) TxId {
            return self.nextTxId();
        }

        pub fn nextTimestamp(self: *Self) u64 {
            return self.clock.getTimestamp();
        }

        pub fn resetClock(self: *Self, ts: u64) void {
            self.clock.reset(ts);
        }

        pub fn appendCommittedWrites(
            self: *Self,
            tx_id: TxId,
            rows: []const RowType,
        ) !u64 {
            const commit_ts = self.nextTimestamp();
            try self.storage.appendLogRecord(.{
                .tx_id = tx_id,
                .commit_ts = commit_ts,
                .rows = rows,
            });
            return commit_ts;
        }

        pub fn insertVersion(
            self: *Self,
            row_id: RowId,
            begin_ts: u64,
            end_ts: ?u64,
            row: RowType,
        ) !void {
            const gop = try self.versions.getOrPut(row_id);
            if (!gop.found_existing) gop.value_ptr.* = try VersionList.initCapacity(self.allocator, 0);

            try gop.value_ptr.append(self.allocator, .{
                .begin = .{ .timestamp = begin_ts },
                .end = if (end_ts) |v| .{ .timestamp = v } else null,
                .row = row,
            });
        }

        pub fn closeLatestVersion(
            self: *Self,
            row_id: RowId,
            end_ts: u64,
        ) !bool {
            const list = self.versions.getPtr(row_id) orelse return false;
            if (list.items.len == 0) return false;
            list.items[list.items.len - 1].end = .{ .timestamp = end_ts };
            return true;
        }

        pub fn latestVisibleVersion(
            self: *const Self,
            row_id: RowId,
            tx_begin_ts: u64,
        ) ?*const Version {
            const list = self.versions.getPtr(row_id) orelse return null;
            var i: usize = list.items.len;
            while (i > 0) : (i -= 1) {
                const rv = &list.items[i - 1];
                if (mvcc.isVisibleVersion(RowType, TxId, tx_begin_ts, rv.*)) {
                    return rv;
                }
            }
            return null;
        }
    };
}
