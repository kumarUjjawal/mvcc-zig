const std = @import("std");
const mvcc = @import("mvcc.zig");
const tx_mod = @import("transaction.zig");
const clock_mod = @import("../clock.zig");
const storage_mod = @import("../persistent_storage/storage.zig");

pub const TxId = u64;
pub const RowId = u64;

pub const DbError = error{
    NoSuchTransaction,
    TxNotActive,
    InvalidStateTransition,
    OutOfMemory,
};

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
        txs: std.AutoHashMap(TxId, tx_mod.Transaction),

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
                .txs = std.AutoHashMap(TxId, tx_mod.Transaction).init(allocator),
            };
        }

        pub fn deinit(self: *Self) void {
            var it = self.versions.valueIterator();
            while (it.next()) |list| list.deinit(self.allocator);
            self.versions.deinit();
            self.txs.deinit();
            self.storage.deinit(self.allocator);
        }

        pub fn nextTxId(self: *Self) TxId {
            return self.tx_ids.fetchAdd(1, .seq_cst);
        }

        pub fn nextTimestamp(self: *Self) u64 {
            return self.clock.getTimestamp();
        }

        pub fn resetClock(self: *Self, ts: u64) void {
            self.clock.reset(ts);
        }

        pub fn beginTx(self: *Self) !TxId {
            const tx_id = self.nextTxId();
            const begin_ts = self.nextTimestamp();

            try self.txs.put(tx_id, .{
                .begin_ts = begin_ts,
                .state = .{ .active = {} },
            });

            return tx_id;
        }

        pub fn commitTx(self: *Self, tx_id: TxId) DbError!u64 {
            const tx = self.txs.getPtr(tx_id) orelse return DbError.NoSuchTransaction;

            if (!tx_mod.canTransition(tx.state, .{ .preparing = {} })) {
                return DbError.InvalidStateTransition;
            }
            tx.state = .{ .preparing = {} };

            const commit_ts = self.nextTimestamp();
            try self.materializeTxBoundaries(tx_id, tx.begin_ts, commit_ts);

            if (!tx_mod.canTransition(tx.state, .{ .committed = commit_ts })) {
                return DbError.InvalidStateTransition;
            }
            tx.state = .{ .committed = commit_ts };

            return commit_ts;
        }

        pub fn rollbackTx(self: *Self, tx_id: TxId) DbError!void {
            const tx = self.txs.getPtr(tx_id) orelse return DbError.NoSuchTransaction;
            if (!tx_mod.canTransition(tx.state, .{ .aborted = {} })) {
                return DbError.InvalidStateTransition;
            }
            tx.state = .{ .aborted = {} };
        }

        pub fn terminateTx(self: *Self, tx_id: TxId) DbError!void {
            const tx = self.txs.getPtr(tx_id) orelse return DbError.NoSuchTransaction;
            if (!tx_mod.canTransition(tx.state, .{ .terminated = {} })) {
                return DbError.InvalidStateTransition;
            }
            tx.state = .{ .terminated = {} };
        }

        pub fn getTxBeginTs(self: *const Self, tx_id: TxId) DbError!u64 {
            const tx = self.txs.get(tx_id) orelse return DbError.NoSuchTransaction;
            return tx.begin_ts;
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

        pub fn closeLatestVersion(self: *Self, row_id: RowId, end_ts: u64) !bool {
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
                if (mvcc.isVisibleVersion(RowType, TxId, tx_begin_ts, rv.*)) return rv;
            }
            return null;
        }

        pub fn read(self: *const Self, tx_id: TxId, row_id: RowId) DbError!?RowType {
            const tx = self.txs.get(tx_id) orelse return DbError.NoSuchTransaction;
            return switch (tx.state) {
                .active, .preparing, .committed => {
                    const rv = self.latestVisibleVersion(row_id, tx.begin_ts) orelse return null;
                    return rv.row;
                },
                else => DbError.TxNotActive,
            };
        }

        pub fn insert(self: *Self, tx_id: TxId, row_id: RowId, row: RowType) DbError!void {
            _ = try self.ensureTxActive(tx_id);
            const list = try self.getOrCreateVersionList(row_id);
            try list.append(self.allocator, .{
                .begin = .{ .tx_id = tx_id },
                .end = null,
                .row = row,
            });
        }

        pub fn delete(self: *Self, tx_id: TxId, row_id: RowId) DbError!bool {
            const tx = try self.ensureTxActive(tx_id);
            const current = self.latestVisibleVersion(row_id, tx.begin_ts) orelse return false;

            const list = self.versions.getPtr(row_id) orelse return false;
            if (list.items.len == 0) return false;

            var idx: usize = list.items.len;
            while (idx > 0) : (idx -= 1) {
                const rv = &list.items[idx - 1];
                if (rv.row.id == current.row.id and mvcc.isVisibleVersion(RowType, TxId, tx.begin_ts, rv.*)) {
                    rv.end = .{ .tx_id = tx_id };
                    return true;
                }
            }
            return false;
        }

        fn ensureTxActive(self: *const Self, tx_id: TxId) DbError!tx_mod.Transaction {
            const tx = self.txs.get(tx_id) orelse return DbError.NoSuchTransaction;
            return switch (tx.state) {
                .active => tx,
                else => DbError.TxNotActive,
            };
        }

        fn getOrCreateVersionList(self: *Self, row_id: RowId) !*VersionList {
            const gop = try self.versions.getOrPut(row_id);
            if (!gop.found_existing) gop.value_ptr.* = try VersionList.initCapacity(self.allocator, 0);
            return gop.value_ptr;
        }

        fn materializeTxBoundaries(self: *Self, tx_id: TxId, begin_ts: u64, commit_ts: u64) !void {
            var map_it = self.versions.valueIterator();
            while (map_it.next()) |list| {
                var i: usize = 0;
                while (i < list.items.len) : (i += 1) {
                    var rv = &list.items[i];

                    switch (rv.begin) {
                        .tx_id => |id| {
                            if (id == tx_id) {
                                rv.begin = .{ .timestamp = begin_ts };
                            }
                        },
                        else => {},
                    }

                    if (rv.end) |end_val| {
                        switch (end_val) {
                            .tx_id => |id| {
                                if (id == tx_id) {
                                    rv.end = .{ .timestamp = commit_ts };
                                }
                            },
                            else => {},
                        }
                    }
                }
            }
        }
    };
}
