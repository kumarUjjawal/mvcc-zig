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
    TestUnexpectedResult,
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
            self.cleanupUncommittedTx(tx_id);
            tx.state = .{ .aborted = {} };
        }

        pub fn terminateTx(self: *Self, tx_id: TxId) DbError!void {
            const tx = self.txs.getPtr(tx_id) orelse return DbError.NoSuchTransaction;
            if (!tx_mod.canTransition(tx.state, .{ .terminated = {} })) {
                return DbError.InvalidStateTransition;
            }
            tx.state = .{ .terminated = {} };
            _ = self.txs.remove(tx_id);
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

        pub fn latestVisibleVersionForTx(self: *const Self, row_id: RowId, tx_id: TxId, tx_begin_ts: u64) ?*const Version {
            const list = self.versions.getPtr(row_id) orelse return null;
            var i: usize = list.items.len;
            while (i > 0) : (i -= 1) {
                const rv = &list.items[i - 1];
                if (self.isVisibleForTx(tx_id, tx_begin_ts, rv.*)) return rv;
            }
            return null;
        }

        pub fn scanRowIds(self: *const Self, allocator: std.mem.Allocator, tx_id: TxId) DbError![]RowId {
            const tx = self.txs.get(tx_id) orelse return DbError.NoSuchTransaction;
            switch (tx.state) {
                .active, .preparing, .committed => {},
                else => return DbError.TxNotActive,
            }

            var out = try std.ArrayList(RowId).initCapacity(allocator, 0);
            errdefer out.deinit(allocator);

            var it = self.versions.iterator();
            while (it.next()) |entry| {
                const row_id = entry.key_ptr.*;
                if (self.latestVisibleVersionForTx(row_id, tx_id, tx.begin_ts) != null) {
                    try out.append(allocator, row_id);
                }
            }

            std.mem.sort(RowId, out.items, {}, std.sort.asc(RowId));
            return try out.toOwnedSlice(allocator);
        }

        pub fn dropUnusedRowVersions(self: *Self) DbError!usize {
            const oldest_active_begin_ts = self.oldestActiveBeginTs();
            var removed: usize = 0;

            var empty_keys = try std.ArrayList(RowId).initCapacity(self.allocator, 0);
            defer empty_keys.deinit(self.allocator);

            var it = self.versions.iterator();
            while (it.next()) |entry| {
                const row_id = entry.key_ptr.*;
                const list = entry.value_ptr;

                var i: usize = 0;
                while (i < list.items.len) {
                    const rv = list.items[i];
                    var should_drop = false;

                    switch (rv.begin) {
                        .timestamp => {
                            if (rv.end) |end_union| {
                                switch (end_union) {
                                    .timestamp => |end_ts| {
                                        should_drop = if (oldest_active_begin_ts) |oldest|
                                            end_ts <= oldest
                                        else
                                            true;
                                    },
                                    .tx_id => {},
                                }
                            }
                        },
                        .tx_id => {},
                    }

                    if (should_drop) {
                        _ = list.orderedRemove(i);
                        removed += 1;
                        continue;
                    }
                    i += 1;
                }

                if (list.items.len == 0) {
                    try empty_keys.append(self.allocator, row_id);
                }
            }

            for (empty_keys.items) |row_id| {
                _ = self.versions.remove(row_id);
            }

            return removed;
        }

        fn oldestActiveBeginTs(self: *const Self) ?u64 {
            var oldest: ?u64 = null;
            var tx_it = self.txs.valueIterator();
            while (tx_it.next()) |tx| {
                switch (tx.state) {
                    .active, .preparing => {
                        if (oldest == null or tx.begin_ts < oldest.?) oldest = tx.begin_ts;
                    },
                    else => {},
                }
            }
            return oldest;
        }

        fn isVisibleForTx(self: *const Self, tx_id: TxId, tx_begin_ts: u64, rv: Version) bool {
            _ = self;
            const begin_ok = switch (rv.begin) {
                .timestamp => |rv_begin_ts| tx_begin_ts >= rv_begin_ts,
                .tx_id => |rv_begin_tx| rv_begin_tx == tx_id,
            };
            if (!begin_ok) return false;

            return if (rv.end) |rv_end| switch (rv_end) {
                .timestamp => |rv_end_ts| tx_begin_ts < rv_end_ts,
                .tx_id => |rv_end_tx| rv_end_tx != tx_id,
            } else true;
        }

        pub fn read(self: *const Self, tx_id: TxId, row_id: RowId) DbError!?RowType {
            const tx = self.txs.get(tx_id) orelse return DbError.NoSuchTransaction;
            return switch (tx.state) {
                .active, .preparing, .committed => {
                    const rv = self.latestVisibleVersionForTx(row_id, tx_id, tx.begin_ts) orelse return null;
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
            const current = self.latestVisibleVersionForTx(row_id, tx_id, tx.begin_ts) orelse return false;

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

        pub fn update(self: *Self, tx_id: TxId, row_id: RowId, row: RowType) DbError!bool {
            if (!(try self.delete(tx_id, row_id))) {
                return false;
            }

            try self.insert(tx_id, row_id, row);
            return true;
        }

        pub fn upsert(self: *Self, tx_id: TxId, row_id: RowId, row: RowType) DbError!void {
            _ = try self.delete(tx_id, row_id);
            try self.insert(tx_id, row_id, row);
        }

        fn ensureTxActive(self: *const Self, tx_id: TxId) DbError!tx_mod.Transaction {
            const tx = self.txs.get(tx_id) orelse return DbError.NoSuchTransaction;
            return switch (tx.state) {
                .active => tx,
                else => DbError.TxNotActive,
            };
        }

        fn cleanupUncommittedTx(self: *Self, tx_id: TxId) void {
            var map_it = self.versions.valueIterator();
            while (map_it.next()) |list| {
                var i: usize = 0;
                while (i < list.items.len) {
                    const rv = list.items[i];

                    switch (rv.begin) {
                        .tx_id => |begin_tx| {
                            if (begin_tx == tx_id) {
                                _ = list.orderedRemove(i);
                                continue;
                            }
                        },
                        else => {},
                    }

                    if (list.items[i].end) |end_val| {
                        switch (end_val) {
                            .tx_id => |end_tx| {
                                if (end_tx == tx_id) {
                                    list.items[i].end = null;
                                }
                            },
                            else => {},
                        }
                    }
                    i += 1;
                }
            }
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
