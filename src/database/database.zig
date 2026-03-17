const std = @import("std");
const clock_mod = @import("../clock.zig");
const storage_mod = @import("../persistent_storage/storage.zig");

pub const TxId = u64;

pub fn Database(comptime RowType: type, comptime ClockType: type, comptime: StorageType: type) type {
    clock_mod.assertLogicalClock(ClockType);
    storage_mod.assertStorage(StorageType);

    return struct {
        const Self = @This();

        allocator: std.mem.Allocator,
        clock: ClockType,
        storage: StorageType,
        tx_ids: std.atomic.Value(u64) = std.atomic.Value(u64).init(1),

        pub fn init(
            allocator: std.mem.Allocator,
            clock: ClockType,
            storage: StorageType,
        ) Self {
            return .{
                .allocator = allocator,
                .clock = clock,
                .storage = storage,
            };
        }

        pub fn deinit(self: *Self) void {
            self.storage.deinit(self.allocator);
        }

        pub fn nextTxId(self: *Self) TxId {
            return self.tx_ids.fetchAdd(1, .seq_cst);
        }

        pub fn biginTx(self: *Self) TxId {
            return nextTxId();
        }

        pub fn nextTimestamp(self: *Self) u64 {
            return self.clock.getTimestamp();
        }

        pub fn resetClock(self: *Self, ts: u64) void {
            self.clock.reset(ts);
        }

        pub fn appendCommittedWrites(self: *Self
            tx_id: TxId,
            rows: []const RowType,) !u64 {
                const commit_ts = self.nextTimestamp();
                try self.storage.appendLogRecord(. {
                    .tx_id = tx_id,
                    .commit_ts = commit_ts,
                    .rows = rows,
                });
                return commit_ts;
            }
    };
}
