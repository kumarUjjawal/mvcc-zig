const std = @import("std");
const mvcc = @import("mvcc.zig");

pub const Transaction = struct {
    begin_ts: u64,
    state: mvcc.TransactionState,
    read_set: std.AutoHashMap(u64, void),
    write_set: std.AutoHashMap(u64, void),

    pub fn init(allocator: std.mem.Allocator, begin_ts: u64) !Transaction {
        return .{
            .begin_ts = begin_ts,
            .state = .{ .active = {} },
            .read_set = std.AutoHashMap(u64, void).init(allocator),
            .write_set = std.AutoHashMap(u64, void).init(allocator),
        };
    }

    pub fn deinit(self: *Transaction) void {
        self.read_set.deinit();
        self.write_set.deinit();
    }

    pub fn markRead(self: *Transaction, row_id: u64) !void {
        _ = try self.read_set.getOrPut(row_id);
    }

    pub fn markWrite(self: *Transaction, row_id: u64) !void {
        _ = try self.write_set.getOrPut(row_id);
    }
};

pub fn canTransition(from: mvcc.TransactionState, to: mvcc.TransactionState) bool {
    return switch (from) {
        .active => switch (to) {
            .preparing, .aborted => true,
            else => false,
        },
        .preparing => switch (to) {
            .committed, .aborted => true,
            else => false,
        },
        .aborted => switch (to) {
            .terminated => true,
            else => false,
        },
        .committed => switch (to) {
            .terminated => true,
            else => false,
        },
        .terminated => false,
    };
}
