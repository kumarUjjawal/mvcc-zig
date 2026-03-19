const std = @import("std");

pub const TxId = u64;
pub const Timestamp = u64;

pub fn TxTimestampOrId(comptime TxIdType: type) type {
    return union(enum) {
        timestamp: Timestamp,
        tx_id: TxIdType,
    };
}

pub const TransactionStateTag = enum { active, preparing, aborted, terminated, committed};

pub const TransactionState = union(TransactionStateTag) {
    active: void,
    preparing: void,
    aborted: void,
    terminated: void,
    committed: Timestamp,

    pub fn encode(self: TransactionState) u64 {
        return switch(self) {
            .active => 0,
            .preparing => 1,
            .aborted => 2,
            .terminated => 3,
            .committed => |ts| blk: {
                // Highest bit is reserved as the "committed timestamp" tag.
                std.debug.assert((ts & 0x8000_0000_0000_0000) == 0);
                break :blk 0x8000_0000_0000_0000 | ts;
            },
        };
    }

    pub fn decode(v: u64) TransactionState {
        return switch (v) {
            0 => .{ .active = {} },
            1 => .{ .preparing = {}},
            2 => .{ .aborted = {}},
            3 => .{ .terminated = {}},
            else => if ((v & 0x8000_0000_0000_0000) != 0)
                .{ .committed = (v & 0x7fff_ffff_ffff_ffff)}
                else
                    unreachable,
        };
    }
};

pub fn RowVersion(comptime RowType: type, comptime TxIdType: type) type {
    return struct {
        begin: TxTimestampOrId(TxIdType),
        end: ?TxTimestampOrId(TxIdType),
        row: RowType,
    };
}

pub fn isVisibleAt(
    tx_begin_ts: Timestamp,
    rv_begin_ts: Timestamp,
    rv_end_ts: ?Timestamp,
) bool {
    if (tx_begin_ts < rv_begin_ts) return false;
    if (rv_end_ts) |end_ts| return tx_begin_ts < end_ts;
    return true;
}

pub fn isVisibleVersion(
    comptime RowType: type,
    comptime TxIdType: type,
    tx_begin_ts: Timestamp,
    rv: RowVersion(RowType, TxIdType),
) bool {
    const begin_ts = switch (rv.begin) {
        .timestamp => |ts| ts,
        .tx_id => return false,
    };

    const end_ts: ?Timestamp = if (rv.end) |e|
        switch (e) {
            .timestamp => |ts| ts,
            .tx_id => return false,
        }
    else
        null;

    return isVisibleAt(tx_begin_ts, begin_ts, end_ts);
}
