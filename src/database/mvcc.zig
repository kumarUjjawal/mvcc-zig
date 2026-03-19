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
            .committed => |ts| 0x8000_0000_0000 | ts,
        };
    }

    pub fn decode(v: u64) TransactionState {
        return switch (v) {
            0 => .{ .active = {} },
            1 => .{ .preparing = {}},
            2 => .{ .aborted = {}},
            3 => .{ terminated = {}},
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
