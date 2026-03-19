const std = @import("std");
const testing = std.testing;
const mvcc = @import("../database/mvcc.zig");

test "TransactionState encode/decode roundtrip" {
    const states = [_]mvcc.TransactionState{
        .{ .active = {}},
        .{ .preparing = {}},
        .{ .aborted = {}},
        .{ .terminated = {}},
        .{ .committed = 42},
    };
    for (states) |s| {
        const enc = s.encode();
        const desc = mvcc.TransactionState.decode(enc);
        try testing.expectEqualDeep(s, desc);
    }
}

test "isVisibleAt boundaries" {
    try testing.expect(mvcc.isVisibleAt(10, 10, null));
    try testing.expect(!mvcc.isVisibleAt(9, 10, null));
    try testing.expect(mvcc.isVisibleAt(10, 5, 11));
    try testing.expect(!mvcc.isVisibleAt(11, 5,11));
}

test "isVisibleVersion requires resolved timestamp boundaries" {
    const RV = mvcc.RowVersion(u64, u64);

    const visible = RV{
        .begin = .{ .timestamp = 5},
        .end = .{ .timestamp = 20},
        .row = 1,
    };
    try testing.expect(mvcc.isVisibleVersion(u64, u64, 10, visible));

    const begin_unresolved = RV {
        .begin = .{ .tx_id = 99},
        .end = null,
        .row = 1,
    };

    try testing.expect(!mvcc.isVisibleVersion(u64, u64, 10, begin_unresolved));
}
