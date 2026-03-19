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
