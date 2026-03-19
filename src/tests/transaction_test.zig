const std = @import("std");
const testing = std.testing;
const mvcc = @import("../database/mvcc.zig");
const tx = @import("../database/transaction.zig");

test "transaction state transitions" {
    try testing.expect(tx.canTransition(.{ .active = {} }, .{ .preparing = {} }));
    try testing.expect(tx.canTransition(.{ .preparing = {} }, .{ .committed = 10 }));
    try testing.expect(tx.canTransition(.{ .aborted = {} }, .{ .terminated = {} }));
    try testing.expect(!tx.canTransition(.{ .terminated = {} }, .{ .active = {} }));
    try testing.expect(!tx.canTransition(.{ .committed = 1 }, .{ .active = {} }));

    _ = mvcc;
}
