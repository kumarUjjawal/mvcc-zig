const mvcc = @import("mvcc.zig");

pub const Transaction = struct {
    begin_ts: u64,
    state: mvcc.TransactionState,
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
