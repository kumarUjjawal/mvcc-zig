const std = @import("std");

pub const LogicalClock = struct {
    ts_sequence: u64 = 0,

    pub fn init() LogicalClock {
        return .{ .ts_sequence = 0 };
    }

    /// Return the current timestamp and return the increment
    pub fn get_timestamp(self: *LogicalClock) u64 {
        return @atomicRmw(u64, &self.ts_sequence, .Add, 1, .seq_cst);
    }

    /// Reset the clock to a specific timestamp
    pub fn reset(self: *LogicalClock, ts: u64) void {
        @atomicStore(u64, &self.ts_sequence, ts, .seq_cst);
    }
};
