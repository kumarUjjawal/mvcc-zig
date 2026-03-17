const std = @import("std");

pub fn assertLogicalClock(comptime T: type) void {
    comptime {
        if (!@hasDecl(T, "getTimestamp")) {
            @compileError(@typeName(T) ++ " must declare: getTimestamp(self: *T) u64");
        }

        if (!@hasDecl(T, "reset")) {
            @compileError(@typeName(T) ++ " must declare: reset(self, *T, ts: u64) void");
        }
    }
}

pub const LocalClock = struct {
    ts_sequence: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    pub fn init() LocalClock {
        return .{};
    }

    pub fn getTimeStamp(self: *LocalClock) u64 {
        return self.ts_sequence.fetchAdd(1, .seq_cst);
    }

    pub fn reset(self: *LocalClock, ts: u64) void {
        self.ts_sequence.store(ts, .seq_cst);
    }

    pub fn peek(self: *const LocalClock) u64 {
        return self.ts_sequence.load(.seq_cst);
    }
};
