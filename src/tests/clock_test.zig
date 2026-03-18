const std = @import("std");
const testing = std.testing;
const clock_mod = @import("../clock.zig");

test "LocalClock: starts at zero and increments monotonically" {
    var clock = clock_mod.LocalClock.init();

    try testing.expectEqual(@as(u64, 0), clock.getTimestamp());
    try testing.expectEqual(@as(u64, 1), clock.getTimestamp());
    try testing.expectEqual(@as(u64, 2), clock.getTimestamp());
    try testing.expectEqual(@as(u64, 3), clock.peek());
}

test "LocalClock: reset changes next returned timestamp" {
    var clock = clock_mod.LocalClock.init();

    _ = clock.getTimestamp();
    _ = clock.getTimestamp();
    clock.reset(42);

    try testing.expectEqual(@as(u64, 42), clock.getTimestamp());
    try testing.expectEqual(@as(u64, 43), clock.peek());
}

test "assertLogicalClock accepts a valid clock type" {
    const MockClock = struct {
        next: u64 = 0,

        pub fn getTimestamp(self: *@This()) u64 {
            const v = self.next;
            self.next += 1;
            return v;
        }

        pub fn reset(self: *@This(), ts: u64) void {
            self.next = ts;
        }
    };

    clock_mod.assertLogicalClock(MockClock);
}

test "LogicalClock: concurrent fetchAdd returns unique contiguous values" {
    var clock = clock_mod.LocalClock.init();

    const WorkerCtx = struct {
        clock: *clock_mod.LocalClock,
        out: []u64,

        fn run(self: *@This()) void {
            for (self.out) |*slot| {
                slot.* = self.clock.getTimestamp();
            }
        }
    };

    const thread_count = 8;
    const per_thread = 5_000;
    const total = thread_count * per_thread;

    var values: [total]u64 = undefined;
    var threads: [thread_count]std.Thread = undefined;
    var ctxs: [thread_count]WorkerCtx = undefined;

    var t: usize = 0;
    while (t < thread_count) : (t += 1) {
        const begin = t * per_thread;
        const end = begin + per_thread;
        ctxs[t] = .{
            .clock = &clock,
            .out = values[begin..end],
        };
        threads[t] = try std.Thread.spawn(.{}, WorkerCtx.run, .{&ctxs[t]});
    }

    for (threads) |th| th.join();

    std.mem.sort(u64, values[0..], {}, std.sort.asc(u64));

    const base = values[0];
    var i: usize = 0;
    while (i < total) : (i += 1) {
        try testing.expectEqual(base + @as(u64, @intCast(i)), values[i]);
    }
}
