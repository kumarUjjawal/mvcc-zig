comptime {
    _ = @import("tests/clock_test.zig");
    _ = @import("tests/storage_test.zig");
    _ = @import("tests/database_test.zig");
    _ = @import("tests/mvcc_test.zig");
}
