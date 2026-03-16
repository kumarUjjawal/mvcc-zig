const std = @import("std");
const mvcc_zig = @import("mvcc_zig");

pub fn main() !void {
    // Prints to stderr, ignoring potential errors.
    std.debug.print("All your {s} are belong to us.\n", .{"codebase"});
    try mvcc_zig.bufferedPrint();
}
