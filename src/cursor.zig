const std = @import("std");
const db_mod = @import("database/database.zig");

pub const ScanCursor(comptime RowType: type, comptime ClockType: type, comptime StorageType: type) type {
    const DB = db_mod.Database(RowType, ClockType, StorageType);

    return struct {
        const self = @This();

        allocator: std.mem.Allocator,
        db: *const DB,
        tx_id: db_mod.TxId,
        row_ids: []db_mod.RowId,
        pos: usize = 0,

        pub fn init(
            allocator: std.mem.Allocator,
            db: *const DB,
            tx_id: db_mod.TxId
        ) !Self {
            const row_ids = try db.scanRowIds(allocator, tx_id);
            return .{
                .allocator = allocator,
                .db = db,
                .tx_id = tx_id,
                .row_ids = row_ids,
                .pos = 0,
            };
        }

        pub fn deinit(self: *Self) void {
            self.allocator.free(self.row_ids);
            self.row_ids = &.{};
            self.pos = 0;
        }

        pub fn close(self: *Self) void {
            self.deinit();
        }

        pub fn currentRowId(self: *const Self) !?RowType {
            const row_id = self.currentRowId() orelse return null;
            return try self.db.read(self.tx_id, row_id);
        }

        pub fn forward(self: *Self) bool {
            if (self.pos + 1 >= self.row_ids.len) return false;
            self.pos += 1;
            return true;
        }

        pub fn isEmpty(self: *const Self) bool {
            return self.row_ids.len == 0;
        }
    };
}
