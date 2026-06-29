package org.example.rowid;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.example.Executor;
import org.example.MyCatalog;

import java.io.IOException;
import java.util.List;

/**
 * Row Lineage (RowID) API 体验示例。
 *
 * Iceberg V3 (format-version=3) 表自动启用 Row Lineage：
 *   - 每行数据有全局唯一的 _row_id (Long 类型)
 *   - Snapshot.firstRowId() — 快照中第一个新分配 row-id
 *   - Snapshot.addedRows()  — 快照分配 row-id 的行数上界
 *   - DataFile.firstRowId() — 文件中第一行的 row-id
 *   - 查询时通过 MetadataColumns.schemaWithRowLineage() 读取 _row_id 列
 *   - 增量扫描通过 IncrementalAppendScan 实现 CDC 场景
 *
 * 参考: docs/rowid/Java Vs Rust-Rowid.md
 */
public class Example {
    public static void main(String[] args) throws IOException {
        MyCatalog myCatalog = MyCatalog.INSTANCE;
        try {
            run(myCatalog);
        } finally {
            myCatalog.close();
        }
    }

    private static void run(MyCatalog myCatalog) throws IOException {
        TableIdentifier tableId = TableIdentifier.of("mydb", "rowid_v3_test");

        // ══════════ 示例1: 创建 V3 表 (自动启用 Row Lineage) ══════════
        System.out.println("\n══════ 示例1: 创建 V3 表 ══════");
        Table table = myCatalog.createTableExampleV3(tableId);

        // ══════════ 示例2: 遍历所有快照, 查看 firstRowId ══════════
        System.out.println("\n══════ 示例2: 遍历所有快照的 firstRowId ══════");
        System.out.println("关键 API: table.snapshots() → snapshot.firstRowId()");
        for (Snapshot snap : table.snapshots()) {
            System.out.printf("  快照 ID=%d, 操作=%s, firstRowId=%s, addedRows=%s%n",
                    snap.snapshotId(), snap.operation(),
                    snap.firstRowId(), snap.addedRows());
        }

        // ══════════ 示例3: 当前快照的 firstRowId ══════════
        System.out.println("\n══════ 示例3: 当前快照的 firstRowId ══════");
        System.out.println("关键 API: table.currentSnapshot() → snapshot.firstRowId()");
        Snapshot snap = table.currentSnapshot();
        System.out.printf("  快照 ID=%d, firstRowId=%s, addedRows=%s%n",
                snap.snapshotId(), snap.firstRowId(), snap.addedRows());
        System.out.println("  → 此快照分配的 row-id 范围: ["
                + snap.firstRowId() + ", "
                + (snap.firstRowId() + snap.addedRows() - 1) + "]");

        // ══════════ 示例4: 数据文件级 firstRowId ══════════
        System.out.println("\n══════ 示例4: 数据文件级 firstRowId ══════");
        System.out.println("关键 API: DataFile.firstRowId()");
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile df = task.file();
                System.out.printf("  文件: %s%n", fileName(df.path().toString()));
                System.out.printf("    记录数=%d, firstRowId=%s, fileSize=%d bytes%n",
                        df.recordCount(), df.firstRowId(), df.fileSizeInBytes());
            }
        }

        // ══════════ 示例5: 查询 + _row_id 元数据列 ══════════
        System.out.println("\n══════ 示例5: 读取数据 + _row_id 元数据列 ══════");
        System.out.println("关键 API: MetadataColumns.schemaWithRowLineage()");
        Executor.searchWithRowLineage(tableId);

        // ══════════ 示例6: 精确查询 + _row_id ══════════
        System.out.println("\n══════ 示例6: 精确查找 + _row_id ══════");
        Executor.searchByIdWithRowLineage(tableId, List.of(1L, 50L, 100L));

        // ══════════ 示例7: 增量扫描 (两个快照之间) ══════════
        System.out.println("\n══════ 示例7: 增量扫描 (IncrementalAppendScan) ══════");
        long snap1Id = table.currentSnapshot().snapshotId();
        System.out.println("快照1 ID: " + snap1Id);

        // 追加数据, 创造新快照
        table = myCatalog.appendData(tableId, 200, 10);
        long snap2Id = table.currentSnapshot().snapshotId();
        System.out.println("快照2 ID: " + snap2Id);

        // 关键 API: table.newIncrementalAppendScan()
        //           .fromSnapshotExclusive(snap1) — 不包含 snap1
        //           .toSnapshot(snap2)            — 包含 snap2
        System.out.println("\n增量扫描 snap1 → snap2 (只含新增数据):");
        IncrementalAppendScan incScan = table.newIncrementalAppendScan()
                .fromSnapshotExclusive(snap1Id)
                .toSnapshot(snap2Id);

        try (CloseableIterable<FileScanTask> tasks = incScan.planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile df = task.file();
                System.out.printf("  新增文件: %s (记录数=%d, firstRowId=%s)%n",
                        fileName(df.path().toString()), df.recordCount(), df.firstRowId());
            }
        }

        // ══════════ 示例8: CDC 持续增量扫描 ══════════
        System.out.println("\n══════ 示例8: CDC 增量扫描 (fromSnapshotExclusive) ══════");
        System.out.println("关键 API: .fromSnapshotExclusive(lastCheckpointSnapshotId)");
        System.out.println("场景: CDC 管道, 从上次检查点之后读取所有新增数据");

        long checkpointId = snap1Id;
        IncrementalAppendScan cdcScan = table.newIncrementalAppendScan()
                .fromSnapshotExclusive(checkpointId);

        System.out.println("从快照 " + checkpointId + " 之后的新增文件:");
        try (CloseableIterable<FileScanTask> tasks = cdcScan.planFiles()) {
            for (FileScanTask task : tasks) {
                System.out.printf("  %s → 记录数=%d, firstRowId=%s%n",
                        fileName(task.file().path().toString()),
                        task.file().recordCount(),
                        task.file().firstRowId());
            }
        }
        System.out.println("(实际 CDC 场景中会循环轮询并更新检查点)");

        // ══════════ 示例9: 追加后查看 row-id 分布 ══════════
        System.out.println("\n══════ 示例9: 追加后所有文件的 row-id 分布 ══════");
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile df = task.file();
                System.out.printf("  %s → 记录数=%d, firstRowId=%s%n",
                        fileName(df.path().toString()), df.recordCount(), df.firstRowId());
            }
        }
        System.out.println("→ row-id 跨快照全局递增: 新文件 firstRowId = 上一快照 firstRowId + addedRows");

        // ══════════ 示例10: Changelog 变更扫描 ══════════
        System.out.println("\n══════ 示例10: Changelog 变更扫描 ══════");
        System.out.println("关键 API: table.newIncrementalChangelogScan()");
        System.out.println("场景: 追踪 INSERT/UPDATE/DELETE 操作");

        long changelogStartId = table.currentSnapshot().snapshotId();
        // 追加 + 覆盖 = 产生 INSERT + DELETE 变更
        table = myCatalog.appendData(tableId, 300, 5);
        table = myCatalog.overwriteData(tableId, 300, 5);
        long changelogEndId = table.currentSnapshot().snapshotId();
        System.out.println("变更范围: snapshot " + changelogStartId + " → " + changelogEndId);

        IncrementalChangelogScan changelogScan = table.newIncrementalChangelogScan()
                .fromSnapshotExclusive(changelogStartId)
                .toSnapshot(changelogEndId);

        try (CloseableIterable<ChangelogScanTask> tasks = changelogScan.planFiles()) {
            for (ChangelogScanTask task : tasks) {
                System.out.printf("  操作=%s, 序号=%d, 提交快照=%d%n",
                        task.operation(), task.changeOrdinal(), task.commitSnapshotId());
            }
        }

        // ══════════ 示例11: 快照祖先链追溯 ══════════
        System.out.println("\n══════ 示例11: 快照祖先链追溯 ══════");
        System.out.println("关键 API: snapshot.parentId() 递归向前");
        System.out.println("场景: 沿快照链追溯 Row Lineage 历史");

        Long id = table.currentSnapshot().snapshotId();
        int depth = 0;
        while (id != null && depth < 10) {
            Snapshot s = table.snapshot(id);
            System.out.printf("  层%d: snapshot=%d, firstRowId=%s, addedRows=%s, 操作=%s%n",
                    depth, s.snapshotId(), s.firstRowId(), s.addedRows(), s.operation());
            id = s.parentId();
            depth++;
        }

        // ══════════ 示例12: Overwrite 与 Row ID 重新分配 ══════════
        System.out.println("\n══════ 示例12: Overwrite 与 Row ID 重新分配 ══════");
        System.out.println("关键 API: table.newOverwrite().overwriteByRowFilter()");
        System.out.println("注意: overwriteByRowFilter 要求 filter 匹配文件中的所有行");
        System.out.println("场景: 覆盖整个数据文件时 _row_id 会被重新分配");

        // 先记录覆盖前的 row-id
        System.out.println("覆盖前 snapshot: " + table.currentSnapshot().snapshotId()
                + ", firstRowId=" + table.currentSnapshot().firstRowId()
                + ", addedRows=" + table.currentSnapshot().addedRows());

        // 覆盖整个原始文件 (id 1~100), filter 覆盖全部 100 行
        long beforeOverwriteSnapId = table.currentSnapshot().snapshotId();
        table = myCatalog.overwriteData(tableId, 1, 100);

        System.out.println("覆盖后 snapshot: " + table.currentSnapshot().snapshotId()
                + ", firstRowId=" + table.currentSnapshot().firstRowId()
                + ", addedRows=" + table.currentSnapshot().addedRows());

        // 增量扫描查看覆盖产生的文件
        IncrementalAppendScan overwriteScan = table.newIncrementalAppendScan()
                .fromSnapshotExclusive(beforeOverwriteSnapId)
                .toSnapshot(table.currentSnapshot().snapshotId());
        try (CloseableIterable<FileScanTask> tasks = overwriteScan.planFiles()) {
            for (FileScanTask task : tasks) {
                DataFile df = task.file();
                System.out.printf("  新增文件: %s, 记录数=%d, firstRowId=%s%n",
                        fileName(df.path().toString()), df.recordCount(), df.firstRowId());
            }
        }
        // 读取被覆盖 id 的新 _row_id
        System.out.println("覆盖后 id=1,2,100 的新 _row_id:");
        Executor.searchByIdWithRowLineage(tableId, List.of(1L, 2L, 100L));

        // ══════════ 示例13: _last_updated_sequence_number 递增 ══════════
        System.out.println("\n══════ 示例13: _last_updated_sequence_number 递增 ══════");
        System.out.println("关键 API: MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER");
        System.out.println("场景: 覆盖后该行的 _last_updated_sequence_number 更新为新快照的 sequence number");

        // id=1 刚刚被 Example12 overwrite 覆盖, sequence number 应该是新快照的
        System.out.println("\n被覆盖的数据 (id=1, 刚刚 overwrite):");
        Executor.searchByIdWithRowLineage(tableId, List.of(1L));
        // id=200 在 Example7 中追加, 没有被覆盖, sequence number 不变
        System.out.println("\n未覆盖的追加数据 (id=200, 仅 append):");
        Executor.searchByIdWithRowLineage(tableId, List.of(200L));
        System.out.println("→ 覆盖后 _last_updated_sequence_number 递增到当前快照的 sequence number");
    }

    private static String fileName(String path) {
        int idx = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        return idx >= 0 ? path.substring(idx + 1) : path;
    }
}
