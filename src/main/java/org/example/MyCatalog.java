package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class MyCatalog {
    public static final MyCatalog INSTANCE = new MyCatalog();

    private static final String ID_COL = Const.ID_NAME;
    private static final Schema DEMO_SCHEMA = new Schema(
            Types.NestedField.required(1, ID_COL, Types.LongType.get()),
            Types.NestedField.required(2, "name", Types.StringType.get()),
            Types.NestedField.required(3, "score", Types.DoubleType.get())
    );

    private final String warehousePath;
    private final Catalog catalog;

    public MyCatalog() {
        this.warehousePath = Const.WARE_HOUSE_PATH;
        Configuration hadoopConf = new Configuration();
        this.catalog = new HadoopCatalog(hadoopConf, warehousePath);
        System.out.println("Catalog 初始化成功，仓库路径：" + warehousePath);
    }

    // ──────────── Table CRUD ────────────

    public Table createTable(TableIdentifier tableId, Schema schema, PartitionSpec spec) {
        return createTable(tableId, schema, spec, new HashMap<>());
    }

    public Table createTable(TableIdentifier tableId, Schema schema, PartitionSpec spec,
                             Map<String, String> properties) {
        dropTableIfExists(tableId);
        Table table = catalog.createTable(tableId, schema, spec, properties);
        System.out.println("=========== 表信息 ===========");
        System.out.println("新建表: " + table.name());
        System.out.println("表位置: " + table.location());
        return table;
    }

    private void dropTableIfExists(TableIdentifier tableId) {
        if (catalog.tableExists(tableId)) {
            System.out.println("有同名表, 将旧表删除");
            catalog.dropTable(tableId);
        }
    }

    // ──────────── Data write helpers ────────────

    /** 生成 DEMO_SCHEMA 的测试记录 */
    private List<Record> generateRecords(long startId, long count) {
        List<Record> records = new ArrayList<>();
        for (long i = startId; i < startId + count; i++) {
            GenericRecord record = GenericRecord.create(DEMO_SCHEMA);
            record.setField(ID_COL, i);
            record.setField("name", "user_" + i);
            record.setField("score", Math.random() * 100);
            records.add(record);
        }
        return records;
    }

    /** 将 Record 列表写入一个 Parquet 文件并追加到表, 返回提交后的 DataFile */
    private DataFile writeAndAppend(Table table, List<Record> records, long commitId)
            throws IOException {
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, commitId)
                .format(FileFormat.PARQUET)
                .build();

        DataWriter<Record> writer = Parquet.writeData(fileFactory.newOutputFile())
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(table.spec())
                .overwrite()
                .build();

        for (Record record : records) {
            writer.write(record);
        }
        writer.close();

        DataFile dataFile = writer.toDataFile();
        table.newAppend().appendFile(dataFile).commit();
        return dataFile;
    }

    // ──────────── Demo table factories ────────────

    /** 创建 V2 表并写入 100 条测试数据 */
    public Table createTableExample(TableIdentifier tableId) throws IOException {
        return createTableExampleV3(tableId);
    }

    /** 创建 V3 表 (自动启用 Row Lineage) 并写入 100 条测试数据 */
    public Table createTableExampleV3(TableIdentifier tableId) throws IOException {
        dropTableIfExists(tableId);

        // 使用 buildTable() API, 确保 FORMAT_VERSION 等 reserved 属性生效
        Table table = catalog.buildTable(tableId, DEMO_SCHEMA)
                .withPartitionSpec(PartitionSpec.unpartitioned())
                .withProperty(TableProperties.FORMAT_VERSION, "3")
                .create();
        System.out.println("=========== 表信息 ===========");
        System.out.println("新建 V3 表: " + table.name() + ", 位置: " + table.location());

        List<Record> records = generateRecords(1, 100);
        writeAndAppend(table, records, 1);
        System.out.println("✅ V3 表写入 " + records.size() + " 条记录 (row lineage 已启用)");
        return table;
    }

    /** 创建空的 V3 表（不写数据），用于需要精细控制文件结构的测试 */
    public Table createEmptyV3Table(TableIdentifier tableId) {
        dropTableIfExists(tableId);
        Table table = catalog.buildTable(tableId, DEMO_SCHEMA)
                .withPartitionSpec(PartitionSpec.unpartitioned())
                .withProperty(TableProperties.FORMAT_VERSION, "3")
                .create();
        System.out.println("新建空 V3 表: " + table.name() + ", 位置: " + table.location());
        return table;
    }

    /** 向已有表追加数据, 演示 row-id 递增 */
    public Table appendData(TableIdentifier tableId, long startId, long count) throws IOException {
        Table table = catalog.loadTable(tableId);
        List<Record> records = generateRecords(startId, count);
        writeAndAppend(table, records, System.currentTimeMillis());
        System.out.println("✅ 追加 " + records.size() + " 条记录 (id " + startId + "~"
                + (startId + count - 1) + ")");
        return catalog.loadTable(tableId);
    }

    /** 覆盖表中 id 在 [startId, startId+count) 范围的数据, 演示 _row_id 是否会重新分配 */
    public Table overwriteData(TableIdentifier tableId, long startId, long count) throws IOException {
        Table table = catalog.loadTable(tableId);

        // 准备覆盖数据 (score 设为 -1 标记为覆盖数据)
        List<Record> records = generateRecords(startId, count);

        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .format(FileFormat.PARQUET)
                .build();

        DataWriter<Record> writer = Parquet.writeData(fileFactory.newOutputFile())
                .schema(table.schema())
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(table.spec())
                .overwrite()
                .build();

        for (Record record : records) {
            writer.write(record);
        }
        writer.close();

        DataFile newFile = writer.toDataFile();
        // overwriteByRowFilter: 删除匹配行, 替换为新数据文件
        // validateFromSnapshot 避免快照间的冲突
        table.newOverwrite()
                .overwriteByRowFilter(Expressions.and(
                        Expressions.greaterThanOrEqual(Const.ID_NAME, startId),
                        Expressions.lessThan(Const.ID_NAME, startId + count)))
                .addFile(newFile)
                .validateFromSnapshot(table.currentSnapshot().snapshotId())
                .commit();
        System.out.println("✅ 覆盖 id " + startId + "~" + (startId + count - 1)
                + " (新 firstRowId=" + newFile.firstRowId() + ")");
        return catalog.loadTable(tableId);
    }

    // ──────────── Info ────────────

    public void printTableSnapshotInfo(Table table) {
        System.out.println("=========== 快照信息 ===========");
        Snapshot snapshot = table.currentSnapshot();
        System.out.println("当前快照 ID: " + snapshot.snapshotId());
        System.out.println("快照时间: " + snapshot.timestampMillis());
        System.out.println("快照操作: " + snapshot.operation());
        System.out.println("firstRowId: " + snapshot.firstRowId());
        System.out.println("addedRows:  " + snapshot.addedRows());
    }

    // ──────────── RowID 验证辅助方法 ────────────

    /** 获取当前快照所有 Parquet 数据文件的路径列表 */
    public List<String> getParquetFilePaths(Table table) throws IOException {
        List<String> paths = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                paths.add(task.file().path().toString());
            }
        }
        return paths;
    }

    /**
     * 获取当前 next-row-id (从 TableMetadata 直接读取)。
     * <p>
     * next-row-id 是 metadata.json 中的字段，SDK 提供了访问路径:
     * {@code HasTableOperations → TableOperations.current() → TableMetadata.nextRowId()}
     */
    public long getNextRowId(Table table) {
        if (table instanceof HasTableOperations htOps) {
            TableMetadata metadata = htOps.operations().current();
            return metadata.nextRowId();
        }
        // Fallback: 从快照推断
        Snapshot snap = table.currentSnapshot();
        if (snap == null) return 0;
        Long first = snap.firstRowId();
        Long added = snap.addedRows();
        if (first != null && added != null) {
            return first + added;
        }
        return -1;
    }

    /**
     * 使用 table.newRewrite() 执行文件级重写。
     * 读取旧文件数据（含 _row_id），写入新文件，然后原子替换。
     * 用于验证 Compaction 后 _row_id 是否保留。
     */
    public Table rewriteDataFiles(TableIdentifier tid, Table table) throws IOException {
        Set<DataFile> oldFiles = new HashSet<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                oldFiles.add(task.file());
            }
        }

        if (oldFiles.isEmpty()) {
            System.out.println("  无文件可重写");
            return table;
        }

        // 一次性读取所有数据（含 _row_id 和 _last_updated_sequence_number）
        Schema schemaWithLineage = MetadataColumns.schemaWithRowLineage(table.schema());
        List<Record> allRows = new ArrayList<>();
        try (CloseableIterable<Record> iter = IcebergGenerics.read(table)
                .project(schemaWithLineage)
                .build()) {
            for (Record rec : iter) {
                allRows.add(rec);
            }
        }
        System.out.println("  读取 " + allRows.size() + " 行 (含 _row_id)");

        // 写入一个新文件（含 _row_id 物理列）
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, System.currentTimeMillis())
                .format(FileFormat.PARQUET)
                .build();

        DataWriter<Record> writer = Parquet.writeData(fileFactory.newOutputFile())
                .schema(schemaWithLineage)
                .createWriterFunc(GenericParquetWriter::create)
                .withSpec(table.spec())
                .overwrite()
                .build();

        for (Record row : allRows) {
            writer.write(row);
        }
        writer.close();
        DataFile newFile = writer.toDataFile();
        Set<DataFile> newFiles = Set.of(newFile);

        System.out.println("  写入新文件: " + fileName(newFile.path().toString())
                + " (记录数=" + newFile.recordCount() + ", firstRowId=" + newFile.firstRowId() + ")");

        // 原子替换: 删除所有旧文件，添加新文件
        table.newRewrite()
                .rewriteFiles(oldFiles, newFiles)
                .commit();
        System.out.println("✅ Rewrite 完成: " + oldFiles.size() + " 个旧文件 → 1 个新文件");
        return catalog.loadTable(tid);
    }

    private static String fileName(String path) {
        int idx = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        return idx >= 0 ? path.substring(idx + 1) : path;
    }

    public void close() throws IOException {
        ((HadoopCatalog) this.catalog).close();
        System.out.println("\n✅ 演示完成，仓库路径: " + warehousePath);
    }

    public Catalog getCatalog() {
        return catalog;
    }
}
