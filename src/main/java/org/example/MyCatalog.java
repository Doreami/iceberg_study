package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

    public void close() throws IOException {
        ((HadoopCatalog) this.catalog).close();
        System.out.println("\n✅ 演示完成，仓库路径: " + warehousePath);
    }

    public Catalog getCatalog() {
        return catalog;
    }
}
