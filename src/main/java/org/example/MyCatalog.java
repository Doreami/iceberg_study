package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
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
import java.util.List;

public class MyCatalog {
    public static final MyCatalog myCatalog;

    static {
        myCatalog = new MyCatalog();
    }

    public static Catalog getCatalog() {
        return myCatalog.getCatalog();
    }

    private String warehousePath;
    private Catalog catalog;

    public MyCatalog() {
        this.warehousePath = Const.WARE_HOUSE_PATH;
        Configuration hadoopConf = new Configuration();
        this.catalog = new HadoopCatalog(hadoopConf, warehousePath);
        System.out.println("Catalog 初始化成功，仓库路径：" + warehousePath);
    }

    public void printTableSnapshotInfo(Table table) {
        System.out.println("=========== 快照信息 ===========");
        Snapshot snapshot = table.currentSnapshot();
        System.out.println("\n当前快照 ID: " + snapshot.snapshotId());
        System.out.println("快照时间: " + snapshot.timestampMillis());
        System.out.println("快照操作: " + snapshot.operation());
    }

    public Table createTable(TableIdentifier tableId,
                             Schema schema, PartitionSpec partitionSpec) {
        Table table = catalog.createTable(tableId, schema, partitionSpec);
        System.out.println("=========== 表信息 ===========");
        System.out.println("新建表:");
        System.out.println("表名称：" + table.name());
        System.out.println("表位置：" + table.location());
        return table;
    }

    public Table createTableExample() throws IOException {
        TableIdentifier tableId = TableIdentifier.of("mydb", "user_table");
        if (catalog.tableExists(tableId)) {
            catalog.dropTable(tableId);
        }

        // 1. 新建表
        Schema schema = new Schema(
                Types.NestedField.required(1, "id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "score", Types.DoubleType.get())
        );

        PartitionSpec spec = PartitionSpec.unpartitioned();
        Table table = createTable(tableId, schema, spec);

        // 2. 准备 100 条测试数据
        List<Record> records = new ArrayList<>();
        for (long i = 1; i <= 100; i++) {
            GenericRecord record = GenericRecord.create(schema);
            record.setField(Const.ID_NAME, i);
            record.setField("name", "user_" + i);
            record.setField("score", Math.random() * 100);
            records.add(record);
        }

        // 3. 写parquet文件
        OutputFileFactory fileFactory = OutputFileFactory.builderFor(table, 1, 1)
                .format(FileFormat.PARQUET)
                .build();

        DataWriter<Record> writer = Parquet.writeData(fileFactory.newOutputFile())
                .schema(schema)
                .createWriterFunc(GenericParquetWriter::buildWriter)
                .withSpec(spec)
                .overwrite()
                .build();

        for (Record record : records) {
            writer.write(record);
        }
        writer.close();

        // 4. 提交数据文件到表（生成快照）
        table.newAppend().appendFile(writer.toDataFile()).commit();
        System.out.println("✅ 成功写入 " + records.size() + " 条记录到 Parquet 文件");
        return table;
    }

    public void close() throws IOException {
        ((HadoopCatalog) this.catalog).close();
        System.out.println("\n✅ 演示完成，可以在 " + warehousePath + " 目录下查看 Iceberg 文件结构");
    }
}
