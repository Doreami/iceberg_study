package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.FileFormat;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
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
    private Catalog catalog;

    public MyCatalog() {
        Configuration hadoopConf = new Configuration();
        catalog = new HadoopCatalog(hadoopConf, Const.WARE_HOUSE_PATH);
    }

    public Table createTable(TableIdentifier tableId,
                             Schema schema, PartitionSpec partitionSpec) {
        Table table = catalog.createTable(tableId, schema, partitionSpec);
        System.out.println("新建表:");
        System.out.println("表名称：" + table.name());
        System.out.println("表位置：" + table.location());
        return table;
    }

    public Table createTableExample() throws IOException {
        // 1. 新建表
        Schema schema = new Schema(
                Types.NestedField.required(1, "user_id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "score", Types.DoubleType.get())
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("user_id")
                .build();

        TableIdentifier tableId = TableIdentifier.of("mydb", "user_table");
        Table table = createTable(tableId, schema, spec);

        // 2. 主播数据
        List<Record> records = new ArrayList<>();
        for (long i = 1; i <= 100; i++) {
            GenericRecord record = GenericRecord.create(schema);
            record.setField("user_id", i);
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
                .overwrite()
                .build();
        for (Record record : records) {
            writer.write(record);
        }
        writer.close();
        table.newAppend().appendFile(writer.toDataFile()).commit();

        return table;
    }

    public void close() throws IOException {
        ((HadoopCatalog)this.catalog).close();
    }
}
