package org.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.GenericRecord;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.data.parquet.GenericParquetWriter;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.hadoop.HadoopCatalog;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.DataWriter;
import org.apache.iceberg.io.OutputFileFactory;
import org.apache.iceberg.parquet.Parquet;
import org.apache.iceberg.types.Types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class IcebergLocalDemo {
    public static void main(String[] args) throws IOException {
        Configuration hadoopConf = new Configuration();
        // Windows 下请改为 "file:///D:/iceberg_learn" 等带盘符的路径
        String warehousePath = "/tmp/iceberg_learn";
        HadoopCatalog catalog = new HadoopCatalog(hadoopConf, warehousePath);

        Schema schema = new Schema(
                Types.NestedField.required(1, "user_id", Types.LongType.get()),
                Types.NestedField.required(2, "name", Types.StringType.get()),
                Types.NestedField.required(3, "score", Types.DoubleType.get())
        );

        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .identity("user_id")
                .build();

        TableIdentifier tableId = TableIdentifier.of("mydb", "user_table");
        Table table = catalog.createTable(tableId, schema, spec);
        System.out.println("表位置：" + table.location());

        // 准备数据
        List<Record> records = new ArrayList<>();
        for (long i = 1; i <= 100; i++) {
            GenericRecord record = GenericRecord.create(schema);
            record.setField("user_id", i);
            record.setField("name", "user_" + i);
            record.setField("score", Math.random() * 100);
            records.add(record);
        }

        // 写入 Parquet
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

        // 读取过滤
        try (CloseableIterable<Record> result = IcebergGenerics.read(table)
                .where(Expressions.greaterThan("score", 85.0))
                .build()) {
            for (Record rec : result) {
                System.out.println(rec.getField("user_id") + " -> " + rec.getField("score"));
            }
        }

        catalog.close();
    }
}