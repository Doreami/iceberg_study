package org.example;

import org.apache.iceberg.*;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;

public class IcebergLocalDemo {
    public static void main(String[] args) throws IOException {
        MyCatalog myCatalog = new MyCatalog();
        Table table = myCatalog.createTableExample();

        // 打印快照信息
        myCatalog.printTableSnapshotInfo(table);

        // 读取过滤
        try (CloseableIterable<Record> result = IcebergGenerics.read(table)
                .where(Expressions.greaterThan("score", 85.0))
                .build()) {
            for (Record rec : result) {
                System.out.println(rec.getField("user_id") + " -> " + rec.getField("score"));
            }
        }

        myCatalog.close();
    }
}