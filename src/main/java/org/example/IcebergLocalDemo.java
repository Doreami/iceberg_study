package org.example;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.IOException;

public class IcebergLocalDemo {
    public static void main(String[] args) throws IOException {
        MyCatalog myCatalog = MyCatalog.INSTANCE;
        TableIdentifier tableId = TableIdentifier.of("mydb", "user_table");
        Table table = myCatalog.createTableExample(tableId);

        // 打印快照信息
        myCatalog.printTableSnapshotInfo(table);

        // 读取所有数据
        Executor.search(tableId);

        // 创建索引
        IndexCommander.createIndex(tableId);

        myCatalog.close();
    }
}