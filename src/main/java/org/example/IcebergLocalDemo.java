package org.example;

import org.apache.iceberg.*;
import org.apache.iceberg.catalog.TableIdentifier;

import java.io.IOException;

public class IcebergLocalDemo {
    public static void main(String[] args) throws IOException {
        MyCatalog myCatalog = MyCatalog.INSTANCE;
        TableIdentifier tableId = TableIdentifier.of("mydb", "user_table");

        // 创建 V3 表并写入测试数据
        Table table = myCatalog.createTableExample(tableId);

        // 打印快照信息 (含 row-id)
        myCatalog.printTableSnapshotInfo(table);

        // 读取所有数据
        System.out.println("\n=========== 全部数据 ===========");
        Executor.search(tableId);

        // 读取数据 + _row_id 元数据列
        System.out.println("\n=========== 数据 + _row_id ===========");
        Executor.searchWithRowLineage(tableId);

        // 创建索引 (V3 + Puffin)
        IndexCommander.createIndex(tableId);

        myCatalog.close();
    }
}
