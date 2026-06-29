package org.example.rowid;

import org.apache.iceberg.Snapshot;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.TableIdentifier;
import org.example.Executor;
import org.example.MyCatalog;

import java.io.IOException;

public class Example {
    public static void main(String[] args) throws IOException {
        MyCatalog myCatalog = MyCatalog.INSTANCE;

        // 1. 创建测试表 (已插入数据)
        TableIdentifier tableId = TableIdentifier.of("mydb", "rowid_test");
        Table table = myCatalog.createTableExample(tableId);

        // 2. 读取所有数据
        Executor.search(tableId);

        // =============== rowid 相关API ===================
        Snapshot currentSnapshot = table.currentSnapshot();

        // finish
        myCatalog.close();
    }
}
