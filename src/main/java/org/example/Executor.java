package org.example;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;

import java.io.IOException;
import java.util.List;

public class Executor {
    private static Catalog catalog = MyCatalog.INSTANCE.getCatalog();

    public static void searchById(TableIdentifier tableId, List<Long> ids) throws IOException {
        Table table = catalog.loadTable(tableId);

        try (CloseableIterable<Record> result = IcebergGenerics.read(table)
                .where(Expressions.in(Const.ID_NAME, ids))
                .build()) {
            for (Record rec : result) {
                System.out.println(rec.getField(Const.ID_NAME) + " -> " + rec.getField("score"));
            }
        }
    }

    public static void search(TableIdentifier tableId) throws IOException {
        Table table = catalog.loadTable(tableId);
        try (CloseableIterable<Record> result = IcebergGenerics.read(table)
                .build()) {
            for (Record rec : result) {
                System.out.println(rec.getField(Const.ID_NAME) + " -> " + rec.getField("score"));
            }
        }
    }
}
