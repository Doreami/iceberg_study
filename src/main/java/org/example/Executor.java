package org.example;

import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.IcebergGenerics;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.expressions.Expressions;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.MetadataColumns;
import org.apache.iceberg.Schema;

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

    // TODO 索引扫描

    /**
     * 读取数据并同时获取 _row_id 元数据列（仅 V3 表支持）。
     * 使用 MetadataColumns.schemaWithRowLineage() 将 _row_id 追加到 schema 中。
     */
    public static void searchWithRowLineage(TableIdentifier tableId) throws IOException {
        Table table = catalog.loadTable(tableId);

        // 将 _row_id 和 _last_updated_sequence_number 加入 projection
        Schema schemaWithLineage = MetadataColumns.schemaWithRowLineage(table.schema());

        try (CloseableIterable<Record> result = IcebergGenerics.read(table)
                .project(schemaWithLineage)
                .build()) {
            int count = 0;
            for (Record rec : result) {
                Object rowId = rec.getField(MetadataColumns.ROW_ID.name());
                Object lastSeq = rec.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
                System.out.printf("id=%s, name=%s, score=%.2f, %s=%s, %s=%s%n",
                        rec.getField("id"), rec.getField("name"), rec.getField("score"),
                        MetadataColumns.ROW_ID.name(), rowId,
                        MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), lastSeq);
                if (++count >= 10) {
                    System.out.println("  ... (仅显示前 10 条)");
                    break;
                }
            }
        }
    }

    /**
     * 精确查找 id, 同时输出 _row_id 和 _last_updated_sequence_number 元数据列。
     */
    public static void searchByIdWithRowLineage(TableIdentifier tableId, List<Long> ids) throws IOException {
        Table table = catalog.loadTable(tableId);
        Schema schemaWithLineage = MetadataColumns.schemaWithRowLineage(table.schema());

        try (CloseableIterable<Record> result = IcebergGenerics.read(table)
                .where(Expressions.in(Const.ID_NAME, ids))
                .project(schemaWithLineage)
                .build()) {
            for (Record rec : result) {
                Object rowId = rec.getField(MetadataColumns.ROW_ID.name());
                Object lastSeq = rec.getField(MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name());
                System.out.printf("id=%s, name=%s, score=%.2f, %s=%s, %s=%s%n",
                        rec.getField("id"), rec.getField("name"), rec.getField("score"),
                        MetadataColumns.ROW_ID.name(), rowId,
                        MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER.name(), lastSeq);
            }
        }
    }

    /**
     * 直接用 _row_id 值查找数据（仅 V3 表支持，Parquet 中需有该列）。
     * 注意: _row_id 是元数据列，不在 table schema 中，Expressions 无法直接过滤元数据列。
     * 因此先投影全量数据，再应用层过滤 _row_id。
     */
    public static void searchByRowId(TableIdentifier tableId, java.util.Set<Long> rowIdSet) throws IOException {
        Table table = catalog.loadTable(tableId);
        Schema schemaWithLineage = MetadataColumns.schemaWithRowLineage(table.schema());

        try (CloseableIterable<Record> result = IcebergGenerics.read(table)
                .project(schemaWithLineage)
                .build()) {
            for (Record rec : result) {
                Long rowId = (Long) rec.getField(MetadataColumns.ROW_ID.name());
                if (rowId != null && rowIdSet.contains(rowId)) {
                    System.out.printf("id=%s, name=%s, score=%.2f, %s=%s%n",
                            rec.getField("id"), rec.getField("name"), rec.getField("score"),
                            MetadataColumns.ROW_ID.name(), rowId);
                }
            }
        }
    }
}
