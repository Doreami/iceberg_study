package org.example.rowid;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * 原始 Parquet 文件检查工具。
 * 用于绕过 Iceberg 读路径，直接检查 Parquet 文件中的物理列。
 */
public class ParquetInspector {

    private final Configuration hadoopConf;

    public ParquetInspector() {
        this.hadoopConf = new Configuration();
    }

    /**
     * 使用 ParquetFileReader 读取 Parquet 文件的 footer metadata，
     * 打印完整的 schema 信息（列名、类型、field ID）。
     */
    public SchemaInfo inspectSchema(String filePath) throws IOException {
        Path path = new Path(filePath);
        ParquetMetadata footer;
        try (ParquetFileReader reader = ParquetFileReader.open(hadoopConf, path)) {
            footer = reader.getFooter();
        }

        MessageType schema = footer.getFileMetaData().getSchema();
        int rowCount = (int) footer.getBlocks().stream()
                .mapToLong(b -> b.getRowCount()).sum();

        SchemaInfo info = new SchemaInfo(filePath, schema, rowCount);

        System.out.println("  Parquet 文件: " + fileName(filePath));
        System.out.println("  总行数: " + rowCount);
        System.out.println("  物理列 (" + schema.getFieldCount() + " 列):");
        for (int i = 0; i < schema.getFieldCount(); i++) {
            Type field = schema.getType(i);
            String idStr = field.getId() != null ? String.valueOf(field.getId().intValue()) : "(无)";
            System.out.printf("    [%d] name=%-40s type=%-15s fieldId=%s%n",
                    i, field.getName(), field.asPrimitiveType().getPrimitiveTypeName(), idStr);
            info.addColumn(field.getName(), field.getId() != null ? field.getId().intValue() : -1);
        }

        // 检查是否有 _row_id 相关列
        boolean hasRowId = info.columns.containsKey("_row_id")
                        || info.columns.containsKey("_file_row_id");
        System.out.println("  _row_id 物理列: " + (hasRowId ? "✅ 存在" : "❌ 不存在"));
        if (hasRowId) {
            System.out.println("    列名: " + (info.columns.containsKey("_row_id") ? "_row_id" : "_file_row_id"));
        }
        System.out.println();

        return info;
    }

    /**
     * 读取 Parquet 文件的前 limit 行数据，逐列打印值。
     * 特别关注 _row_id / _file_row_id 列的值。
     */
    public List<Map<String, Object>> readRows(String filePath, int limit) throws IOException {
        Path path = new Path(filePath);
        List<Map<String, Object>> rows = new ArrayList<>();

        MessageType schema;
        try (ParquetFileReader reader = ParquetFileReader.open(hadoopConf, path)) {
            schema = reader.getFooter().getFileMetaData().getSchema();
        }

        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(hadoopConf)
                .build()) {
            Group group;
            int rowNum = 0;
            while ((group = reader.read()) != null && rowNum < limit) {
                Map<String, Object> row = new LinkedHashMap<>();
                for (int i = 0; i < schema.getFieldCount(); i++) {
                    Type field = schema.getType(i);
                    String fieldName = field.getName();
                    try {
                        // 尝试多种类型读取
                        Object value = readFieldValue(group, fieldName, i);
                        row.put(fieldName, value);
                    } catch (Exception e) {
                        row.put(fieldName, "<error: " + e.getMessage() + ">");
                    }
                }
                rows.add(row);
                rowNum++;
            }
        }

        // 打印结果
        if (rows.isEmpty()) {
            System.out.println("  (文件为空)");
            return rows;
        }

        // 打印表头
        Map<String, Object> firstRow = rows.get(0);
        System.out.print("  row |");
        for (String col : firstRow.keySet()) {
            System.out.printf(" %-15s |", truncate(col, 15));
        }
        System.out.println();

        // 打印数据
        for (int r = 0; r < rows.size(); r++) {
            System.out.printf("  %3d |", r);
            for (String col : firstRow.keySet()) {
                Object val = rows.get(r).get(col);
                String str = val == null ? "NULL" : val.toString();
                System.out.printf(" %-15s |", truncate(str, 15));
            }
            System.out.println();
        }
        System.out.println();

        return rows;
    }

    private Object readFieldValue(Group group, String fieldName, int fieldIndex) {
        try {
            return group.getLong(fieldName, 0);
        } catch (Exception e1) {
            try {
                return group.getLong(fieldIndex, 0);
            } catch (Exception e2) {
                try {
                    return group.getString(fieldName, 0);
                } catch (Exception e3) {
                    try {
                        return group.getString(fieldIndex, 0);
                    } catch (Exception e4) {
                        try {
                            return group.getDouble(fieldName, 0);
                        } catch (Exception e5) {
                            try {
                                return group.getDouble(fieldIndex, 0);
                            } catch (Exception e6) {
                                return "<unreadable>";
                            }
                        }
                    }
                }
            }
        }
    }

    private static String fileName(String path) {
        int idx = Math.max(path.lastIndexOf('/'), path.lastIndexOf('\\'));
        return idx >= 0 ? path.substring(idx + 1) : path;
    }

    private static String truncate(String s, int len) {
        return s.length() > len ? s.substring(0, len - 1) + "…" : s;
    }

    /** Parquet schema 信息 */
    public static class SchemaInfo {
        public final String filePath;
        public final MessageType rawSchema;
        public final int rowCount;
        public final Map<String, Integer> columns; // name -> fieldId (-1 if absent)

        SchemaInfo(String filePath, MessageType rawSchema, int rowCount) {
            this.filePath = filePath;
            this.rawSchema = rawSchema;
            this.rowCount = rowCount;
            this.columns = new LinkedHashMap<>();
        }

        void addColumn(String name, int fieldId) {
            columns.put(name, fieldId);
        }

        public boolean hasColumn(String name) {
            return columns.containsKey(name);
        }

        public boolean hasRowIdColumn() {
            return hasColumn("_row_id") || hasColumn("_file_row_id");
        }

        public boolean hasLastUpdatedSeqColumn() {
            return hasColumn("_last_updated_sequence_number")
                || hasColumn("_file_last_updated_sequence_number");
        }
    }
}
