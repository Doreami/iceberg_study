"""Fix ALL remaining issues in one pass."""
path = r"D:\Projects\JavaProjects\iceberg_study\docs\rowid\Java Iceberg SDK RowID 能力深度报告.md"
with open(path, "r", encoding="utf-8") as f:
    content = f.read()
content = content.replace(" ", " ")

fixes = []

# 1. §3.2 point 5: 在"写入"节不该描述读取，缩为一句
old = "5. **读取时动态计算**：详见 §3.3。`Parquet.writeData()` 不嵌入物理列，首次 INSERT 后 Parquet 只有用户 schema 列。Compaction 时显式投影则新文件有物理列（验证 V1/V7）。"
new = "5. **不嵌入物理列**：`Parquet.writeData()` 仅写用户 schema 列，`_row_id` 不在 Parquet 中。读取机制见 §3.3。"
content = content.replace(old, new); fixes.append("3.2 point5")

# 2. §3.3: "验证确认 (V1/V2)" → 中性描述
old = "**验证确认** (V1/V2): ParquetFileReader 检查确认仅含 id/name/score 三列。读取时 _row_id 正常返回 0,1,...,99。"
new = "实测 (V1/V2): ParquetFileReader 检查确认仅含 id/name/score 三列，读取时 `_row_id` 正常返回 0,1,...,99。"
content = content.replace(old, new); fixes.append("3.3 V1/V2 label")

# 3. §3.3: "报告原描述的 COALESCE..." → 中性
old = "> **注意**：报告原描述的 COALESCE(_row_id, ICEBERG_FIRSTROWID + FILE_POSITION) 是 Impala/Hive 等引擎层的 SQL 表达式，不是 Java SDK 级别的代码逻辑。"
new = "COALESCE(_row_id, ICEBERG_FIRSTROWID + FILE_POSITION) 是 Impala/Hive 等引擎层的 SQL 表达式，非 SDK 级别的代码逻辑。SDK 内部通过 scan 框架在读取时注入 `_row_id` 值。"
content = content.replace(old, new); fixes.append("3.3 COALESCE note")

# 4. §3.4: "**验证确认** (V3):" → 中性
old = "**验证确认** (V3):"
new = "实测 (V3):"
content = content.replace(old, new); fixes.append("3.4 label")

# 5. §3.4 table: _row_id 说明加上物理列路径
old = "| `MetadataColumns.ROW_ID`                       | `_row_id`                       | 2147483540 | 读取时通过 firstRowId + row_position 动态计算 |"
new = "| `MetadataColumns.ROW_ID`                       | `_row_id`                       | 2147483540 | 默认 firstRowId + position 动态计算；物理列存在时直接读取 |"
content = content.replace(old, new); fixes.append("3.4 ROW_ID desc")

# 6. §3.4 table: _last_updated_sequence_number 同理
old = "| `MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER` | `_last_updated_sequence_number` | 2147483539 | 读取时赋值为当前快照的 sequenceNumber           |"
new = "| `MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER` | `_last_updated_sequence_number` | 2147483539 | 默认赋值为当前快照 sequenceNumber；物理列存在时直接读取    |"
content = content.replace(old, new); fixes.append("3.4 SEQ desc")

# 7. §7 (能力总结): _row_id 读取 — 补充物理列路径
old = "| **`_row_id` 读取**                    | 动态计算 (firstRowId + position) | ✅ 完整支持  |"
new = "| **`_row_id` 读取**                    | 默认动态计算；物理列存在时直接读取（自动切换） | ✅ 完整支持  |"
content = content.replace(old, new); fixes.append("7 read desc")

# 8. §7 (能力总结): _last_updated_sequence_number — 同理
old = "| **`_last_updated_sequence_number`** | 动态计算 (当前快照 sequenceNumber)   | ✅ 完整支持  |"
new = "| **`_last_updated_sequence_number`** | 默认动态计算；物理列存在时直接读取（自动切换）   | ✅ 完整支持  |"
content = content.replace(old, new); fixes.append("7 seq desc")

# 9. §7 (能力总结): Compaction — 补充 _last_updated_sequence_number
old = "| **Compaction RowID**                | **保持不变**（需显式投影 _row_id 写入）   | ✅ 完整支持  |"
new = "| **Compaction RowID**                | **保持不变**（需显式投影 _row_id + _last_updated_sequence_number 写入） | ✅ 完整支持  |"
content = content.replace(old, new); fixes.append("7 compaction desc")

# 10. §7 (能力总结): _row_id 写入 — 更新"元数据层分配"去歧义
old = "| **`_row_id` 写入**                    | 元数据层分配 (next-row-id)；默认不写物理列，Compaction 可显式嵌入 | ✅ 完整支持  |"
new = "| **`_row_id` 写入**                    | next-row-id 分配 + manifest 记录 first_row_id；默认不写物理列 | ✅ 完整支持  |"
content = content.replace(old, new); fixes.append("7 write desc")

# 11. §8 (Rust对比): Java _row_id 列读取 — 对齐§7
old = "| **`_row_id` 列读取**                   | ✅ 动态计算 (firstRowId + position)                                 | ❌ 无公开 PR            | Java 默认动态计算；Compaction 后有物理列时读物理列                |"
new = "| **`_row_id` 列读取**                   | ✅ 动态计算 / 物理列读取 自动切换                                     | ❌ 无公开 PR            | Java 默认动态计算；Compaction 后有物理列时读物理列                |"
content = content.replace(old, new); fixes.append("8 read col")

# 12. §8 (Rust对比): Java _last_updated_sequence_number 对齐
old = "| **`_last_updated_sequence_number`** | ✅ 完整支持                                                         | ❌ 无公开 PR            | Rust 完全缺失此字段的支持                                  |"
new = "| **`_last_updated_sequence_number`** | ✅ 动态计算 / 物理列读取 自动切换                                      | ❌ 无公开 PR            | Java 默认动态计算；Compaction 后有物理列时读物理列                |"
content = content.replace(old, new); fixes.append("8 seq col")

# 13. §8 (Rust对比): Java Compaction 补充 _last_updated_sequence_number
old = "| **Compaction 保留 RowID**             | ✅ 完整支持 (需显式投影)                                                 | ❌ 无公开 PR            | Java 需用 schemaWithRowLineage 读写；Rust 暂无此能力       |"
new = "| **Compaction 保留 RowID**             | ✅ 完整支持 (需显式投影 _row_id + _last_updated_sequence_number)               | ❌ 无公开 PR            | Java 需用 schemaWithRowLineage 同时投影两列；Rust 暂无此能力    |"
content = content.replace(old, new); fixes.append("8 compaction col")

# 14. §10 (总结): 补充物理列读取路径
old = "| **元数据分配 + 动态计算**                         | 读取时 firstRowId + position，Compaction 时显式写入物理列 |"
new = "| **元数据分配 + 动态计算 + 物理列自动切换**             | 默认动态计算；Compaction 显式嵌入后自动切换为物理列读取 |"
content = content.replace(old, new); fixes.append("10 summary")

# 15. §10 (总结): 最后一段"物理列写入和读取能力"措辞更新
old = "当前 Rust SDK 的优先事项是补齐 `_row_id` 的物理列写入和读取能力，以及 `_last_updated_sequence_number` 的完整支持。"
new = "当前 Rust SDK 的优先事项是补齐 `_row_id` 和 `_last_updated_sequence_number` 的动态计算能力（默认读取路径），以及 Compaction 时显式投影写入的能力。"
content = content.replace(old, new); fixes.append("10 Rust priority")

# 16. §5.1: "验证确认 (V7)" → 统一语气
old = "> **验证确认** (V7): 此原则正确，但实现需要明确以下机制："
new = "> 此原则正确，但实现需要明确以下机制（验证 V7）："
content = content.replace(old, new); fixes.append("5.1 V7 label")

# 17. §4.3: 还有一处"验证确认"
old = "> **验证确认** (V5): SDK 层的"
new = "> SDK 层的"
content = content.replace(old, new); fixes.append("4.3 V5 label")

# 18. §3.1: "在新数据文件的元数据中" → 精确
old = "4. **记录文件元数据**：在新数据文件的元数据中记录其 `first_row_id`。"
new = "4. **记录文件元数据**：在 manifest entry 中记录该文件的 `first_row_id`，作为该文件行 ID 范围的起始锚点。"
content = content.replace(old, new); fixes.append("3.2 point4")

with open(path, "w", encoding="utf-8") as f:
    f.write(content)

print(f"Fixed {len(fixes)} issues:")
for f in fixes:
    print(f"  - {f}")
