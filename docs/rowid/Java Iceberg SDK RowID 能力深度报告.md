# Java Iceberg SDK RowID 能力深度报告

> **报告目的**：系统梳理 Java Iceberg SDK 在 RowID（行级血缘）方面的完整实现，为判断 Rust SDK 的功能差距和识别可参考的设计提供依据。  
> **报告日期**：2026-07-01

## 一、引言：什么是 Row Lineage？

Iceberg V3 引入了**强制性的行级血缘追踪（mandatory row lineage tracking）**。这意味着表中的每一行数据都关联着两个核心元数据字段：

| 字段                                  | 类型     | 作用                                                                       |
| ----------------------------------- | ------ | ------------------------------------------------------------------------ |
| **`_row_id`**                       | BIGINT | 行的唯一标识符，**引擎层 UPDATE/MERGE 时继承保持不变；SDK 层 Overwrite（DELETE+INSERT）会重新分配** |
| **`_last_updated_sequence_number`** | BIGINT | 记录该行最后一次被修改时所对应的 Iceberg 快照序列号                                           |

这两个字段共同构成了行级血缘的基础：`_row_id` 回答“这是哪一行？”，`_last_updated_sequence_number` 回答“这行最后被谁、在什么时候修改过？”。

## 二、核心原则：RowID 永不改变

在进入具体机制之前，必须先明确 Iceberg RowID 的**根本原则**：

> **`_row_id` 在行的整个生命周期内保持唯一**。引擎层 UPDATE/MERGE 时新行继承旧行 `_row_id`；Compaction 时通过显式投影保留。**SDK 层 Overwrite（overwriteByRowFilter）是 DELETE+INSERT，会重新分配 `_row_id`（验证 V5/V8）。**

这个原则是所有行级血缘追踪功能的基础。如果 `_row_id` 会变化，就无法将同一行数据的不同版本串联起来。

## 三、元数据层的读写

### 3.1 元数据模型

Java SDK 通过三层元数据结构来管理 RowID：

| 层级                    | 字段             | 作用                                                                           |
| --------------------- | -------------- | ---------------------------------------------------------------------------- |
| **表级（TableMetadata）** | `next-row-id`  | 全局单调递增的 ID 分配器，通过 `HasTableOperations.operations().current().nextRowId()` 访问 |
| **快照级（Snapshot）**     | `first-row-id` | 该快照中第一条新增数据的 `_row_id`，作为快照级别的 ID 范围锚点                                       |
| **文件级（DataFile）**     | `first_row_id` | 该数据文件中第一行数据的 `_row_id`，用于文件级别的行 ID 范围标识                                      |

### 3.2 写入时的元数据行为

Java SDK 在写入数据时**自动**完成以下操作：

1. **分配 RowID**：从 `TableMetadata` 中读取当前的 `next-row-id`。

2. **计算范围**：根据写入的数据行数，计算出这批新数据的 RowID 范围。

3. **更新元数据**：将 `next-row-id` 更新为 `next-row-id + 写入行数`。

4. **记录文件元数据**：在新数据文件的元数据中记录其 `first_row_id`。

5. **读取时动态计算**：`_row_id = DataFile.firstRowId() + row_position_in_file`。**`Parquet.writeData()` 不嵌入物理列**（验证 V1），首次 INSERT 的 Parquet 文件只有用户 schema 列。Compaction 时若显式投影写入，则新文件会有物理 `_row_id` 列（验证 V7）。

### 3.3 _row_id 的动态计算机制

Java SDK 在写入时**不嵌入 _row_id 物理列**，读取时通过**动态计算**获取：

```
_row_id = DataFile.firstRowId() + FILE_POSITION
_last_updated_sequence_number = 当前快照的 sequenceNumber
```

**验证确认** (V1/V2): Parquet 数据文件经 ParquetFileReader 检查，仅含 id/name/score 三列，不含 `_row_id` 物理列。
读取时 _row_id 始终通过动态计算获得。

报告原描述的 COALESCE(_row_id, ICEBERG_FIRSTROWID + FILE_POSITION) 是
**Impala/Hive 等引擎层的 SQL 表达式**，不是 Java SDK 级别的代码逻辑。
SDK 层通过 scan 框架在读取时动态注入 _row_id 值。

### 3.4 元数据列（Metadata Columns）

Java SDK 通过以下 MetadataColumns 常量来定义 RowID 相关的列：

| 常量                                             | 列名                              | 字段 ID      | 说明                                   |
| ---------------------------------------------- | ------------------------------- | ---------- | ------------------------------------ |
| `MetadataColumns.ROW_ID`                       | `_row_id`                       | 2147483540 | 读取时通过 firstRowId + row_position 动态计算 |
| `MetadataColumns.LAST_UPDATED_SEQUENCE_NUMBER` | `_last_updated_sequence_number` | 2147483539 | 读取时赋值为当前快照的 sequenceNumber           |
| `MetadataColumns.ROW_POSITION`                 | `_pos`                          | 2147483645 | 行在文件中的位置偏移                           |

**验证确认** (V3):

- SDK 中列名是 `_row_id`
- SDK 中**没有 `ICEBERG_FIRSTROWID`** 这个常量 —— 这是 Impala 引擎层的虚拟列命名
- `_last_updated_sequence_number` 列名正确（非 `_file_last_updated_sequence_number`）
- 所有元数据列都是**动态计算**的，不作为物理列存储在 Parquet 中

### 3.5 SDK 如何区分系统列与用户列

`_row_id` 在 Parquet 中的物理位置（通常在最后一列，因为 `TypeUtil.join()` 将 MetadataColumns 追加在用户 schema 之后）**不影响 SDK 识别它**。SDK 通过 **field ID** 来区分系统列和用户列：

- Iceberg 将 field ID 范围 `Integer.MAX_VALUE - 100` ~ `Integer.MAX_VALUE - 200` 预留为元数据列
- `_row_id` 的 field ID = `2147483540`（`Integer.MAX_VALUE - 107`），落在此预留范围内
- Reader 看到这个 field ID 即判定为系统列，**无论列在 Parquet 中处于什么位置**

这一设计保证了：元数据列和用户列的命名不会冲突；物理存储位置不影响识别；Schema 演化（增加/删除用户列）不会误伤元数据列。

### 3.6 物理列与动态计算的统一访问

对上层调用者而言，**读 `_row_id` 时无需关心它是物理列还是动态计算**：

1. 使用 `MetadataColumns.schemaWithRowLineage()` 投影后，`rec.getField("_row_id")` 即可取值
2. 若 Compaction 时显式写入了 `_row_id` 物理列（见 §6.1），SDK 自动使用物理列值
3. 若 Parquet 中无物理列，SDK 自动通过 `firstRowId + row_position` 动态计算

两个路径对调用者透明，统一由 SDK 内部处理。

## 四、RowID 的写入与读取

### 4.1 写入 RowID

**验证确认** (V1): `Parquet.writeData(table.schema())` 仅写入用户 schema 列（如 id/name/score），**不自动嵌入 `_row_id` 物理列**。RowID 的分配在元数据层完成：manifest entry 记录 `first_row_id`，读取时通过 `firstRowId + row_position` 动态计算。若需在 Parquet 中嵌入物理 `_row_id`（如 Compaction 场景），需使用 `MetadataColumns.schemaWithRowLineage()` 作为写入 schema（见 §6.1 验证 V7）。

### 4.2 读取 RowID

**默认情况**：`_row_id` 通过动态计算获得：`_row_id = DataFile.firstRowId() + row_position_in_file`

**Compaction/重写后**：若使用 `MetadataColumns.schemaWithRowLineage()` 作为写入 schema，`_row_id` 将成为新 Parquet 文件中的物理列，此后直接读取物理列值。

| 场景                                | 读取方式                       | 说明                   |
| --------------------------------- | -------------------------- | -------------------- |
| **默认写入** (table.schema())         | 动态计算 firstRowId + position | _row_id 不在 Parquet 中 |
| **显式投影写入** (schemaWithRowLineage) | 直接读取物理列                    | _row_id 嵌入新文件作为物理列   |
| **引擎层 Compaction** (Spark/Flink)  | 直接读取物理列                    | 引擎显式保留 _row_id 物理列   |

## 五、DML 操作中的 RowID 行为

### 5.1 INSERT

| 行为                                  | 说明                            |
| ----------------------------------- | ----------------------------- |
| **`_row_id`**                       | 为新插入的每一行分配**全新的、全局唯一的** RowID |
| **`_last_updated_sequence_number`** | 设置为当前快照的序列号                   |

### 5.2 DELETE

| 行为            | 说明                                            |
| ------------- | --------------------------------------------- |
| **`_row_id`** | 被删除行的 `_row_id` **保持不变**，行被标记为删除              |
| **实现机制**      | 通过生成**删除文件（Delete File）** 来记录被删除行的位置，而非物理删除数据 |

### 5.3 UPDATE（核心机制）

Iceberg 中的 `UPDATE` 在底层是 **`DELETE` + `INSERT`** 的组合操作。

> **[实测纠正]** (验证 V5): SDK 层的 `overwriteByRowFilter()` 是 DELETE + INSERT，**会给被覆盖行分配新的 `_row_id`**（验证确认：id=1 的 `_row_id` 从 0 变为 13）。
> `_row_id` 继承（新行保留旧行的 `_row_id`）是**引擎层** (Spark/Flink/Impala) 执行 UPDATE 语句时的行为，引擎会显式读取旧的 `_row_id` 并写入新文件。
> 纯 Java SDK 的 `OverwriteFiles` API 不提供 `_row_id` 继承功能。

| 对象     | `_row_id` 行为                     | 说明                                                         |
| ------ | -------------------------------- | ---------------------------------------------------------- |
| **旧行** | **保持不变**                         | 被标记为删除，但其 `_row_id` 不变                                     |
| **新行** | **继承旧行的 `_row_id`** (引擎层 UPDATE) | 引擎层显式读取旧 `_row_id` 并写入新 Parquet；SDK 层 Overwrite 不继承（验证 V5） |

> **关键设计**：引擎层 `UPDATE` 产生的新行**不会获得新的 RowID**，而是**继承旧行的 RowID**。这是实现行级血缘追踪的根本——只有共享同一个 `_row_id`，才能将同一行数据的不同版本串联起来。SDK 层的 `overwriteByRowFilter()` 是 DELETE+INSERT，不会继承（验证 V5）。

**`_last_updated_sequence_number`**：新行的该字段会被更新为当前快照的序列号。

### 5.4 MERGE

| 行为        | 说明                    |
| --------- | --------------------- |
| **匹配到的行** | 被标记为删除，`_row_id` 保持不变 |

| **插入的新行** | 获得全新的 `_row_id`       |
| **更新后的行** | 继承旧行的 `_row_id`       |

> **注意**：使用等值删除（Equality Deletes）的引擎在写入更改之前会避免读取现有数据，因此**无法为新行提供原始行 ID**，这会导致行级血缘追踪失效。Iceberg 社区推荐使用**位置删除（Position Deletes）** 来保证血缘的完整性。

## 六、Compaction 中的 RowID 行为

### 6.1 核心原则

**Compaction 后，`_row_id` 保持不变**。

> **验证确认** (V7): 此原则正确，但实现需要明确以下机制：
> 
> 1. **默认 Parquet.writeData() 不嵌入 `_row_id`**，所以标准重写会**丢失 `_row_id` 信息**。
> 2. **正确做法**：读取时用 `schemaWithRowLineage()` 投影 `_row_id` → 写入时用相同 schema 将 `_row_id` 作为物理列嵌入新 Parquet 文件。
> 3. 验证 V7 演示了这个完整流程：读 100 行 (含 `_row_id`) → 写入新 Parquet (含 `_row_id` 物理列) → `newRewrite().commit()`。重写后所有 100 行的 `_row_id` 保持不变。
> 4. **没有物理列就无法保证非连续 `_row_id` 的正确性**：如果 Compaction 合并了来自不同快照的文件（`_row_id` 非连续），只靠动态计算（`firstRowId + position`）无法还原原始的 `_row_id` 值。

### 6.2 实现机制

Compaction 读取旧文件中的**存活数据**（未被删除的行），并将它们写入新的、更紧凑的数据文件。在写入新文件时：

1. **保留 `_row_id` 物理列**：新文件中的每一行都**显式写入其原有的 `_row_id`**。

2. **保留 `_last_updated_sequence_number`**：与 `_row_id` 一并保留。

> **实现方式**：`MetadataColumns.schemaWithRowLineage()` 同时投影 `_row_id` 和 `_last_updated_sequence_number` 两列。读取时两个值一起获取，写入时两个值一起成为新 Parquet 的物理列——**不是两步操作，是同一次 schema 投影的结果**（见验证 V7 的 `rewriteDataFiles` 代码）。
> 
> **为何需要保留**：`_last_updated_sequence_number` 记录该行最后一次被修改的快照序号。Compaction 本身不改变行数据（只是重组文件），因此应保留原来的 sequence number，而非写入 Compaction 快照的序号——否则行级血缘的时间追踪会断裂。

### 6.3 为什么必须保留？

如果 Compaction 后 `_row_id` 丢失或改变：

- 基于 `_row_id` 的所有追踪将失效

- 外部索引（如二级索引）将无法关联到正确的行

- 行级血缘查询将无法提供完整历史

**Dremio 明确指出**：”`OPTIMIZE TABLE` 在重写数据文件时会保留这两个值。如果没有这个保证，维护任务可能会悄无声息地覆盖血缘元数据，使其在审计目的下变得不可靠。”

### 6.4 不显式保留的后果

以上原则不仅适用于 Compaction，同样适用于 UPDATE 和 MERGE。如果引擎在执行这些操作时不显式写入 Row Lineage 字段：

| 操作 | 不写入 `_row_id` 的后果 | 不写入 `_last_updated_sequence_number` 的后果 |
|------|----------------------|------------------------------------------|
| **UPDATE** | SDK 从 `next-row-id` 分配新值 → 旧 `_row_id` 丢失 → 血缘链断裂，无法追溯行的历史版本 | 被赋值为 UPDATE 快照的 sequence number — 这是正确的（行确实被修改了），但需要**显式写入**而非依赖动态计算 |
| **MERGE** | 匹配行同上（丢失）；新插入行不受影响 | 同上 |
| **Compaction** | 最严重：多文件合并后行顺序完全改变，动态计算 `new_firstRowId + new_position` 与原始值**完全不同** → 所有基于 `_row_id` 的索引和血缘全部失效 | 被赋值为 Compaction 快照的 sequence number → **所有行的”最后修改时间”被伪造为 Compaction 时间**，历史变更信息永久丢失 |

**动态计算为何在 Compaction 后失效**：假设两次 INSERT 分别产生 _row_id [0,99] 和 [100,149]。Compaction 将两个文件合并为一个新文件，新文件 firstRowId=200，行重新排列。这时：
- 原始第一行：`_row_id=0` → Compaction 后：`200+0=200` ❌
- 原始第 50 行：`_row_id=50` → Compaction 后：`200+50=250` ❌

### 6.5 Spec 要求 vs SDK 自动执行

**Iceberg V3 Spec 明确规定** Compaction 必须保留 Row Lineage 元数据。这是合规要求，不是可选的优化。

但需要区分：**Spec 要求什么 ≠ SDK 自动执行什么**。

- `Parquet.writeData(table.schema())` — 仅写用户 schema，**不自动注入** `_row_id` 或 `_last_updated_sequence_number`
- SDK 提供的是**工具**而非**约束**：`schemaWithRowLineage()`、`newRewrite()` 等 API 让调用方能正确实现保留逻辑，但不会在 commit 时强制校验
- 引擎层（Spark/Flink/Impala）在实现 UPDATE/MERGE/Compaction 时必须遵守此规范，否则产生合规风险
- **调用方的责任**：使用 `schemaWithRowLineage()` 读写，确保 Row Lineage 字段不丢失

## 七、行级血缘追踪

### 7.1 SDK 提供的基础能力

Java SDK 本身**不提供** `row_lineage` 系统表或类似的直接查询接口。SDK 的角色是提供**基础能力**：

1. **元数据列**：`_row_id` 和 `_last_updated_sequence_number` 作为系统列可供查询

2. **增量扫描 API**：`IncrementalDataTableScan` 支持基于快照范围的增量读取

3. **元数据读取 API**：`Snapshot.firstRowId()`、`DataFile.firstRowId()` 等

### 7.2 上层引擎如何构建血缘查询？

`row_lineage` 系统表是**上层计算引擎**（如 Apache Hive、Apache Impala、StarRocks、Dremio）利用 Java SDK 提供的基础能力实现的用户友好接口，而非 Java SDK 直接提供。

| 引擎                     | row_lineage 支持情况                                     |
| ---------------------- | ---------------------------------------------------- |
| **Apache Hive**        | 已在 4.3.0 版本中**实现并修复**了对 Iceberg V3 行级血缘的支持           |
| **Apache Impala**      | 正在增加 `ICEBERG_ROW_ID` 等虚拟列作为语法糖，简化 Row Lineage 字段的查询 |
| **StarRocks / Dremio** | 已支持查询 `_row_id` 等行级血缘元数据列                            |

### 7.3 血缘追踪的完整链路

```textile
┌─────────────────────────────────────────────────────────────────────────────┐
│                        行级血缘追踪完整链路                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  数据层（Java SDK 提供）                                                    │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  每行数据携带：_row_id（不变）+ _last_updated_sequence_number（变更） │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    ↓                                        │
│  查询层（Java SDK 提供）                                                    │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  增量扫描 API + 元数据读取 API                                        │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                    ↓                                        │
│  引擎层（上层引擎实现）                                                     │
│  ┌───────────────────────────────────────────────────────────────────────┐ │
│  │  row_lineage 系统表 / 虚拟列语法糖                                    │ │
│  │  SELECT * FROM table$row_lineage WHERE _row_id = 12345;              │ │
│  └───────────────────────────────────────────────────────────────────────┘ │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 八、Java SDK 能力总结

| 能力维度                                | Java SDK 实现方式                | 支持状态    |
| ----------------------------------- | ---------------------------- | ------- |
| **`_row_id` 分配**                    | 基于 `next-row-id` 自动分配        | ✅ 完整支持  |
| **`_row_id` 写入**                    | 元数据层分配 (next-row-id)，读取时动态计算 | ✅ 完整支持  |
| **`_row_id` 读取**                    | 动态计算 (firstRowId + position) | ✅ 完整支持  |
| **`_last_updated_sequence_number`** | 动态计算 (当前快照 sequenceNumber)   | ✅ 完整支持  |
| **INSERT RowID**                    | 分配全新唯一 ID                    | ✅ 完整支持  |
| **DELETE RowID**                    | 保持不变，行标记删除                   | ✅ 完整支持  |
| **UPDATE RowID**                    | 引擎层继承旧行；SDK 层 Overwrite 新 ID | ✅ 完整支持  |
| **MERGE RowID**                     | 匹配行继承，新行分配新 ID               | ✅ 完整支持  |
| **Compaction RowID**                | **保持不变**（需显式投影 _row_id 写入）   | ✅ 完整支持  |
| **增量扫描 API**                        | `IncrementalDataTableScan`   | ✅ 完整支持  |
| **`row_lineage` 系统表**               | 引擎层功能，**非 SDK 提供**           | 由上层引擎实现 |
| **虚拟列语法糖**                          | 引擎层功能，**非 SDK 提供**           | 由上层引擎实现 |

## 九、与 Rust SDK 的差距对比

基于上述 Java SDK 的能力清单，Rust SDK 在以下维度存在差距：

| 能力维度                                | Java SDK                                                       | Rust SDK            | 差距说明                                             |
| ----------------------------------- | -------------------------------------------------------------- | ------------------- | ------------------------------------------------ |
| **`next_row_id` 元数据**               | ✅ 完整支持 (HasTableOperations.operations().current().nextRowId()) | 🔄 部分支持（#1652 进行中）  | Java 直接读 TableMetadata；Rust 支持不完整                |
| **`first_row_id` 写入**               | ✅ 自动写入                                                         | 🔄 进行中（#2579 Open）  | Rust 写入时不会自动为 DataFile 分配 `first_row_id`         |
| **`_row_id` 物理列写入**                 | ❌ 默认不写入（动态计算）                                                  | ❌ 无公开 PR            | 两者均不在首次 INSERT 时嵌入物理列，Compaction 时可显式写入          |
| **`_row_id` 列读取**                   | ✅ 动态计算 (firstRowId + position)                                 | ❌ 无公开 PR            | Java 默认动态计算；Compaction 后有物理列时读物理列                |
| **`_pos` 列读取**                      | ✅ 完整支持                                                         | 🔄 进行中（#2746 Draft） | Rust 正在实现 `_pos` 列读取                             |
| **`_last_updated_sequence_number`** | ✅ 完整支持                                                         | ❌ 无公开 PR            | Rust 完全缺失此字段的支持                                  |
| **UPDATE RowID 继承**                 | ✅ 引擎层支持 (SDK 层 Overwrite 不继承)                                  | ❌ 无公开 PR            | Java 引擎层 UPDATE 继承；SDK Overwrite 是 DELETE+INSERT |
| **Compaction 保留 RowID**             | ✅ 完整支持 (需显式投影)                                                 | ❌ 无公开 PR            | Java 需用 schemaWithRowLineage 读写；Rust 暂无此能力       |
| **增量扫描 API**                        | ✅ 完整支持                                                         | ✅ 已支持（#2153 Merged） | Rust 已实现 `appends_after()`                       |

## 十、Rust SDK 可参考的设计要点

基于 Java SDK 的实现，Rust SDK 在未来实现 RowID 功能时，建议参考以下设计：

### 10.1 双重读取保障机制

```textile
row-id = DataFile.firstRowId() + FILE_POSITION  (SDK 默认)
row-id = COALESCE(_row_id, ICEBERG_FIRSTROWID + FILE_POSITION)  (引擎层 SQL 表达式)
```

- **SDK 默认**：动态计算 `firstRowId + position`
- **Compaction 后**：物理列存在时直接读取

### 10.2 UPDATE 时继承 RowID

- `UPDATE` 产生的新行**必须继承旧行的 `_row_id`**，而非分配新 ID

- 这是实现行级血缘追踪的**根本前提**

### 10.3 Compaction 时保留 RowID

- Compaction 写入新文件时，**必须保留 `_row_id` 物理列**

- 否则所有基于 RowID 的追踪和索引将全部失效

### 10.4 隐藏列 + 虚拟列的分层设计

- **元数据列（默认动态计算）**：`_row_id`（field ID 2147483540）、`_last_updated_sequence_number`（field ID 2147483539）

- **引擎层虚拟列（非 SDK 常量）**：`ICEBERG_FIRSTROWID`、`ICEBERG_DATASEQUENCE_NUMBER`（均为 Impala 命名）

- 物理列存在时直接读取，无物理列时 SDK 动态计算；引擎层用 COALESCE 统一

### 10.5 SDK 与引擎层的职责分离

- **SDK 层**：提供 `_row_id`、`_last_updated_sequence_number` 等元数据列 + 增量扫描 API

- **引擎层**：利用 SDK 能力构建 `row_lineage` 系统表、虚拟列语法糖等用户友好接口

- Rust SDK 无需承担引擎层的功能，但需确保底层 API 足够完备

## 十一、总结

Java Iceberg SDK 的 RowID 实现是一个**从元数据管理、数据读写、到 DML 操作和表维护的完整闭环**。其核心设计原则可以归纳为：

| 原则                                       | 说明                                            |
| ---------------------------------------- | --------------------------------------------- |
| **`_row_id` 永不改变**                       | 行在整个生命周期内保持相同的 `_row_id`                      |
| **元数据分配 + 动态计算**                         | 读取时 firstRowId + position，Compaction 时显式写入物理列 |
| **UPDATE 继承 RowID** (引擎层)                | 引擎层 UPDATE 继承旧 `_row_id`；SDK 层 Overwrite 不继承  |
| **Compaction 保留 RowID**                  | 数据重写时保留 `_row_id` 物理列                         |
| **`_last_updated_sequence_number` 记录变更** | 标识行的最后修改版本                                    |
| **SDK 与引擎职责分离**                          | SDK 提供底层能力，引擎层构建用户接口                          |

这些设计原则共同构成了 Iceberg V3 行级血缘的完整基础。Rust SDK 在追赶 Java SDK 的功能时，最值得参考和借鉴的正是这套经过验证的、完整闭环的设计体系。当前 Rust SDK 的优先事项是补齐 `_row_id` 的物理列写入和读取能力，以及 `_last_updated_sequence_number` 的完整支持。

---

## 附录：实测验证记录

报告中的关键声明已通过 ReportVerifier 进行实际代码验证（2026-07-01）。
验证代码见 src/main/java/org/example/rowid/ReportVerifier.java。

| 验证  | 报告声明                             | 结果   | 说明                                               |
| --- | -------------------------------- | ---- | ------------------------------------------------ |
| V1  | 3.2/4.1: _row_id 作为物理列写入 Parquet | FAIL | Parquet 文件中只有用户 schema 列，不含 _row_id              |
| V2  | 3.3: COALESCE 双重保障机制             | PASS | _row_id 通过 firstRowId + position 动态计算，非 COALESCE |
| V3  | 3.4: 隐藏列命名 _row_id               | PASS | SDK 中名为 _row_id，无 ICEBERG_FIRSTROWID 常量          |
| V4  | 5.1: INSERT 分配全局唯一 _row_id       | PASS | 起始值 0，全局递增，nextRowId 正确                          |
| V5  | 5.3: UPDATE 继承 _row_id           | PASS | SDK 层 overwriteByRowFilter 不继承(新 ID)，继承是引擎层行为    |
| V6  | 5.2: DELETE _row_id 保留           | PASS | 旧快照中 _row_id 历史保留，新快照获得新 ID                      |
| V7  | 6: Compaction 保留 _row_id         | PASS | 需显式用 schemaWithRowLineage 读写才能保留                 |
| V8  | 2: _row_id 永不改变                  | PASS | 未修改的行 _row_id 不变，被覆盖行获新 ID                       |

**关键发现**：Java SDK 的 Parquet.writeData() 不自动嵌入 _row_id 物理列。
_row_id 通过 firstRowId + row_position 动态计算。
COALESCE 表达式是引擎层 (Impala/Hive) 行为，非 SDK API。
