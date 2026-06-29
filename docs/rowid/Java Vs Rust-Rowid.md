## 一、Java Iceberg SDK vs Rust Iceberg SDK：ROWID 相关能力详细对比

### 1.1 整体成熟度对比

| 维度                          | Java SDK                                                                    | Rust SDK                                                                                                         |
| --------------------------- | --------------------------------------------------------------------------- | ---------------------------------------------------------------------------------------------------------------- |
| **V3 元数据支持**                | ✅ 完整支持（1.x 系列）                                                              | ✅ 自 0.8.0 起支持 V3 元数据格式[](https://iceberg.incubator.apache.org/blog/apache-iceberg-rust-0.8.0-release/)           |
| **`_row_id` 字段定义**          | ✅ 完整                                                                        | ✅ 提供 `RESERVED_FIELD_ID_ROW_ID` 常量                                                                               |
| **增量扫描 (Incremental Scan)** | ✅ `IncrementalDataTableScan` 完整实现                                           | ⚠️ PR #2153 已合并，支持 `appends_after()` 等 API                                                                       |
| **行级血缘 (Row Lineage)**      | ✅ 规划中（`row_lineage` 系统表）                                                    | ❌ 暂无                                                                                                             |
| **删除向量 (Deletion Vector)**  | ✅ 完整支持[](https://blog.javatask.dev/blog/iceberg-for-ot-part-3-v3-features/) | ⚠️ 仅定义了 `DELETION_VECTOR_V1` 常量，完整支持开发中[](https://blog.javatask.dev/blog/iceberg-for-ot-part-3-v3-features/)     |
| **生态集成**                    | Spark/Flink/Trino/StarRocks 等                                               | DataFusion 集成，生态仍在扩展[](https://dev.to/alexmercedcoder/apache-data-lakehouse-weekly-march-10-17-2026-12fd)        |
| **发布节奏**                    | 稳定（1.10.x/1.11.x）                                                           | 快速迭代（0.8.0 → 0.9.0，四个月四个版本）[](https://dev.to/alexmercedcoder/apache-data-lakehouse-weekly-march-10-17-2026-12fd) |

### 1.2 ROWID 相关核心 API 对比

| API 功能                       | Java SDK                                                                             | Rust SDK                                                                                                                                            |
| ---------------------------- | ------------------------------------------------------------------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------- |
| **获取 snapshot 的 firstRowId** | `snapshot.firstRowId()`                                                              | `snapshot.first_row_id()`                                                                                                                           |
| **增量扫描（两快照之间）**              | `table.newIncrementalDataTableScan().fromSnapshotExclusive(fromId).toSnapshot(toId)` | `table.scan().from_snapshot_exclusive(from_id).to_snapshot(to_id).build()?`                                                                         |
| **增量扫描（指定快照之后）**             | `table.newIncrementalDataTableScan().fromSnapshotExclusive(fromId)`                  | `table.scan().appends_after(from_id).build()?`                                                                                                      |
| **行级血缘查询**                   | `row_lineage` 系统表（规划中）                                                               | ❌ 不支持                                                                                                                                               |
| **删除向量读取**                   | 完整 Puffin 集成                                                                         | `read_deletion_vector_from_puffin()` 基础函数[](https://docs.rs/hyperstreamdb/0.1.4/hyperstreamdb/core/puffin/fn.read_deletion_vector_from_puffin.html) |

---

## 二、Java Iceberg SDK ROWID 使用示例

### 示例 1：获取快照的 firstRowId

```java
import org.apache.iceberg.Table;
import org.apache.iceberg.Snapshot;

Table table = catalog.loadTable(tableIdentifier);

// 获取当前快照
Snapshot currentSnapshot = table.currentSnapshot();
if (currentSnapshot != null) {
    long firstRowId = currentSnapshot.firstRowId();
    System.out.println("First row ID in current snapshot: " + firstRowId);
    // 该快照中所有新增行的 row_id >= firstRowId[reference:19]
}

// 遍历所有快照
for (Snapshot snapshot : table.snapshots()) {
    System.out.println("Snapshot " + snapshot.snapshotId() + 
                       " firstRowId: " + snapshot.firstRowId());
}
```

### 示例 2：增量扫描（两快照之间的新增数据）

```java
import org.apache.iceberg.TableScan;
import org.apache.iceberg.expressions.Expressions;

// 从快照 100（不包含）到快照 200（包含）的增量读取
TableScan incrementalScan = table
    .newIncrementalDataTableScan()
    .fromSnapshotExclusive(100L)  // 不包含起始快照
    .toSnapshot(200L)             // 包含结束快照
    .build();

// 执行扫描，获取新增的数据文件
Iterable<FileScanTask> tasks = incrementalScan.planFiles();
for (FileScanTask task : tasks) {
    // 处理每个数据文件中新增的行
    // 这些行的 _row_id >= firstRowId[reference:20]
}

// 配合过滤条件
TableScan filteredScan = table
    .newIncrementalDataTableScan()
    .fromSnapshotExclusive(100L)
    .toSnapshot(200L)
    .filter(Expressions.equal("category", "A"))
    .build();
```

### 示例 3：从指定快照之后持续增量读取（CDC 场景）

```java
// 场景：CDC 管道，从上次检查点快照之后读取所有新增数据
long lastProcessedSnapshotId = getLastCheckpointSnapshotId(); // 从状态存储读取

TableScan cdcScan = table
    .newIncrementalDataTableScan()
    .fromSnapshotExclusive(lastProcessedSnapshotId)  // 只读新增数据
    .build();

// 每次轮询获取新增数据
while (true) {
    TableScan scan = table
        .newIncrementalDataTableScan()
        .fromSnapshotExclusive(lastProcessedSnapshotId)
        .build();
    
    for (FileScanTask task : scan.planFiles()) {
        processNewRows(task);
    }
    
    // 更新检查点
    lastProcessedSnapshotId = table.currentSnapshot().snapshotId();
    saveCheckpoint(lastProcessedSnapshotId);
    Thread.sleep(pollInterval);
}
```

### 示例 4：变更日志扫描（Changelog Scan）

```java
import org.apache.iceberg.TableScan;

// 获取两个快照之间的变更日志（包含删除和更新）
TableScan changelogScan = table
    .newIncrementalChangelogScan()
    .fromSnapshotExclusive(startSnapshotId)
    .toSnapshot(endSnapshotId)
    .build();
// 注意：ChangelogScan 会包含 DELETE 和 UPDATE 操作，而不仅仅是 APPEND[reference:21]
```

### 示例 5：行级血缘查询（规划中）

```java
// 注意：此功能在 Iceberg Java SDK 中仍在开发/规划阶段[reference:22]
// 未来可通过系统表查询单行的完整变更历史

// 伪代码 - 未来 API 可能如下：
// Table rowLineageTable = catalog.loadTable(
//     TableIdentifier.of("system", "row_lineage")
// );
// 
// // 查询某一行（_row_id = 12345）的完整血缘
// Iterable<RowLineageEntry> lineage = rowLineageTable
//     .scan()
//     .filter(Expressions.equal("_row_id", 12345L))
//     .build()
//     .planFiles();
// 
// // 遍历所有快照，重建该行的变更历史
// for (RowLineageEntry entry : lineage) {
//     System.out.println("Snapshot: " + entry.snapshotId() + 
//                        ", Operation: " + entry.operation() +
//                        ", Value: " + entry.rowData());
// }
```

---

## 三、Rust Iceberg SDK ROWID 使用示例

### 示例 1：获取快照的 first_row_id（✅ 支持）

```rust
use iceberg::Table;

let table: Table = catalog.load_table(&table_identifier).await?;

// 获取当前快照
if let Some(snapshot) = table.current_snapshot() {
    let first_row_id = snapshot.first_row_id();
    println!("First row ID in current snapshot: {}", first_row_id);
    // 该快照中所有新增行的 row_id >= first_row_id[reference:23]
}

// 遍历所有快照
for snapshot in table.snapshots()? {
    println!("Snapshot {} first_row_id: {}", 
             snapshot.snapshot_id(), 
             snapshot.first_row_id());
}
```

### 示例 2：增量扫描（✅ 支持，PR #2153 已合并）

```rust
use iceberg::Table;
use iceberg::scan::TableScan;

// 从快照 100（不包含）到快照 200（包含）的增量读取
let scan = table
    .scan()
    .from_snapshot_exclusive(100)?  // 不包含起始快照
    .to_snapshot(200)?             // 包含结束快照
    .build()?;

// 执行扫描
let tasks = scan.plan_files().await?;
for task in tasks {
    // 处理增量数据
}

// 从指定快照之后的所有新增数据
let scan = table
    .scan()
    .appends_after(100)?  // 快照 100 之后的所有 APPEND 操作
    .build()?;

// 包含起始快照
let scan = table
    .scan()
    .from_snapshot_inclusive(100)?
    .to_snapshot(200)?
    .build()?;
```

### 示例 3：DataFusion 集成增量查询（✅ 支持）

```rust
use iceberg::Table;
use datafusion::prelude::*;
use iceberg_datafusion::IcebergStaticTableProvider;

let table: Table = catalog.load_table(&table_identifier).await?;

// 创建增量表提供者
let provider = IcebergStaticTableProvider::try_new_incremental(
    table,
    from_snapshot_id,  // 不包含
    to_snapshot_id     // 包含
).await?;

// 注册到 DataFusion 上下文
let ctx = SessionContext::new();
ctx.register_table("changes", Arc::new(provider))?;

// 使用 SQL 查询增量数据
let df = ctx.sql("SELECT * FROM changes WHERE category = 'A'").await?;
let results = df.collect().await?;

// 快捷方式：从指定快照之后的所有新增数据
let provider = IcebergStaticTableProvider::try_new_appends_after(
    table,
    from_snapshot_id
).await?;
```

### 示例 4：读取删除向量（⚠️ 基础支持，完整功能开发中）

```rust
use iceberg::puffin::{PuffinReader, DELETION_VECTOR_V1};

// Rust SDK 定义了删除向量常量[reference:30][reference:31]
// 以及基础的读取函数[reference:32][reference:33]

// 读取 Puffin 文件中的删除向量（当前在 hyperstreamdb 等下游库中可用）
// let reader = PuffinReader::new(file)?;
// let deleted_positions = read_deletion_vector_from_puffin(&mut reader, blob_idx)?;
// // 返回 RoaringBitmap，包含已删除的行位置[reference:34]
```

### 示例 5：遍历快照祖先链（✅ 支持）

```rust
use iceberg::table::Table;

// 获取快照的祖先链[reference:35]
let table: Table = catalog.load_table(&table_identifier).await?;
let metadata = table.metadata();

// 从指定快照开始遍历到根快照
let ancestors = metadata.ancestors_of(snapshot_id)?;
for snapshot in ancestors {
    println!("Ancestor snapshot: {}, first_row_id: {}", 
             snapshot.snapshot_id(), 
             snapshot.first_row_id());
}
```

---

## 四、❌ Rust SDK 不支持（需自行实现）的场景

### 4.1 行级血缘查询（Row Lineage）

**Java SDK 状态**：规划中，通过 `row_lineage` 系统表支持

**Rust SDK 状态**：❌ 完全不支持

**自行实现思路**：

```rust
// 需要自行实现：
// 1. 遍历目标行所在的所有快照（从创建到当前）
// 2. 在每个快照中定位该行的数据
// 3. 重建该行的变更历史

// 伪代码框架
async fn get_row_lineage(table: &Table, row_id: i64) -> Result<Vec<RowLineageEntry>> {
    let mut lineage = Vec::new();
    let snapshots = table.snapshots()?;
    
    for snapshot in snapshots {
        // 检查该快照是否包含此 row_id
        if snapshot.first_row_id() <= row_id {
            // 扫描该快照的数据，查找 row_id 对应的行
            let scan = table.scan()
                .use_snapshot(snapshot.snapshot_id())?
                .build()?;
            // ... 扫描并过滤 _row_id == row_id
            // ... 记录该行在此快照中的状态
        }
    }
    Ok(lineage)
}
```

### 4.2 基于 `_row_id` 的精确点查（Point Query）

**Rust SDK 状态**：❌ 无内置支持

**自行实现思路**：

```rust
// 需要自行实现基于 _row_id 的精确查找
// 核心挑战：_row_id 是元数据列，需要扫描所有数据文件定位

async fn query_by_row_id(table: &Table, row_id: i64) -> Result<Option<RecordBatch>> {
    // 方案1：扫描所有数据文件，过滤 _row_id
    let scan = table.scan().build()?;
    let tasks = scan.plan_files().await?;
    for task in tasks {
        // 读取数据文件，过滤 _row_id == row_id
        // 需要手动解析 Parquet 文件中的 _row_id 列
    }
    
    // 方案2：利用快照 first_row_id 缩小范围
    // 找到 first_row_id <= row_id 的快照
    // 只扫描这些快照对应的数据文件
    Ok(None)
}
```

### 4.3 删除向量（Deletion Vector）完整读写

**Rust SDK 状态**：⚠️ 仅定义了常量，完整 Puffin 删除向量支持有未解决的问题[](https://blog.javatask.dev/blog/iceberg-for-ot-part-3-v3-features/)

**自行实现思路**：需要完整实现 Puffin 文件的读写、RoaringBitmap 的序列化/反序列化、以及删除向量在扫描时的应用逻辑。

---

## 五、关于你的目标：使用 Rust Iceberg SDK 在 Iceberg Parquet 上建索引

### 5.1 Iceberg 原生索引现状

Iceberg 社区**正在讨论**二级索引（Secondary Index）的支持，但目前**没有任何 SDK（Java 或 Rust）原生支持在 Iceberg 表上构建索引**。

索引的构建、维护和查询（包括增量构建、索引回表等）都需要**自行实现**。

### 5.2 基于 ROWID 建索引的可行性

`_row_id` 是构建索引的理想基础，因为：

- 它是**稳定、唯一的行标识符**[](https://blog.javatask.dev/blog/iceberg-for-ot-part-3-v3-features/)

- 每个数据文件都有 `firstRowId` 元信息

- 新写入数据的 `_row_id` = `firstRowId + row_position`

### 5.3 索引构建架构建议

```textile
┌─────────────────────────────────────────────────────────────┐
│                    你的索引系统                             │
├─────────────────────────────────────────────────────────────┤
│  索引元数据管理（存储索引类型、版本、快照关联等）           │
├─────────────────────────────────────────────────────────────┤
│  增量索引构建 ← 利用 Rust SDK 的增量扫描 API               │
│  (table.scan().appends_after(last_snapshot_id))            │
├─────────────────────────────────────────────────────────────┤
│  全量索引构建 ← 扫描全表或历史快照                         │
├─────────────────────────────────────────────────────────────┤
│  索引查询（回表）← 通过 _row_id 定位数据行                 │
├─────────────────────────────────────────────────────────────┤
│  Rust Iceberg SDK（底层表操作、快照管理、数据扫描）         │
└─────────────────────────────────────────────────────────────┘
```

### 5.4 增量索引构建示例（基于 Rust SDK）

```rust
use iceberg::Table;
use iceberg_datafusion::IcebergStaticTableProvider;

/// 增量索引构建器
struct IncrementalIndexBuilder {
    table: Table,
    last_indexed_snapshot_id: i64,
}

impl IncrementalIndexBuilder {
    /// 从上次索引的快照之后构建增量索引
    async fn build_incremental_index(&mut self) -> Result<()> {
        // 1. 获取自上次索引以来的所有新增数据
        let scan = self.table
            .scan()
            .appends_after(self.last_indexed_snapshot_id)?
            .build()?;
        
        // 2. 扫描新增数据文件
        let tasks = scan.plan_files().await?;
        for task in tasks {
            // 3. 读取数据，提取索引字段 + _row_id
            let rows = read_task_data(&task).await?;
            for row in rows {
                let row_id = extract_row_id(&row)?;
                let index_key = extract_index_key(&row)?;
                // 4. 写入索引（如 RocksDB、Lucene 等）
                index_store.put(index_key, row_id)?;
            }
        }
        
        // 5. 更新检查点
        if let Some(snapshot) = self.table.current_snapshot() {
            self.last_indexed_snapshot_id = snapshot.snapshot_id();
        }
        Ok(())
    }
}
```

### 5.5 索引回表示例（通过 `_row_id` 定位数据）

```rust
/// 索引回表：通过索引找到 row_id，再读取完整数据
async fn index_lookup_and_fetch(
    table: &Table,
    index_store: &IndexStore,
    key: &str,
) -> Result<Option<RecordBatch>> {
    // 1. 从索引中查询 row_id
    let row_ids = index_store.get(key)?;
    
    // 2. 对于每个 row_id，需要定位到具体的数据文件和行位置
    //    挑战：_row_id 是逻辑 ID，需要映射到物理位置
    //    方案A：在索引中同时存储 (file_path, row_position)
    //    方案B：扫描数据文件过滤 _row_id（较慢）
    
    // 方案B 示例：
    let scan = table.scan().build()?;
    let tasks = scan.plan_files().await?;
    for task in tasks {
        // 读取数据文件，过滤 _row_id in row_ids
        // 需要实现 _row_id 列的过滤读取
    }
    
    Ok(None)
}
```

### 5.6 关键挑战与建议

| 挑战                | 说明                                                                                                          | 建议                                               |
| ----------------- | ----------------------------------------------------------------------------------------------------------- | ------------------------------------------------ |
| **`_row_id` 稳定性** | 压缩（`rewriteDataFiles`）可能重新分配 `_row_id`[](https://blog.javatask.dev/blog/iceberg-for-ot-part-3-v3-features/) | 索引需感知压缩操作，或采用 `(file_path, row_position)` 作为物理标识 |
| **索引与快照一致性**      | 新快照产生时需同步更新索引                                                                                               | 利用 `appends_after()` 实现增量索引更新                    |
| **回表性能**          | 通过 `_row_id` 回表需要扫描数据文件                                                                                     | 考虑在索引中存储物理位置 `(file_path, row_position)`         |
| **删除向量**          | 删除的行在 `_row_id` 空间中留下空洞                                                                                     | 索引需处理删除标记，或定期重建                                  |

### 5.7 技术选型建议

如果你的目标是**生产环境使用**：

1. **短期**：使用 **Java SDK + Spark/Flink** 构建索引，生态成熟，功能完整

2. **中期**：关注 Rust SDK 的增量扫描（已合并）和删除向量支持的进展

3. **长期**：Rust SDK 具有无 JVM 依赖、高性能的优势[](https://dev.to/alexmercedcoder/apache-data-lakehouse-weekly-march-10-17-2026-12fd)，适合构建高性能索引服务

如果你的团队**必须使用 Rust**：

- 基于 Rust SDK 0.9.0+ 自行实现索引层

- 利用已支持的增量扫描 API实现增量索引构建

- 索引存储可选用 RocksDB（Rust 绑定）或 Sled 等嵌入式 KV 存储

- 回表逻辑需要自行实现 `_row_id` → 物理位置的映射


