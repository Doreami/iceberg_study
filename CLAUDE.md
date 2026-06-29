# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build & Run

```bash
# Compile
mvn clean compile

# Package (uber-jar via maven-shade-plugin)
mvn clean package

# Dev run (exec plugin: 末尾的 ShutdownHookManager 警告是 classloader 问题，不影响功能)
mvn exec:java -Dexec.mainClass="org.example.IcebergLocalDemo"
mvn exec:java -Dexec.mainClass="org.example.rowid.Example"

# 干净运行 (推荐, 无警告)
mvn clean package -q
java -cp target/iceberg-study-1.0-SNAPSHOT.jar org.example.IcebergLocalDemo
java -cp target/iceberg-study-1.0-SNAPSHOT.jar org.example.rowid.Example
```

Java 21 required. The project uses Apache Iceberg 1.11.0, Hadoop 3.3.4, and Parquet 1.12.3.

## Platform Note

This project uses Hadoop Catalog with local filesystem. Run in **WSL (Ubuntu-24.04) with JDK 21**. JDK 25+ is incompatible with Hadoop 3.3.4's `UserGroupInformation` (calls removed `Subject.getSubject()`). The warehouse path defaults to `/tmp/iceberg_learn` on Linux.

## Architecture Overview

This is a research/demo project exploring Apache Iceberg internals — custom index building and Row Lineage (RowID) features.

### Core flow

1. **`MyCatalog`** (singleton) — Manages a `HadoopCatalog` instance. All demo methods default to **V3 tables** (`format-version=3`) which auto-enable Row Lineage. Key methods:
   - `createTableExample(tid)` — creates V3 table, writes 100 test records (id 1–100)
   - `createTableExampleV3(tid)` — explicit V3 creator
   - `appendData(tid, startId, count)` — appends data, demonstrating row-id increment
   - Internal: `generateRecords()` + `writeAndAppend()` extract the duplicated write logic

2. **`Executor`** — Runs queries via `IcebergGenerics.read()`:
   - `search(tid)` / `searchById(tid, ids)` — basic scans
   - `searchWithRowLineage(tid)` — uses `MetadataColumns.schemaWithRowLineage()` to project `_row_id` + `_last_updated_sequence_number`
   - `searchByIdWithRowLineage(tid, ids)` — filtered scan with Row Lineage columns

3. **`rowid/Example`** — Comprehensive RowID / Row Lineage API demo (9 examples):
   - V3 table creation, snapshot-firstRowId inspection, per-file firstRowId
   - Reading `_row_id` metadata column via `schemaWithRowLineage()`
   - **Incremental scan**: `table.newIncrementalAppendScan().fromSnapshotExclusive(id).toSnapshot(id)` — CDC pattern
   - Row-id distribution across snapshots

4. **`IcebergLocalDemo`** — Original demo entry point, now V3-default

### Row Lineage key APIs (V3 tables only)

| API | Layer | Description |
|-----|-------|-------------|
| `Snapshot.firstRowId()` | Snapshot | First row-id assigned in this snapshot |
| `Snapshot.addedRows()` | Snapshot | Upper bound of rows with assigned row-ids |
| `DataFile.firstRowId()` | File | First row-id in a specific data file |
| `MetadataColumns.ROW_ID` | Constant | `_row_id` metadata column (Integer.MAX_VALUE-107) |
| `MetadataColumns.schemaWithRowLineage(schema)` | Util | Joins `_row_id` + `_last_updated_sequence_number` into schema |
| `table.newIncrementalAppendScan()` | Scan | Incremental scan for append-only changes (CDC) |
| `fromSnapshotExclusive(id)` / `toSnapshot(id)` | Scan | Bounding for incremental scans |

### Key Iceberg APIs used

- `HadoopCatalog` — file-system-backed catalog
- `IcebergGenerics.read()` — generic record reading with filter expressions
- `Puffin.write()` / `PuffinWriter` — Puffin statistics file format (deprecated in 1.11)
- `table.updateStatistics().setStatistics()` — attaching statistics/index files
- `Parquet.writeData()` with `GenericParquetWriter::create()` — writing Parquet via Iceberg's API
- `ParquetReader<Group>` — low-level Parquet reading outside Iceberg's read path
- `IncrementalAppendScan` — CDC/incremental scan between snapshots

### 1.11.0 API changes from older versions

- `GenericParquetWriter::buildWriter` → `GenericParquetWriter::create` (static method rename)
- Write path now uses `FormatModelRegistry`; old `createWriterFunc()` still works but is deprecated
- V3 tables auto-enable Row Lineage (no explicit `enableRowLineage` property needed)
- `Table` interface: no `formatVersion()` method — use `table.properties().get(TableProperties.FORMAT_VERSION)`

### Directory structure under warehouse

```
{warehouse}/
  mydb.db/
    user_table/
      data/          — Parquet data files
      metadata/      — Iceberg metadata (table metadata, manifest lists, manifest files)
      indices/       — Custom Puffin index files
```
