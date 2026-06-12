package org.example;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.fs.Path;
import org.apache.iceberg.*;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.CloseableIterable;
import org.apache.iceberg.io.FileIO;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Blob;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetReader;
import org.apache.parquet.hadoop.example.GroupReadSupport;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;

import java.nio.ByteBuffer;
import java.util.*;

public class IndexCommander {
    private static final Catalog catalog = MyCatalog.INSTANCE.getCatalog();
    private static final ObjectMapper objectMapper = new ObjectMapper();

    /**
     * 读取 Parquet 文件，返回每个 id 对应的所有行号列表。
     * 行号从 0 开始，表示该行在文件中的顺序位置。
     */
    private static Map<Long, List<Long>> readIdsWithRowNumbers(String filePath,
                                                               Configuration hadoopConf) throws IOException {
        Map<Long, List<Long>> result = new HashMap<>();
        Path path = new Path(filePath);
        long rowNumber = 0;
        try (ParquetReader<Group> reader = ParquetReader.builder(new GroupReadSupport(), path)
                .withConf(hadoopConf)
                .build()) {
            Group group;
            while ((group = reader.read()) != null) {
                // 假设 id 列名称为 "id"，类型为 long
                long uid = group.getLong(Const.ID_NAME, 0);
                result.computeIfAbsent(uid, k -> new ArrayList<>()).add(rowNumber);
                rowNumber++;
            }
        }
        return result;
    }

    public static void createIndex(TableIdentifier tableId) throws IOException {
        // 1. load table
        Table table = catalog.loadTable(tableId);
        Snapshot snapshot = table.currentSnapshot();

        // 2. 固定为id列创建puffin文件索引
        int id = table.schema().findField(Const.ID_NAME).fieldId();

        // 3. 获取所有parquet文件
        List<DataFile> dataFiles = new ArrayList<>();
        try (CloseableIterable<FileScanTask> tasks = table.newScan().planFiles()) {
            for (FileScanTask task : tasks) {
                dataFiles.add(task.file());
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        // 4. 遍历文件获取id到文件路径的映射
        Map<Long, List<FileOffset>> index = new HashMap<>();
        int fileIdx = 0;
        Configuration hadoopConf = new Configuration();
        for (DataFile dataFile : dataFiles) {
            String filePath = dataFile.path().toString();
            // 读取该文件中每条记录的 id 及其行号
            Map<Long, List<Long>> rowsPerId = readIdsWithRowNumbers(filePath, hadoopConf);
            for (Map.Entry<Long, List<Long>> entry : rowsPerId.entrySet()) {
                Long uid = entry.getKey();
                List<Long> rowNumbers = entry.getValue();
                // 将每个行号作为一个独立的 FileOffset 记录（也可以合并为一个 FileOffset 包含多个行号，但为了回表演示保持简单）
                for (Long rowNum : rowNumbers) {
                    index.computeIfAbsent(uid, k -> new ArrayList<>())
                            .add(new FileOffset(filePath, rowNum));
                }
            }
//            System.out.printf("处理文件 %d/%d: %s，发现 %d 个唯一 id%n",
//                    ++fileIdx, dataFiles.size(), filePath, rowsPerId.size());
        }

        // 获取序列化btree blob
        byte[] indexBytes;
        try {
            indexBytes = objectMapper.writeValueAsBytes(index);
        } catch (JsonProcessingException e) {
            throw new IOException("Failed to serialize index to JSON", e);
        }
        ByteBuffer blobData = ByteBuffer.wrap(indexBytes);
        Blob blob = new Blob(
                "btree-index-id-" + table.name(),
                Collections.singletonList(id),
                snapshot.snapshotId(),
                -1L,
                blobData
        );

        // 写puffin文件
        String puffinFilePath = String.format("%s/%s/%s/btree-index-id-%s.puffin",
                Const.WARE_HOUSE_PATH.replace("file:/", ""),
                table.location(),
                "indices",
                snapshot.snapshotId());
        FileIO fileIO = table.io();
        OutputFile outputFile = fileIO.newOutputFile(puffinFilePath);

        PuffinWriter writer = Puffin.write(outputFile)
                .createdBy("BTreeIndexWithRowNumber/1.0")
                .compressBlobs(PuffinCompressionCodec.NONE)
                .build();
        writer.add(blob);
        writer.finish();

        // 8. 关联统计文件到表
        List<org.apache.iceberg.BlobMetadata> statsBlobs = new ArrayList<>();
        for (org.apache.iceberg.puffin.BlobMetadata puffinBlobMeta : writer.writtenBlobsMetadata()) {
            org.apache.iceberg.BlobMetadata statsBlobMeta = new GenericBlobMetadata(
                    puffinBlobMeta.type(),
                    snapshot.snapshotId(),             // sourceSnapshotId
                    snapshot.sequenceNumber(),         // sourceSnapshotSequenceNumber
                    puffinBlobMeta.inputFields(),
                    puffinBlobMeta.properties()
            );
            statsBlobs.add(statsBlobMeta);
        }

        StatisticsFile statsFile = new GenericStatisticsFile(
                snapshot.snapshotId(),
                puffinFilePath,
                outputFile.toInputFile().getLength(),
                -1L,
                statsBlobs
        );
        table.updateStatistics().setStatistics(snapshot.snapshotId(), statsFile).commit();

        System.out.println("创建索引成功");
    }
}
