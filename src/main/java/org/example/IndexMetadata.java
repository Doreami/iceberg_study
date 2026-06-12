package org.example;

import org.apache.iceberg.StatisticsFile;
import org.apache.iceberg.Table;

import java.util.ArrayList;
import java.util.List;

public class IndexMetadata {
    public static StatisticsFile getMetadataStatisticFile(Table table) {
        long snapshotId = table.currentSnapshot().snapshotId();
        for (StatisticsFile statisticsFile : table.statisticsFiles()) {
            if (statisticsFile.snapshotId() != snapshotId) {
                continue;
            }

            if (statisticsFile.blobMetadata().get(0).type().equals("index_metadata")) {
                return statisticsFile;
            }
        }

        return null;
    }

    public static List<Object> getIndices(Table table) {
        StatisticsFile metadataStatisticFile = getMetadataStatisticFile(table);
        if (metadataStatisticFile == null) {
            return null;
        }

        // parse metadataStatisticFile
        return new ArrayList<>();
    }
}
