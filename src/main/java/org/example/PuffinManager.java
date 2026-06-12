package org.example;

import org.apache.iceberg.Files;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.io.OutputFile;
import org.apache.iceberg.puffin.Puffin;
import org.apache.iceberg.puffin.PuffinCompressionCodec;
import org.apache.iceberg.puffin.PuffinWriter;

public class PuffinManager {
    private final static Catalog catalog = MyCatalog.getCatalog();

    // puffin
//    private void writePuffin(TableIdentifier tableId) {
//        Table table = catalog.loadTable(tableId);
//        Schema schema = table.schema();
//        // 固定为id列创建puffin文件索引
//        int id = schema.findField(Const.ID_NAME).fieldId();
//
//        table.currentSnapshot().dataManifests()
//
//        String puffinFilePath = "";
//        OutputFile outputFile = Files.localOutput(puffinFilePath);
//        PuffinWriter writer = Puffin.write(outputFile)
//                .createdBy("自定义索引实现demo")       // 标识创建者，便于追溯
//                .compressBlobs(PuffinCompressionCodec.NONE)
//                .set("description", "索引metadata.puffin")
//                .build();
//    }
}
