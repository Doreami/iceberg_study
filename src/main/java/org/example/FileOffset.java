package org.example;

/* parquet 文件行号映射 */
public class FileOffset {
    public String path;
    public long rowNumber;

    public FileOffset() {} // for Jackson
    public FileOffset(String path, long rowNumber) {
        this.path = path;
        this.rowNumber = rowNumber;
    }
}