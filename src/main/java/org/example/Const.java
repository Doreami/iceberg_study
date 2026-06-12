package org.example;

public class Const {
    public static boolean isWindows() {
        return System.getProperty("os.name").toLowerCase().contains("win");
    }

    public static final String WARE_HOUSE_PATH;

    static {
        if (isWindows()) {
            // Windows 下请改为 "file:///D:/iceberg_learn" 等带盘符的路径
            WARE_HOUSE_PATH = "file:///D:/iceberg_learn";
        } else {
//            等价于 file:///tmp/iceberg_learn
            WARE_HOUSE_PATH = "/tmp/iceberg_learn";
        }
    }

    public static final String ID_NAME = "id";
}
