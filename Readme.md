# 注意
因项目需要使用hadoop catalog, 所以仅支持在`linux`环境使用。

# 编译执行
```shell
mvn clean compile

# 运行测试类
mvn exec:java -Dexec.mainClass="org.example.IcebergLocalDemo"
mvn exec:java -Dexec.mainClass="org.example.rowid.Example"

```

# 打包执行
```shell
mvn clean package

# 运行测试类
java -cp target/iceberg-study-1.0-SNAPSHOT.jar org.example.IcebergLocalDemo
```