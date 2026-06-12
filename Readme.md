# 编译执行
```shell
mvn clean compile

# 运行主类
mvn exec:java -Dexec.mainClass="IcebergLocalDemo"
```

# 打包执行
```shell
mvn clean package

# 运行主类
java -cp target/iceberg-study-1.0-SNAPSHOT.jar IcebergLocalDemo
```