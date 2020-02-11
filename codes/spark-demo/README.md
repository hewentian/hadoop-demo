有时候，直接在IDEA下运行报错，可能是代码没有编译到 jar 包中，在IDEA中将该项目重新编译后，再运行 mvn package，然后重新运行即可。
或者将代码打成jar包，直接使用如下方式运行：
./spark-2.4.4-bin-hadoop2.7/bin/spark-submit --class com.hewentian.spark.sql.SparkSQLExample --master spark://hadoop-host-slave-3:7077 /home/hadoop/spark-1.0-SNAPSHOT.jar

spark 操作hive需要将 hive-site.xml, core-site.xml 和 hdfs-site.xml 放到master节点的`{SPARK_HOME}/conf/`目录下即可，从节点可不放。重启spark集群。


spark访问hive
1. 有时候在IDEA运行无法访问指定的hive，可以尝试打成jar包放到命令行运行。

2. 在IDEA下直接运行，需将 hive-site.xml, core-site.xml 和 hdfs-site.xml放到src/main/resources目录下，我这里的hive-site.xml里面只放了存储元数据相关的信息；

3. 如果是打包成jar，使用spark-submit运行，是不需要将 hive-site.xml, core-site.xml 和 hdfs-site.xml放到src/main/resources目录下的

