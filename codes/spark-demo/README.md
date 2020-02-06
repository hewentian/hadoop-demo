有时候，直接在IDEA下运行报错，可能是代码没有编译到 jar 包中，在IDEA中将该项目重新编译后，再运行 mvn package，然后重新运行即可。
或者将代码打成jar包，直接使用如下方式运行：
./spark-2.4.4-bin-hadoop2.7/bin/spark-submit --class com.hewentian.spark.sql.SparkSQLExample --master spark://hadoop-host-slave-3:7077 /home/hadoop/spark-1.0-SNAPSHOT.jar

