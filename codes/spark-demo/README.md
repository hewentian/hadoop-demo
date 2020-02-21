有时候，直接在IDEA下运行报错，可能是代码没有编译到 jar 包中，在IDEA中将该项目重新编译后，再运行 mvn package，然后重新运行即可。
或者将代码打成jar包，直接使用如下方式运行：
./spark-2.4.4-bin-hadoop2.7/bin/spark-submit --class com.hewentian.spark.sql.SparkSQLExample --master spark://hadoop-host-slave-3:7077 /home/hadoop/spark-1.0-SNAPSHOT.jar

spark 操作hive需要将 hive-site.xml, core-site.xml 和 hdfs-site.xml 放到master节点的`{SPARK_HOME}/conf/`目录下即可，从节点可不放。重启spark集群。


spark访问hive
1. 有时候在IDEA运行无法访问指定的hive，可以尝试打成jar包放到命令行运行。

2. 在IDEA下直接运行，需将 hive-site.xml, core-site.xml 和 hdfs-site.xml放到src/main/resources目录下，我这里的hive-site.xml里面只放了存储元数据相关的信息；

3. 如果是打包成jar，使用spark-submit运行，是不需要将 hive-site.xml, core-site.xml 和 hdfs-site.xml放到src/main/resources目录下的


如果Spark Streaming监控HDFS目录失败，问题的原因可能是：虚拟机的时间和物理机的时间是不同步的，导致物理机的IDEA的Spark Streaming监测不到虚拟机HDFS目录的数据。

{HADOOP_HOME}/bin/hdfs dfs -put {HADOOP_HOME}/README.txt /spark/streaming
用put，可能会产生 File does not exist: /spark/streaming/NOTICE.txt._COPYING_

https://issues.apache.org/jira/browse/SPARK-4314

[Reason]
Intermediate file 200._COPYING_ is found by FileInputDStream interface, and exception throws when NewHadoopRDD
ready to handle non-existent 200._COPYING_ file because file 200._COPYING_ is changed to file 200 when upload
is finished

maji2014 maji2014 added a comment - 26/Nov/14 01:56 - edited
Discription for fileStream in textFileStream method is "HDFS directory to monitor for new file". To reproduce
this scenario, upload the file into the monitoring hdfs directory through command like "hadoop fs -put filename /user/".
do you think "hadoop fs -put filename /user/" is the wrong way? the first time, the upload filename is in intermediate
partly written state "filename._COPYING_", and then the filename is changed when upload complete. But the intermediate
partly is caught by spark. then the scenario appears. of course, you can also refer to conversation in the pull request.
