package com.hewentian.spark.util

import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object SparkUtil {
  val ideaRun = true // 在IDEA下直接跑，或打成jar包在远程使用 {SPARK_HOME}/bin/spark-submit 运行

  val appName = "sparkTest"
  val master = "spark://hadoop-host-slave-3:7077"
  val jarPath = "/home/hewentian/ProjectD/gitHub/bigdata/codes/spark-demo/target/spark-1.0-SNAPSHOT.jar"

  // hdfs是高可用HA & 非HA，都可以使用hdfsBaseUrl
  // 如果hdfs是HA，请直接使用hdfsBaseUrlHa

  // 在hdfs是HA的情况下，端口是8020，非高可用是9000
  // 不需 hdfs-site.xml 和 core-site.xml 在src/main/resources目录下
  val hdfsBaseUrl = "hdfs://hadoop-host-master:8020/"

  // 需 hdfs-site.xml 和 core-site.xml 在src/main/resources目录下
  val hdfsBaseUrlHa = "hdfs://hadoop-cluster-ha/"

  val hdfsUrl = hdfsBaseUrlHa + "spark/"

  val hdfsUser = "hadoop"

  val checkpoint = hdfsUrl + "checkpoint"

  // jdbc连接相关信息
  val jdbcUrl = "jdbc:mysql://mysql.hewentian.com:3306/bfg_db?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull"
  val jdbcUser = "bfg_user"
  val jdbcPassword = "iE1zNB?A91*YbQ9hK"
  val jdbcDriver = "com.mysql.jdbc.Driver"

  val kafkaBrokers = "hadoop-host-master:9092,hadoop-host-slave-1:9092,hadoop-host-slave-2:9092"

  def getSparkSession(): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)

    if (ideaRun) {
      builder.master(master) // 提交到集群运行的时候，注释此行。在IDEA下直接运行才需此配置
    }

    val spark = builder.getOrCreate()

    if (ideaRun) {
      spark.sparkContext.addJar(jarPath) // 提交到集群运行的时候，注释此行。在IDEA下直接运行才需此配置
    }

    spark
  }

  def getSparkSession(isHiveSupport: Boolean = false): SparkSession = {
    val builder = SparkSession
      .builder()
      .appName(appName)

    if (ideaRun) {
      builder.master(master) // 提交到集群运行的时候，注释此行。在IDEA下直接运行才需此配置
    }

    if (isHiveSupport) {
      // spark.sql.warehouse.dir points to the default location for managed databases and tables
      builder.config("spark.sql.warehouse.dir", SparkUtil.hdfsBaseUrlHa + "user/hive/warehouse")
      builder.enableHiveSupport()
    }

    val spark = builder.getOrCreate()

    if (ideaRun) {
      spark.sparkContext.addJar(jarPath) // 提交到集群运行的时候，注释此行。在IDEA下直接运行才需此配置
    }

    spark
  }

  def getSparkConf(): SparkConf = {
    val sparkConf = new SparkConf().setAppName(appName)

    if (ideaRun) {
      sparkConf.setMaster(master)
      sparkConf.setJars(Seq(jarPath))
    }

    sparkConf
  }
}
