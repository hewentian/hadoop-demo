package com.hewentian.spark.util

import org.apache.spark.sql.SparkSession

object SparkUtil {
  var appName = "sparkTest"
  var master = "spark://hadoop-host-slave-3:7077"
  var jarPath = "/home/hewentian/ProjectD/gitHub/bigdata/codes/spark-demo/target/spark-1.0-SNAPSHOT.jar"

  // 在hdfs是高可用HA的情况下，端口是8020，非高可用是9000
  val hdfsUrl = "hdfs://hadoop-host-master:8020/"

  // jdbc连接相关信息
  val jdbcUrl = "jdbc:mysql://mysql.hewentian.com:3306/bfg_db?useUnicode=true&characterEncoding=utf-8&zeroDateTimeBehavior=convertToNull"
  val jdbcUser = "bfg_user"
  val jdbcPassword = "iE1zNB?A91*YbQ9hK"
  val jdbcDriver = "com.mysql.jdbc.Driver"

  def getSparkSession(): SparkSession = {
    val spark = SparkSession
      .builder()
      .appName(appName)
      .master(master) // 提交到集群运行的时候，注释此行。在IDEA下直接运行才需此配置
      .getOrCreate()

    spark.sparkContext.addJar(jarPath) // 提交到集群运行的时候，注释此行。在IDEA下直接运行才需此配置

    spark
  }
}
