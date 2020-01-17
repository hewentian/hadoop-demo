package com.hewentian.spark

import com.hewentian.spark.util.SparkUtil

/**
  * 如果是在IDEA下直接运行，则要将mysql的jar包放到spark集群各个节点的 {SPARK_HOME}/jars 目录下
  * mv mysql-connector-java-5.1.25.jar /home/hadoop/spark-2.4.4-bin-hadoop2.7/jars/
  */
object JdbcTest {
  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", "hadoop");

    val spark = SparkUtil.getSparkSession()

    val sqlContext = spark.sqlContext

    // Creates a DataFrame based on a table named "student" stored in a MySQL database.

    val df = sqlContext
      .read
      .format("jdbc")
      .option("url", SparkUtil.jdbcUrl)
      .option("driver", SparkUtil.jdbcDriver)
      .option("user", SparkUtil.jdbcUser)
      .option("password", SparkUtil.jdbcPassword)
      .option("dbtable", "student")
      .load()

    // Looks the schema of this DataFrame.
    df.printSchema()

    // Counts student by age
    val countsByAge = df.groupBy("age").count()
    countsByAge.show()

    // Saves countsByAge to hdfs in the JSON format.
    countsByAge.write.format("json").save(SparkUtil.hdfsUrl + "countsByAge")

    spark.stop()
  }
}
