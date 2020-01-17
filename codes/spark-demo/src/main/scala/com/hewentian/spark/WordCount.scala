package com.hewentian.spark

import com.hewentian.spark.util.SparkUtil

object WordCount {
  def main(args: Array[String]): Unit = {
    // 设置访问hdfs的帐号，本机帐号可能没有hdfs的写权限
    // 当访问hdfs时，将按以下顺序获取访问hdfs的帐户，源码中commit()方法有详细的描述:
    // 1.System.getenv("HADOOP_USER_NAME")
    // 2.System.getProperty("HADOOP_USER_NAME")
    // 3.use the OS user
    // 这个设置必须是main方法的第一行代码，否则设置不生效
    System.setProperty("HADOOP_USER_NAME", "hadoop");

    val spark = SparkUtil.getSparkSession()

    // 指定本地文件路径，如果spark是集群模式，需要每个节点上对应路径下都要有此文件。或者使用hdfs
    // 加载本地文件，必须采用“file:///”开头的这种格式
    //    val file = spark.sparkContext.textFile("file://" + "/tmp/words.txt")

    // 从hdfs读文件，推荐使用此种方式
    val file = spark.sparkContext.textFile(SparkUtil.hdfsUrl + "words.txt")

    val wordCounts = file
      .flatMap(line => line.split(","))
      .map(word => (word, 1))
      .reduceByKey(_ + _)

    // 输出结果
    wordCounts.collect().foreach(x => println(x))

    //    wordCounts.saveAsTextFile("file://" + "/tmp/words_res")
    wordCounts.saveAsTextFile(SparkUtil.hdfsUrl + "words_res")

    spark.stop()
  }
}
