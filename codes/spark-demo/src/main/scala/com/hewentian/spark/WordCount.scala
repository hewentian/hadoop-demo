package com.hewentian.spark

import com.hewentian.spark.util.SparkUtil

object WordCount {
  def main(args: Array[String]): Unit = {
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
