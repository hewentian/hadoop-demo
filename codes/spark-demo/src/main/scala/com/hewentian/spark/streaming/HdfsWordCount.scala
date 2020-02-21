/**
  * copy from
  * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/streaming/HdfsWordCount.scala
  */

// scalastyle:off println
package com.hewentian.spark.streaming

import com.hewentian.spark.util.SparkUtil
import org.apache.spark.SparkConf
import org.apache.spark.streaming.{Seconds, StreamingContext}

/**
  * Counts words in new text files created in the given directory
  * Usage: HdfsWordCount <directory>
  *   <directory> is the directory that Spark Streaming will use to find and read new text files.
  *
  * To run this on your local machine on directory `localdir`, run this example
  *    $ bin/run-example \
  *       org.apache.spark.examples.streaming.HdfsWordCount localdir
  *
  * Then create a text file in `localdir` and the words in the file will get counted.
  */
object HdfsWordCount {
  def main(args: Array[String]): Unit = {
    if (args.length < 1) {
      System.err.println("Usage: HdfsWordCount <directory>")
      //      System.exit(1)
    }

    System.setProperty("HADOOP_USER_NAME", SparkUtil.hdfsUser)
    StreamingExamples.setStreamingLogLevels()

    //    val sparkConf = new SparkConf().setAppName("HdfsWordCount")
    val sparkConf = SparkUtil.getSparkConf()
    // Create the context
    val ssc = new StreamingContext(sparkConf, Seconds(2))

    // Create the FileInputDStream on the directory and use the
    // stream to count words in new files created
    //    val lines = ssc.textFileStream(args(0))
    val lines = ssc.textFileStream(SparkUtil.hdfsUrl + "streaming")
    val words = lines.flatMap(_.split(" "))
    val wordCounts = words.map(x => (x, 1)).reduceByKey(_ + _)
    wordCounts.print()
    ssc.start()
    ssc.awaitTermination()

    // {HADOOP_HOME}/bin/hdfs dfs -mkdir /spark/streaming
    // after run this program, then use the follow cmd
    // {HADOOP_HOME}/bin/hdfs dfs -put {HADOOP_HOME}/README.txt /spark/
    // {HADOOP_HOME}/bin/hdfs dfs -mv /spark/README.txt /spark/streaming
    // TODO: 如果还遇到问题，请看 README.md
  }
}

// scalastyle:on println
