package com.hewentian.spark

import com.hewentian.spark.util.SparkUtil

import org.apache.spark.sql.Column

object TextSearch {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()

    val sqlContext = spark.sqlContext
    import sqlContext.implicits._

    val textFile = spark.sparkContext.textFile(SparkUtil.hdfsUrl + "mysql.log")

    var colName = "line";
    var column = new Column(colName);

    // Creates a DataFrame having a single column named "line"
    val df = textFile.toDF(colName)
    var errors = df.filter(column.like("%ERROR%"))

    // Counts all the errors
    println("all the errors: " + errors.count())

    errors = errors.filter(column.like("%MySQL%"))

    // Counts errors mentioning MySQL
    println("errors mentioning MySQL: " + errors.count())

    // Fetches the MySQL errors as an array of strings
    errors.collect().foreach(x => println(x))

    spark.stop()
  }
}
