package com.hewentian.spark.sql

import com.hewentian.spark.util.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object SQLAggregationExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()

    val schema = StructType(List(
      StructField("name", StringType, nullable = false),
      StructField("salary", IntegerType, nullable = true)))

    val rowRDD = spark.sparkContext.parallelize(Seq(
      Row("Michael", 3000),
      Row("Andy", 4500),
      Row("Justin", 3500),
      Row("Berta", 4000),
      Row("Andy", 2880)
    ))

    val df = spark.createDataFrame(rowRDD, schema)

    df.createOrReplaceTempView("employees")
    df.show()
    // +-------+------+
    // |   name|salary|
    // +-------+------+
    // |Michael|  3000|
    // |   Andy|  4500|
    // | Justin|  3500|
    // |  Berta|  4000|
    // |   Andy|  2880|
    // +-------+------+

    df.select(count("name")).show()
    // +-----------+
    // |count(name)|
    // +-----------+
    // |          5|
    // +-----------+

    df.select(countDistinct("name")).show()
    // +--------------------+
    // |count(DISTINCT name)|
    // +--------------------+
    // |                   4|
    // +--------------------+

    df.select(approx_count_distinct("name", 0.1)).show()
    // +---------------------------+
    // |approx_count_distinct(name)|
    // +---------------------------+
    // |                          4|
    // +---------------------------+

    //    df.show()
    df.select(first("name"), last("salary")).show()
    // TODO: the result expect is below, but not. I don't know why.
    // +------------------+-------------------+
    // |first(name, false)|last(salary, false)|
    // +------------------+-------------------+
    // |           Michael|               2880|
    // +------------------+-------------------+

    df.select(min("salary"), max("salary")).show()
    // +-----------+-----------+
    // |min(salary)|max(salary)|
    // +-----------+-----------+
    // |       2880|       4500|
    // +-----------+-----------+

    df.select(sum("salary"), sumDistinct("salary")).show()
    // +-----------+--------------------+
    // |sum(salary)|sum(DISTINCT salary)|
    // +-----------+--------------------+
    // |      17880|               17880|
    // +-----------+--------------------+

    df.select(avg("salary")).show()
    // +-----------+
    // |avg(salary)|
    // +-----------+
    // |     3576.0|
    // +-----------+

    df.select(var_pop("salary"), var_samp(("salary")), stddev_pop("salary"), stddev_samp("salary")).show()
    // +---------------+----------------+------------------+-------------------+
    // |var_pop(salary)|var_samp(salary)|stddev_pop(salary)|stddev_samp(salary)|
    // +---------------+----------------+------------------+-------------------+
    // |       371104.0|        463880.0|  609.183059514954|  681.0873659083686|
    // +---------------+----------------+------------------+-------------------+

    df.agg(collect_set("name"), collect_list("salary")).show()
    // +--------------------+--------------------+
    // |   collect_set(name)|collect_list(salary)|
    // +--------------------+--------------------+
    // |[Andy, Michael, B...|[3000, 4500, 3500...|
    // +--------------------+--------------------+

    df.groupBy("name").count().show()
    // +-------+-----+
    // |   name|count|
    // +-------+-----+
    // |Michael|    1|
    // |   Andy|    2|
    // |  Berta|    1|
    // | Justin|    1|
    // +-------+-----+

    // the result same as below
    spark.sql("select name, count(1) from employees group by name").show()
    // +-------+--------+
    // |   name|count(1)|
    // +-------+--------+
    // |Michael|       1|
    // |   Andy|       2|
    // |  Berta|       1|
    // | Justin|       1|
    // +-------+--------+

    df.groupBy("name").agg(count("name").alias("total people"), sum("salary").alias("total salary")).show()
    // +-------+------------+------------+
    // |   name|total people|total salary|
    // +-------+------------+------------+
    // |Michael|           1|        3000|
    // |   Andy|           2|        7380|
    // |  Berta|           1|        4000|
    // | Justin|           1|        3500|
    // +-------+------------+------------+

    // the result same as below two queries
    df.groupBy("name").agg("name" -> "count", "salary" -> "sum").show()
    // +-------+-----------+-----------+
    // |   name|count(name)|sum(salary)|
    // +-------+-----------+-----------+
    // |Michael|          1|       3000|
    // |   Andy|          2|       7380|
    // |  Berta|          1|       4000|
    // | Justin|          1|       3500|
    // +-------+-----------+-----------+

    spark.sql("select name, count(1), sum(salary) from employees group by name").show()
    // +-------+--------+-----------+
    // |   name|count(1)|sum(salary)|
    // +-------+--------+-----------+
    // |Michael|       1|       3000|
    // |   Andy|       2|       7380|
    // |  Berta|       1|       4000|
    // | Justin|       1|       3500|
    // +-------+--------+-----------+

    spark.stop()
  }
}
