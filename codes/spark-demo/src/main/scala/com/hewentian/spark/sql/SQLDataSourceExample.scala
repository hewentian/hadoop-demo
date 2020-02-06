/**
  * copy from
  * https://github.com/apache/spark/blob/master/examples/src/main/scala/org/apache/spark/examples/sql/SQLDataSourceExample.scala
  */

package com.hewentian.spark.sql

import java.util.Properties

import com.hewentian.spark.util.SparkUtil
import org.apache.spark.sql.SparkSession

object SQLDataSourceExample {

  def main(args: Array[String]): Unit = {
    System.setProperty("HADOOP_USER_NAME", SparkUtil.hdfsUser);

    val spark = SparkUtil.getSparkSession()

    runBasicDataSourceExample(spark)
    runBasicParquetExample(spark)
    runParquetSchemaMergingExample(spark)
    runJsonDatasetExample(spark)
    runJdbcDatasetExample(spark)

    spark.stop()
  }

  private def runBasicDataSourceExample(spark: SparkSession): Unit = {
    // $example on:generic_load_save_functions$
    //    val usersDF = spark.read.load("examples/src/main/resources/users.parquet")
    //    usersDF.select("name", "favorite_color").write.save("namesAndFavColors.parquet")
    val usersDF = spark.read.load(SparkUtil.hdfsUrl + "users.parquet")
    usersDF.show()
    usersDF.select("name", "favorite_color").write.save(SparkUtil.hdfsUrl + "namesAndFavColors.parquet")
    // $example off:generic_load_save_functions$

    // $example on:manual_load_options$
    //    val peopleDF = spark.read.format("json").load("examples/src/main/resources/people.json")
    //    peopleDF.select("name", "age").write.format("parquet").save("namesAndAges.parquet")
    val peopleDF = spark.read.format("json").load(SparkUtil.hdfsUrl + "people.json")
    peopleDF.select("name", "age").write.format("parquet").save(SparkUtil.hdfsUrl + "namesAndAges.parquet")
    // $example off:manual_load_options$

    // $example on:manual_load_options_csv$
    val peopleDFCsv = spark.read.format("csv")
      .option("sep", ";")
      .option("inferSchema", "true")
      .option("header", "true")
      //      .load("examples/src/main/resources/people.csv")
      .load(SparkUtil.hdfsUrl + "people.csv")
    peopleDFCsv.show()
    // $example off:manual_load_options_csv$

    // $example on:load_with_path_glob_filter$
    val partitionedUsersDF = spark.read.format("orc")
      .option("pathGlobFilter", "*.orc")
      //      .load("examples/src/main/resources/partitioned_users.orc")
      .load(SparkUtil.hdfsUrl + "users.orc")
    partitionedUsersDF.show()
    // $example off:load_with_path_glob_filter$

    // $example on:manual_save_options_orc$
    usersDF.write.format("orc")
      .option("orc.bloom.filter.columns", "favorite_color")
      .option("orc.dictionary.key.threshold", "1.0")
      .option("orc.column.encoding.direct", "name")
      .save(SparkUtil.hdfsUrl + "users_with_options.orc")
    // $example off:manual_save_options_orc$

    // $example on:direct_sql$
    //    val sqlDF = spark.sql("SELECT * FROM parquet.`examples/src/main/resources/users.parquet`")
    val sqlDF = spark.sql("SELECT * FROM parquet.`" + SparkUtil.hdfsUrl + "users.parquet`")
    sqlDF.show()
    // $example off:direct_sql$

    // $example on:write_sorting_and_bucketing$
    peopleDF.write.bucketBy(42, "name").sortBy("age").saveAsTable("people_bucketed")
    // $example off:write_sorting_and_bucketing$

    // $example on:write_partitioning$
    //    usersDF.write.partitionBy("favorite_color").format("parquet").save("namesPartByColor.parquet")
    usersDF.write.partitionBy("favorite_color").format("parquet").save(SparkUtil.hdfsUrl + "namesPartByColor.parquet")
    // $example off:write_partitioning$

    // $example on:write_partition_and_bucket$
    usersDF
      .write
      .partitionBy("favorite_color")
      .bucketBy(42, "name")
      .saveAsTable("users_partitioned_bucketed")
    // $example off:write_partition_and_bucket$

    spark.sql("DROP TABLE IF EXISTS people_bucketed")
    spark.sql("DROP TABLE IF EXISTS users_partitioned_bucketed")
  }

  private def runBasicParquetExample(spark: SparkSession): Unit = {
    // $example on:basic_parquet_example$
    // Encoders for most common types are automatically provided by importing spark.implicits._
    import spark.implicits._

    //    val peopleDF = spark.read.json("examples/src/main/resources/people.json")
    val peopleDF = spark.read.json(SparkUtil.hdfsUrl + "people.json")

    // DataFrames can be saved as Parquet files, maintaining the schema information
    //    peopleDF.write.parquet("people.parquet")
    peopleDF.write.parquet(SparkUtil.hdfsUrl + "people.parquet")

    // Read in the parquet file created above
    // Parquet files are self-describing so the schema is preserved
    // The result of loading a Parquet file is also a DataFrame
    //    val parquetFileDF = spark.read.parquet("people.parquet")
    val parquetFileDF = spark.read.parquet(SparkUtil.hdfsUrl + "people.parquet")

    // Parquet files can also be used to create a temporary view and then used in SQL statements
    parquetFileDF.createOrReplaceTempView("parquetFile")
    val namesDF = spark.sql("SELECT name FROM parquetFile WHERE age BETWEEN 13 AND 19")
    namesDF.map(attributes => "Name: " + attributes(0)).show()
    // +------------+
    // |       value|
    // +------------+
    // |Name: Justin|
    // +------------+
    // $example off:basic_parquet_example$
  }

  private def runParquetSchemaMergingExample(spark: SparkSession): Unit = {
    // $example on:schema_merging$
    // This is used to implicitly convert an RDD to a DataFrame.
    import spark.implicits._

    // Create a simple DataFrame, store into a partition directory
    val squaresDF = spark.sparkContext.makeRDD(1 to 5).map(i => (i, i * i)).toDF("value", "square")
    //    squaresDF.write.parquet("data/test_table/key=1")
    squaresDF.write.parquet(SparkUtil.hdfsUrl + "data/test_table/key=1")

    // Create another DataFrame in a new partition directory,
    // adding a new column and dropping an existing column
    val cubesDF = spark.sparkContext.makeRDD(6 to 10).map(i => (i, i * i * i)).toDF("value", "cube")
    //    cubesDF.write.parquet("data/test_table/key=2")
    cubesDF.write.parquet(SparkUtil.hdfsUrl + "data/test_table/key=2")

    // Read the partitioned table
    //    val mergedDF = spark.read.option("mergeSchema", "true").parquet("data/test_table")
    val mergedDF = spark.read.option("mergeSchema", "true").parquet(SparkUtil.hdfsUrl + "data/test_table")
    mergedDF.printSchema()

    // The final schema consists of all 3 columns in the Parquet files together
    // with the partitioning column appeared in the partition directory paths
    // root
    //  |-- value: int (nullable = true)
    //  |-- square: int (nullable = true)
    //  |-- cube: int (nullable = true)
    //  |-- key: int (nullable = true)
    // $example off:schema_merging$
  }

  private def runJsonDatasetExample(spark: SparkSession): Unit = {
    // $example on:json_dataset$
    // Primitive types (Int, String, etc) and Product types (case classes) encoders are
    // supported by importing this when creating a Dataset.
    import spark.implicits._

    // A JSON dataset is pointed to by path.
    // The path can be either a single text file or a directory storing text files
    //    val path = "examples/src/main/resources/people.json"
    val path = SparkUtil.hdfsUrl + "people.json"
    val peopleDF = spark.read.json(path)

    // The inferred schema can be visualized using the printSchema() method
    peopleDF.printSchema()
    // root
    //  |-- age: long (nullable = true)
    //  |-- name: string (nullable = true)

    // Creates a temporary view using the DataFrame
    peopleDF.createOrReplaceTempView("people")

    // SQL statements can be run by using the sql methods provided by spark
    val teenagerNamesDF = spark.sql("SELECT name FROM people WHERE age BETWEEN 13 AND 19")
    teenagerNamesDF.show()
    // +------+
    // |  name|
    // +------+
    // |Justin|
    // +------+

    // Alternatively, a DataFrame can be created for a JSON dataset represented by
    // a Dataset[String] storing one JSON object per string
    val otherPeopleDataset = spark.createDataset(
      """{"name":"Yin","address":{"city":"Columbus","state":"Ohio"}}""" :: Nil)
    val otherPeople = spark.read.json(otherPeopleDataset)
    otherPeople.show()
    // +---------------+----+
    // |        address|name|
    // +---------------+----+
    // |[Columbus,Ohio]| Yin|
    // +---------------+----+
    // $example off:json_dataset$
  }

  private def runJdbcDatasetExample(spark: SparkSession): Unit = {
    // $example on:jdbc_dataset$
    // Note: JDBC loading and saving can be achieved via either the load/save or jdbc methods
    // Loading data from a JDBC source
    val jdbcDF = spark.read
      .format("jdbc")
      .option("url", SparkUtil.jdbcUrl)
      .option("driver", SparkUtil.jdbcDriver)
      .option("user", SparkUtil.jdbcUser)
      .option("password", SparkUtil.jdbcPassword)
      .option("dbtable", "student")
      .load()
    jdbcDF.show()

    val connectionProperties = new Properties()
    connectionProperties.put("user", SparkUtil.jdbcUser)
    connectionProperties.put("password", SparkUtil.jdbcPassword)
    val jdbcDF2 = spark.read
      .jdbc(SparkUtil.jdbcUrl, "student", connectionProperties)
    jdbcDF2.show()

    // Specifying the custom data types of the read schema
    connectionProperties.put("customSchema", "sex INTEGER, sname STRING")
    val jdbcDF3 = spark.read
      .jdbc(SparkUtil.jdbcUrl, "student", connectionProperties)
    jdbcDF3.show()

    // Saving data to a JDBC source
    jdbcDF.write
      .format("jdbc")
      .option("url", SparkUtil.jdbcUrl)
      .option("driver", SparkUtil.jdbcDriver)
      .option("user", SparkUtil.jdbcUser)
      .option("password", SparkUtil.jdbcPassword)
      .option("dbtable", "student1")
      .save()

    jdbcDF2.write
      .jdbc(SparkUtil.jdbcUrl, "student2", connectionProperties)

    // Specifying create table column data types on write
    jdbcDF.write
      .option("createTableColumnTypes", "sname CHAR(64), sex VARCHAR(2)")
      .jdbc(SparkUtil.jdbcUrl, "student3", connectionProperties)
    // $example off:jdbc_dataset$
  }
}
