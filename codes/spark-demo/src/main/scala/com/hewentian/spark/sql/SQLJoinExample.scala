package com.hewentian.spark.sql

import com.hewentian.spark.util.SparkUtil
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

object SQLJoinExample {
  def main(args: Array[String]): Unit = {
    val spark = SparkUtil.getSparkSession()

    val deptSchema = StructType(List(
      StructField("dept_no", IntegerType, nullable = true),
      StructField("dept_name", StringType, nullable = true)))

    val empSchema = StructType(List(
      StructField("emp_no", IntegerType, nullable = true),
      StructField("emp_name", StringType, nullable = true),
      StructField("dept_no", IntegerType, nullable = true),
      StructField("job", StringType, nullable = true),
      StructField("salary", IntegerType, nullable = true)))

    val deptRDD = spark.sparkContext.parallelize(Seq(
      Row(1, "International Department"),
      Row(2, "Personnel Department"),
      Row(3, "Technology Department"),
      Row(4, "Human Resource Department")
    ))

    val empRDD = spark.sparkContext.parallelize(Seq(
      Row(1, "Michael", 1, "translator", 3000),
      Row(2, "Andy", 2, "wanted", 4500),
      Row(3, "Justin", 2, "wanted", 3500),
      Row(4, "Berta", 3, "java", 4000),
      Row(5, "Ken", 8, "java", 2880)
    ))

    val deptDF = spark.createDataFrame(deptRDD, deptSchema)
    deptDF.createOrReplaceTempView("dept")
    deptDF.show()
    // +-------+--------------------+
    // |dept_no|           dept_name|
    // +-------+--------------------+
    // |      1|International Dep...|
    // |      2|Personnel Department|
    // |      3|Technology Depart...|
    // |      4|Human Resource De...|
    // +-------+--------------------+

    val empDF = spark.createDataFrame(empRDD, empSchema)
    empDF.createOrReplaceTempView("emp")
    empDF.show()
    // +------+--------+-------+----------+------+
    // |emp_no|emp_name|dept_no|       job|salary|
    // +------+--------+-------+----------+------+
    // |     1| Michael|      1|translator|  3000|
    // |     2|    Andy|      2|    wanted|  4500|
    // |     3|  Justin|      2|    wanted|  3500|
    // |     4|   Berta|      3|      java|  4000|
    // |     5|     Ken|      8|      java|  2880|
    // +------+--------+-------+----------+------+

    val joinExpression = empDF.col("dept_no") === deptDF.col("dept_no")

    // INNER JOIN, default is inner join
    empDF.join(deptDF, joinExpression).select("emp_name", "dept_name").show()
    // same as
    spark.sql("SELECT emp_name, dept_name FROM emp JOIN dept ON emp.dept_no = dept.dept_no").show()
    // +--------+--------------------+
    // |emp_name|           dept_name|
    // +--------+--------------------+
    // | Michael|International Dep...|
    // |   Berta|Technology Depart...|
    // |  Justin|Personnel Department|
    // |    Andy|Personnel Department|
    // +--------+--------------------+

    // FULL OUTER JOIN
    empDF.join(deptDF, joinExpression, "outer").show()
    // same as
    spark.sql("SELECT * FROM emp FULL OUTER JOIN dept ON emp.dept_no = dept.dept_no").show()
    // +------+--------+-------+----------+------+-------+--------------------+
    // |emp_no|emp_name|dept_no|       job|salary|dept_no|           dept_name|
    // +------+--------+-------+----------+------+-------+--------------------+
    // |     1| Michael|      1|translator|  3000|      1|International Dep...|
    // |     4|   Berta|      3|      java|  4000|      3|Technology Depart...|
    // |  null|    null|   null|      null|  null|      4|Human Resource De...|
    // |     5|     Ken|      8|      java|  2880|   null|                null|
    // |     3|  Justin|      2|    wanted|  3500|      2|Personnel Department|
    // |     2|    Andy|      2|    wanted|  4500|      2|Personnel Department|
    // +------+--------+-------+----------+------+-------+--------------------+

    // LEFT OUTER JOIN
    empDF.join(deptDF, joinExpression, "left_outer").show()
    // same as
    spark.sql("SELECT * FROM emp LEFT OUTER JOIN dept ON emp.dept_no = dept.dept_no").show()
    // +------+--------+-------+----------+------+-------+--------------------+
    // |emp_no|emp_name|dept_no|       job|salary|dept_no|           dept_name|
    // +------+--------+-------+----------+------+-------+--------------------+
    // |     1| Michael|      1|translator|  3000|      1|International Dep...|
    // |     4|   Berta|      3|      java|  4000|      3|Technology Depart...|
    // |     5|     Ken|      8|      java|  2880|   null|                null|
    // |     2|    Andy|      2|    wanted|  4500|      2|Personnel Department|
    // |     3|  Justin|      2|    wanted|  3500|      2|Personnel Department|
    // +------+--------+-------+----------+------+-------+--------------------+

    // RIGHT OUTER JOIN
    empDF.join(deptDF, joinExpression, "right_outer").show()
    // same as
    spark.sql("SELECT * FROM emp RIGHT OUTER JOIN dept ON emp.dept_no = dept.dept_no").show()
    // +------+--------+-------+----------+------+-------+--------------------+
    // |emp_no|emp_name|dept_no|       job|salary|dept_no|           dept_name|
    // +------+--------+-------+----------+------+-------+--------------------+
    // |     1| Michael|      1|translator|  3000|      1|International Dep...|
    // |     4|   Berta|      3|      java|  4000|      3|Technology Depart...|
    // |  null|    null|   null|      null|  null|      4|Human Resource De...|
    // |     2|    Andy|      2|    wanted|  4500|      2|Personnel Department|
    // |     3|  Justin|      2|    wanted|  3500|      2|Personnel Department|
    // +------+--------+-------+----------+------+-------+--------------------+

    // LEFT SEMI JOIN
    empDF.join(deptDF, joinExpression, "left_semi").show()
    // same as
    spark.sql("SELECT * FROM emp LEFT SEMI JOIN dept ON emp.dept_no = dept.dept_no").show()
    // +------+--------+-------+----------+------+
    // |emp_no|emp_name|dept_no|       job|salary|
    // +------+--------+-------+----------+------+
    // |     1| Michael|      1|translator|  3000|
    // |     4|   Berta|      3|      java|  4000|
    // |     2|    Andy|      2|    wanted|  4500|
    // |     3|  Justin|      2|    wanted|  3500|
    // +------+--------+-------+----------+------+

    // LEFT ANTI JOIN
    empDF.join(deptDF, joinExpression, "left_anti").show()
    // same as
    spark.sql("SELECT * FROM emp LEFT ANTI JOIN dept ON emp.dept_no = dept.dept_no").show()
    // +------+--------+-------+----+------+
    // |emp_no|emp_name|dept_no| job|salary|
    // +------+--------+-------+----+------+
    // |     5|     Ken|      8|java|  2880|
    // +------+--------+-------+----+------+

    // CROSS JOIN
    empDF.join(deptDF, joinExpression, "cross").show()
    // same as
    spark.sql("SELECT * FROM emp CROSS JOIN dept ON emp.dept_no = dept.dept_no").show()
    // +------+--------+-------+----------+------+-------+--------------------+
    // |emp_no|emp_name|dept_no|       job|salary|dept_no|           dept_name|
    // +------+--------+-------+----------+------+-------+--------------------+
    // |     1| Michael|      1|translator|  3000|      1|International Dep...|
    // |     4|   Berta|      3|      java|  4000|      3|Technology Depart...|
    // |     2|    Andy|      2|    wanted|  4500|      2|Personnel Department|
    // |     3|  Justin|      2|    wanted|  3500|      2|Personnel Department|
    // +------+--------+-------+----------+------+-------+--------------------+

    // NATURAL JOIN
    spark.sql("SELECT * FROM emp NATURAL JOIN dept").show()
    // same as
    spark.sql("SELECT * FROM emp JOIN dept ON emp.dept_no = dept.dept_no").show()
    // +------+--------+-------+----------+------+-------+--------------------+
    // |emp_no|emp_name|dept_no|       job|salary|dept_no|           dept_name|
    // +------+--------+-------+----------+------+-------+--------------------+
    // |     1| Michael|      1|translator|  3000|      1|International Dep...|
    // |     4|   Berta|      3|      java|  4000|      3|Technology Depart...|
    // |     2|    Andy|      2|    wanted|  4500|      2|Personnel Department|
    // |     3|  Justin|      2|    wanted|  3500|      2|Personnel Department|
    // +------+--------+-------+----------+------+-------+--------------------+

    spark.stop()
  }
}
