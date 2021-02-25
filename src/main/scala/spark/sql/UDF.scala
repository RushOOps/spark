package spark.sql

import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Row, SparkSession}

object UDF {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    val input = spark.sparkContext.parallelize(Array("fy", "miracle", "youc"))

    val data = input.map(r => Row(r))
    val schema = StructType(Array(StructField("name", StringType, nullable = true)))

    val df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("names")

    spark.udf.register("strLength", (str: String) => str.length)

    spark.sql("select name, strLength(name) from names").show()

    spark.stop()

  }

}
