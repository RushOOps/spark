package spark.sql

import org.apache.spark.sql.expressions.{MutableAggregationBuffer, UserDefinedAggregateFunction}
import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{DataType, IntegerType, StringType, StructField, StructType}

object UDAF {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .getOrCreate()

    val input = spark.sparkContext.parallelize(Array("fy", "fy", "miracle", "miracle", "miracle", "youc"))

    val data = input.map(r => Row(r))
    val schema = StructType(Array(StructField("name", StringType, nullable = true)))

    val df = spark.createDataFrame(data, schema)
    df.createOrReplaceTempView("names")

    spark.udf.register("strGroupCount", new StrGroupCount)

    spark.sql("select name, strGroupCount(name) from names group by name").show()

  }

}

class StrGroupCount extends UserDefinedAggregateFunction{

  override def inputSchema: StructType = StructType(Array(StructField("name", StringType, nullable = true)))

  override def bufferSchema: StructType = StructType(Array(StructField("count", IntegerType, nullable = true)))

  override def dataType: DataType = IntegerType

  // 永远设为true
  override def deterministic: Boolean = true

  override def initialize(buffer: MutableAggregationBuffer): Unit = buffer(0)

  override def update(buffer: MutableAggregationBuffer, input: Row): Unit = {
    buffer(0) = buffer.getAs[Int](0) + 1
  }

  override def merge(buffer1: MutableAggregationBuffer, buffer2: Row): Unit = {
    buffer1(0) = buffer1.getAs[Int](0) + buffer2.getAs[Int](0)
  }

  // 更改返回数据的样子，可以转字符串，没什么用
  override def evaluate(buffer: Row): Any = {
    buffer.getAs[Int](0)
  }

}