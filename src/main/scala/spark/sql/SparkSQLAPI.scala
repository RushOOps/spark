package spark.sql

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.functions.{asc, desc, lit}
import org.apache.spark.sql.types.{IntegerType, StringType, StructField, StructType}

case class Person(name: String, age: Int)
//case class Person2(name: String, age: Int, exactAge: Int)

object SparkSQLAPI {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession.builder().appName("test").master("local[*]").getOrCreate()

    import spark.implicits._

    /**
     * 通过外部数据集创建dataset
     */
//    val ds = spark.read.json("/path/to").as[Person]

    /**
     * 处理半结构化数据为rdd使用反射转df或ds
     */
//    val ds = spark.sparkContext.textFile("/path/to")
//      .map(_.split("/t"))
//      .map(line => Person(line(0), line(1).toInt))
//      .toDS()

//    val ds = spark.sparkContext.textFile("/path/to")
//      .map(_.split("\t"))
//      .map(line => Person(line(0), line(1).toInt))
//      .toDF()

    /**
     * 处理半结构化数据为rdd使用指定schema来创建dataframe
     */
    val schema = StructField("name", StringType, nullable = false) ::
      StructField("age", IntegerType, nullable = true) :: Nil
    val inputRDD = spark.sparkContext.textFile("/path/to")
      .map(_.split("\t"))
      .map(line => Row(line(0), line(1).toInt))
    val df = spark.createDataFrame(inputRDD, StructType(schema))

    /**
     * 通过内部数据集创建dataset
     */
    val ds = Seq(Person("tom", 29), Person("Jerry", 22), Person("Jerry", 25), Person("Kelly", 22)).toDS()
//    val ds = Seq(Person2("tom", 29, 30), Person2("Jerry", 22, 25)).toDS()

    /**
     * 通过已有的列添加新列或替代存在的列。args(0)是新列的名字或已存在列的名字，args(1)与已存在列的关系
     */
//    ds.withColumn("agenew", 'age+10).show()

    /**
     * 通过已有的列添加新列或替代存在的列。args(0)是新列的名字或已存在列的名字，args(1)是固定值，需要使用lit函数将固定值转为Column类型
     */
//    ds.withColumn("agenew", lit(1000)).show()

    /**
     * 删除列，支持多列同时删除
     */
//    ds.drop("name", "age").show()

    /**
     * orderBy语句，如果有多个字段，优先排序前面的字段，当前面的字段相同时，才会根据后面一个字段排序
     */
//    ds.orderBy(asc("age"), desc("exactAge")).show()

    /**
     * distinct语句，查询所有出现过的年龄，这个查询只能去重，聚合用groupBy
     */
//    ds.select('age).distinct().show()

    /**
     * groupBy语句，根据去重字段，聚合其他select字段，需要提供聚合手段，这里是sum()
     */
//    ds.select('name, 'age).groupBy("name").sum().show()

    /**
     *
     */
  }
}
