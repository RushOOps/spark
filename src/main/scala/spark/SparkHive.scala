package spark

import org.apache.spark.sql.SparkSession

import java.time.LocalDate

object SparkHive {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .config("spark.sql.warehouse.dir", "hdfs://hadoop1:9000/user/hive/warehouse")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("select count(*) from semantic.semantic").rdd.saveAsTextFile("hdfs://hadoop1:9000/output_"+LocalDate.now)

    spark.stop()
  }

}
