package spark.sql

import org.apache.spark.sql.SparkSession

import java.time.LocalDate

object SparkHiveConnect {

  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("test")
      .config("spark.sql.warehouse.dir", "hdfs://localhost:9000/user/hive/warehouse")
      .config("hive.metastore.uris", "thrift://localhost:9083")
      .enableHiveSupport()
      .getOrCreate()

    spark.sql("show databases").show()

    spark.stop()
  }

}
