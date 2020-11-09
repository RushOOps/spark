package spark

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

object DFTest {
  def main(args: Array[String]): Unit = {
    val spark = SparkSession
      .builder()
      .getOrCreate()

    val df = spark.read.json("hdfs://cloud"+args(0))

    df.createOrReplaceTempView("semantic")

    val result = spark.sql("select query_text, count(*) from semantic where return_domain='MUSIC' group by query_text")

    MongoSpark.save(result.write)

    spark.close()
  }
}
