package spark

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession

object WeatherSparkSQL {
  def main(args: Array[String]): Unit = {

    val spark = SparkSession
      .builder()
      .config("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_weather")
      .getOrCreate()

    val df = spark.read.json("hdfs://hadoop1:9000/execDir")
    df.createOrReplaceTempView("semantic")
    val resultDF = spark.sql("select query_text, first(return_intent) as intent, to_json(first(return_semantic)) as semantic, count(*) as count from semantic where return_domain='WEATHER' group by query_text")

    MongoSpark.save(resultDF)

    spark.stop()

  }
}

