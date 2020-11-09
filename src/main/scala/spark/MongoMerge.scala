package spark

import com.mongodb.spark.MongoSpark
import org.apache.spark.sql.SparkSession
import org.bson.Document

object MongoMerge {

  def main(args: Array[String]): Unit = {
    val sc = SparkSession.builder()
      .config("spark.mongodb.input.uri", "mongodb://10.66.188.17/semantic.semantic_music_201910-202009")
      .config("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic.semantic_music_201910-202009_merge")
      .getOrCreate()

    val rdd = MongoSpark.load(sc)

    val result = rdd.rdd
      .map(record => (record.getAs[String]("property")+"@@@"+record.getAs[String]("value"), record.getAs[Int]("count")))
      .reduceByKey(_+_)
      .map(record => {
        val keyArr = record._1.split("@@@")
        new Document()
          .append("property", keyArr(0))
          .append("value", keyArr(1))
          .append("count", record._2)
      })

    MongoSpark.save(result)
    sc.close()
  }

}
