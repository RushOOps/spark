package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import entity.SemanticWithoutDomain
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.JavaConversions._
import scala.collection.mutable

object FoodCache {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.placeholder")
    val sc = new SparkContext(conf)

    // Create a custom WriteConfig
    val writeOverrides = mutable.Map[String, String]()
    writeOverrides.put("collection", "semantic_cache_food_all_07-11")
    writeOverrides.put("writeConcern.w", "majority")
    var writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)

    val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
    val food = input
      .map(JSON.parseObject)
      .filter(record =>{
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(record.getString("query_text")) &&
          StringUtil.isNotEmpty(domain) &&
          domain.equals("FOOD")
      }).cache()

    // job1 All
    val resultAll = food
      .map(record => {
        val semantic = new SemanticWithoutDomain(record.getString("query_text"),
          record.getString("return_intent"),
          record.getJSONObject("return_semantic"))
        (semantic, 1)
      })
      .reduceByKey(_+_)
      .map(record => {
        val semantic = record._1
        new Document()
          .append("query_text", semantic.queryText)
          .append("intent", semantic.intent)
          .append("semantic", semantic.semantic)
          .append("count", record._2)
      })

    MongoSpark.save(resultAll, writeConfig)

    // job2 Semantic
    writeOverrides.put("collection", "semantic_cache_food_semantic_07-11")
    writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)

    val resultSemantic = food
      .flatMap(record => {
        val returnSeq = mutable.ArrayBuffer.empty[((String, String), Int)]
        val semantic = record.getJSONObject("return_semantic")

        for(entry <- semantic.entrySet){
          returnSeq.append(((entry.getKey, entry.getValue.toString), 1))
        }
        returnSeq
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("key", record._1._1)
        .append("value", record._1._2)
        .append("count", record._2)
      )

    MongoSpark.save(resultSemantic, writeConfig)

    // job3 Time
    writeOverrides.put("collection", "semantic_cache_food_time_07-11")
    writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)

    val resultTime = food
      .map(record => {
        val time = record.getString("time").split(" ")(0).split("-")
        ((record.getJSONObject("return_semantic").getString("food"), time(1), time(2)), 1)
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("food", record._1._1)
        .append("month", record._1._2)
        .append("day", record._1._3)
        .append("count", record._2)
      )

    MongoSpark.save(resultTime, writeConfig)

    sc.stop()
  }

}
