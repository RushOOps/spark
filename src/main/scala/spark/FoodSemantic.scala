package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer

object FoodSemantic extends App {

  val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_food_semantic_06-11")
  val sc = new SparkContext(conf)

  val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
  val result = input
    .map(JSON.parseObject)
    .filter(record =>{
        val domain = record.getString("return_domain")
        val semantic = record.getJSONObject("return_semantic")
        StringUtil.isNotEmpty(domain) &&
          domain.equals("FOOD") &&
          semantic != null &&
          semantic.size() != 0
    })
    .flatMap(record => {
      val returnSeq = ArrayBuffer.empty[((String, String), Int)]
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

  MongoSpark.save(result)

  sc.stop()

}
