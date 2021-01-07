package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.MongoClient
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object Universal {

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic.semantic_recipe_mac_7-12")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val result = input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        try{
          StringUtil.isNotEmpty(domain) &&
            domain.equals("RECIPE") &&
            StringUtil.isNotEmpty(record.getJSONObject("return_semantic").getString("recipeName"))
        } catch {
          case _: Exception => false
        }
      })
      .mapPartitions(partition => {
        val client = new MongoClient("10.66.188.17", 27017)
        val collection = client.getDatabase("SemanticLog").getCollection("mac_label")
        val returnArr = ArrayBuffer.empty[JSONObject]
        partition.foreach(record => {
          if(collection.countDocuments(new Document("mac", record.getString("query_mac"))) == 0){
            returnArr.append(record)
          }
        })
        client.close()
        returnArr.iterator
      })
      .map(record => ((record.getJSONObject("return_semantic").getString("recipeName"), record.getString("query_mac")), 1))
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("recipeName", record._1._1)
        .append("mac", record._1._2)
        .append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }

}
