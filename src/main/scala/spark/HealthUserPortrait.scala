package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.MongoClient
import com.mongodb.spark.MongoSpark
import entity.UserPortrait
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object HealthUserPortrait {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_user_portrait_analysis_0701-1224")
    val sc = new SparkContext(conf)

    val words = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+"/domains.txt").collect.toList
    val domainsBC = sc.broadcast(words)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val result = input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) && domainsBC.value.contains(domain)
      })
      .mapPartitions(partition => {
        val client = new MongoClient("10.66.188.17", 27017)
        val collection = client.getDatabase("SemanticLog").getCollection("mac_label")
        val returnArr = ArrayBuffer.empty[JSONObject]
        partition.foreach(record => {
          if(collection.countDocuments(new Document("mac", record.getString("query_mac"))) > 0){
            returnArr.append(record)
          }
        })
        client.close()
        returnArr.iterator
      })
      .map(record => (new UserPortrait(
        record.getString("query_text"),
        record.getString("return_domain"),
        record.getString("return_intent"),
        record.getJSONObject("return_semantic"),
        record.getString("query_mac")
      ), 1))
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("query_text", record._1.queryText)
        .append("domain", record._1.domain)
        .append("intent", record._1.intent)
        .append("semantic", record._1.semantic)
        .append("mac", record._1.mac)
        .append("count", record._2)
      )

    MongoSpark.save(result)

    sc.stop()

  }
}
