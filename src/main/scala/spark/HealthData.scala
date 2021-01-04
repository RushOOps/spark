package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.MongoClient
import com.mongodb.spark.MongoSpark
import entity.Semantic
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

import scala.collection.mutable.ArrayBuffer

object HealthData {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic."+args(0))
    val sc = new SparkContext(conf)

    val domains = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+"/domains.txt").collect.toList
    val domainsBC = sc.broadcast(domains)

    val result = sc
      .textFile("hdfs://hadoop1:9000/execDir")
      .map(JSON.parseObject)
      .filter(record => domainsBC.value.contains(record.getString("return_domain")))
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
      .map(record => (new Semantic(
        queryText = record.getString("query_text"),
        domain = record.getString("return_domain"),
        intent = record.getString("return_intent"),
        semantic = record.getJSONObject("return_semantic"),
        sourceFlag = record.getIntValue("source_flag")), 1)
      )
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("query_text", record._1.queryText)
        .append("domain", record._1.domain)
        .append("intent", record._1.intent)
        .append("semantic", record._1.semantic)
        .append("source_flag", record._1.sourceFlag)
        .append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }

}
