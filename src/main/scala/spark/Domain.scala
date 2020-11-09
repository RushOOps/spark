package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object Domain {

  val DOMAIN: Array[String] = Array("WEATHER", "TIME", "TRAILER", "STOCK", "TRAFFIC", "ALMANAC", "TRAIN", "REMINDER", "FLIGHT")

  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_domain")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
    val result = input
      .map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) &&
        !DOMAIN.contains(domain) &&
        StringUtil.isNotEmpty(record.getString("query_text"))
      })
      .map(record => {
        val key = new JSONObject()
        key.put("query_text", record.getString("query_text"))
        key.put("domain", record.getString("return_domain"))
        key.put("intent", record.getString("return_intent"))
        key.put("semantic", record.getJSONObject("return_semantic"))
        (key.toString(), 1)
      })
      .reduceByKey(_+_)
      .map(record => {
        val key = JSON.parseObject(record._1)
        new Document()
          .append("query_text", key.getString("query_text"))
          .append("domain", key.getString("domain"))
          .append("intent", key.getString("intent"))
          .append("semantic", key.getJSONObject("semantic"))
          .append("count", record._2)
      })

    MongoSpark.save(result)
    sc.stop()
  }

}
