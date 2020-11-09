package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object ContentExport {
  def main(args: Array[String]): Unit = {

    val domainFilter = List("HOTEL", "TAKEOUT", "CATE", "FLIGHT", "LOTTERY", "CINEMA", "ViewSpot")

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_content_201909~202008")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://cloud"+args(0))

    val result = input
      .map(record => JSON.parseObject(record))
      .filter(record => {
        val content = record.getJSONObject("content")
        !domainFilter.contains(record.getString("domain")) &&
          content != null &&
          content.size() != 0 &&
          StringUtil.isNotEmpty(record.getString("queryText"))
      })
      .map(record => {
        val key = new JSONObject()
        key.put("query_text", record.getString("queryText"))
        key.put("domain", record.getString("domain"))
        key.put("content", record.getJSONObject("content"))
        (key.toString(), 1)
      })
      .reduceByKey(_+_)
      .map(record => {
        val key = JSON.parseObject(record._1)
        new Document()
          .append("query_text", key.getString("query_text"))
          .append("domain", key.getString("domain"))
          .append("content", key.getJSONObject("content"))
          .append("count", record._2)
      })
    MongoSpark.save(result)
    sc.stop()
  }
}
