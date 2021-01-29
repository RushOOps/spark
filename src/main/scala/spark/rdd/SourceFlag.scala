package spark.rdd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object SourceFlag {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_video_09")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000"+args(0))

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) &&
          domain.equals("VIDEO") &&
          StringUtil.isNotEmpty(record.getString("query_text"))
      })
      .map(record => {
        val value = new JSONObject()
        value.put("intent", record.getString("return_intent"))
        try{
          value.put("sourceFlag", record.getJSONObject("query").getJSONObject("result").getInteger("sourceFlag"))
        } catch {
          case _: Exception =>
        }
        value.put("semantic", record.getJSONObject("return_semantic"))
        (record.getString("query_text"), value.toString())
      })
      .groupByKey()
      .map(record => {
        val value = JSON.parseObject(record._2.iterator.next())
        val total = record._2.iterator.length
        new Document()
          .append("query_text", record._1)
          .append("intent", value.getString("intent"))
          .append("source_flag", value.getString("sourceFlag"))
          .append("semantic", value.getJSONObject("semantic"))
          .append("count", total)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

