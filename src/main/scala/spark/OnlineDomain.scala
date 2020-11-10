package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object OnlineDomain {
  def main(args: Array[String]): Unit = {

    val domainInclude = Array("VR", "WorldRecords", "IDIOM", "POEM", "JOKE", "REMINDER", "HISTORY", "HTWHYS", "DISEASE", "STORY")

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_tencent_online_domain")
//    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_tencent_online_domain").setMaster("local[*]").setAppName("test")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000"+args(0))
//    val input = sc.textFile("/Users/crash/Desktop/test2.txt")

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        val sourceFlag = record.getInteger("source_flag")
        sourceFlag != null &&
          sourceFlag == 5 &&
          StringUtil.isNotEmpty(domain) &&
          domainInclude.contains(domain) &&
          StringUtil.isNotEmpty(record.getString("query_text"))
      })
      .map(record => {
        val value = new JSONObject()
        value.put("domain", record.getString("return_domain"))
        value.put("intent", record.getString("return_intent"))
        value.put("semantic", record.getJSONObject("return_semantic"))
        value.put("source_flag", record.getInteger("source_flag"))
        (record.getString("query_text"), value.toString())
      })
      .groupByKey()
      .map(record => {
        val value = JSON.parseObject(record._2.iterator.next())
        val total = record._2.iterator.size
        new Document()
          .append("query_text", record._1)
          .append("domain", value.getString("domain"))
          .append("intent", value.getString("intent"))
          .append("semantic", value.getJSONObject("semantic"))
          .append("source_flag", value.getInteger("source_flag"))
          .append("count", total)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

