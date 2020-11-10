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
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000"+args(0))

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        var flag = true
        try{
          if(record.getJSONObject("query").getJSONObject("result").getInteger("sourceFlag") != 5){
            flag = false
          }
        } catch {
          case _: Exception => flag = false
        }
        flag &&
        StringUtil.isNotEmpty(domain) &&
          domainInclude.contains(domain) &&
          StringUtil.isNotEmpty(record.getString("query_text"))
      })
      .map(record => {
        val value = new JSONObject()
        value.put("domain", record.getString("return_domain"))
        value.put("intent", record.getString("return_intent"))
        value.put("semantic", record.getJSONObject("return_semantic"))
        value.put("sourceFlag", record.getJSONObject("query").getJSONObject("result").getInteger("sourceFlag"))
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
          .append("sourceFlag", value.getInteger("sourceFlag"))
          .append("count", total)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

