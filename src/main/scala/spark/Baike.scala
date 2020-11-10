package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object Baike {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_baike_1105-1109")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000"+args(0))

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) &&
          domain.equals("BAIKE")
      })
      .map(record => {
        val value = new JSONObject()
        value.put("intent", record.getString("return_intent"))
        value.put("semantic", record.getJSONObject("return_semantic"))
        (record.getString("query_text"), value.toString())
      })
      .groupByKey()
      .map(record => {
        val total = record._2.iterator.size
        val value = JSON.parseObject(record._2.iterator.next())
        new Document()
          .append("query_text", record._1)
          .append("intent", value.getString("intent"))
          .append("semantic", value.getJSONObject("semantic"))
          .append("count", total)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

