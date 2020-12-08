package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import entity.SemanticWithoutDomain
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object Weather {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_weather")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) &&
          domain.equals("WEATHER") &&
          StringUtil.isNotEmpty(record.getString("query_text"))
      })
      .map(record => {
        val semantic = new SemanticWithoutDomain(record.getString("query_text"),
          record.getString("return_intent"),
          record.getJSONObject("return_semantic"))
        (semantic, 1)
      })
      .reduceByKey(_+_)
      .map(record => {
        new Document()
          .append("query_text", record._1.queryText)
          .append("intent", record._1.intent)
          .append("semantic", record._1.semantic)
          .append("count", record._2)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

