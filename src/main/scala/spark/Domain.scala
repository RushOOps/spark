package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import entity.Semantic
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object Domain extends App {

  val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_domain")
  val sc = new SparkContext(conf)

  val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
  val result = input
    .map(record => JSON.parseObject(record))
    .filter(record => {
      StringUtil.isNotEmpty(record.getString("return_domain")) &&
      StringUtil.isNotEmpty(record.getString("query_text"))
    })
    .map(record => {
      (new Semantic(record.getString("query_text"),
        record.getString("return_domain"),
        record.getString("return_intent"),
        record.getJSONObject("return_semantic")), 1)
    })
    .reduceByKey(_+_)
    .map(record => {
      new Document()
        .append("query_text", record._1.queryText)
        .append("domain", record._1.domain)
        .append("intent", record._1.intent)
        .append("semantic", record._1.semantic)
        .append("count", record._2)
    })

  MongoSpark.save(result)

  sc.stop()

}
