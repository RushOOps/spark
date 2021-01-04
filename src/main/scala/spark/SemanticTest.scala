package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import entity.Semantic
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object SemanticTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic."+args(0))
    val sc = new SparkContext(conf)

    val domains = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+"/"+args(1)).collect.toList
    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val bc = sc.broadcast(domains)

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) &&
          bc.value.contains(domain)
      })
      .map(record => {
        val semantic = new Semantic(record.getString("query_text"),
          record.getString("return_domain"),
          record.getString("return_intent"),
          record.getJSONObject("return_semantic"),
          record.getIntValue("source_flag"))
        (semantic, 1)
      })
      .reduceByKey(_+_)
      .map(record => {
        new Document()
          .append("query_text", record._1.queryText)
          .append("domain", record._1.domain)
          .append("intent", record._1.intent)
          .append("semantic", record._1.semantic)
          .append("source_flag", record._1.sourceFlag)
          .append("count", record._2)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

