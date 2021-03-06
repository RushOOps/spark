package spark.rdd

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import entity.SemanticTest
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

/**
 * args(0)：输出到的mongo数据库的表名，如果没有会自动新建
 * args(1)：上传的参数文件的名称（包括后缀），一般为domains.txt，根据这个参数文件指定抽取数据的领域
 */

object SemanticTest {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://semantic:semantic22s2@172.17.1.181:27017/semantic."+args(0))
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
        val semantic = new SemanticTest(
          record.getString("query_text"),
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

