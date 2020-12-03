package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import entity.Semantic
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object SmartHome {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_smart_home_06-11")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
    val result = input
      .map(JSON.parseObject)
      .flatMap(record => {
        val resultMap = new ArrayBuffer[(Semantic, Int)]
        record.getString("query_text") match {
          case c1 if StringUtil.isEmpty(c1) =>
          case c1 if c1.contains("窗帘") => resultMap.append((new Semantic(c1, record.getString("return_domain")), 1))
          case _ =>
        }
        resultMap
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("query_text", record._1.queryText)
        .append("domain", record._1.domain)
        .append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }
}
