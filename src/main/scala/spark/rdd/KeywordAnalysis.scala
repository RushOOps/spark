package spark.rdd

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import spark.scalautil.ScalaUtil.hasKeyword
import util.StringUtil

import scala.collection.mutable

/**
 * 根据关键词抽取数据
 */

object KeywordAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.placeholder")
    val sc = new SparkContext(conf)

    val writeOverrides = mutable.Map[String, String]()
    writeOverrides.put("collection", "semantic_"+args(0))
    writeOverrides.put("writeConcern.w", "majority")
    var writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)

    val keyword = Array("中医", "中药")
    val bcKeyword = sc.broadcast(keyword)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val cache = input
      .map(JSON.parseObject)
      .filter(record => {
        val queryText = record.getString("query_text")
        StringUtil.isNotEmpty(queryText) && hasKeyword(bcKeyword.value, queryText)
      })
      .cache()

    val queryResult = cache
      .map(r => (r.getString("query_text"),1))
      .reduceByKey(_+_)
      .map(r => new Document()
        .append("query_text", r._1)
        .append("count", r._2))

    MongoSpark.save(queryResult, writeConfig)

    writeOverrides.put("collection", "semantic_"+args(1))
    writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)

    val macResult = cache
      .filter(r => StringUtil.isNotEmpty(r.getString("query_mac")))
      .map(r => (r.getString("query_mac"), 1))
      .reduceByKey(_+_)
      .map(r => new Document()
        .append("mac", r._2)
        .append("count", r._2))

    MongoSpark.save(macResult, writeConfig)

    sc.stop()
  }


}
