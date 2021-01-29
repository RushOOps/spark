package spark.rdd

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object MultipleRoundsAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic."+args(0))
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val result = input
      .map(JSON.parseObject)
      .filter(record =>
        StringUtil.isNotEmpty(record.getString("query_text")) &&
        StringUtil.isNotEmpty(record.getString("query_mac")) &&
        StringUtil.isNotEmpty(record.getString("time")))
      .map(record => new Document()
        .append("query_text", record.getString("query_text"))
        .append("domain", record.getString("return_domain"))
        .append("intent", record.getString("return_intent"))
        .append("semantic", record.getJSONObject("return_semantic"))
        .append("source_flag", record.getInteger("source_flag"))
        .append("mac", record.getString("query_mac"))
        .append("time", record.getString("time")))

    MongoSpark.save(result)

    sc.stop
  }

}
