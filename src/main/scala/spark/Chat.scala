package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object Chat {
  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_chat_0922~1022")
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000"+args(0))

    val result = input.map(record => JSON.parseObject(record))
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) &&
          domain.equals("CHAT") &&
          StringUtil.isNotEmpty(record.getString("query_text"))
      })
      .map(record => {
        (record.getString("query_text"), 1)
      })
      .reduceByKey(_+_)
      .map(record => {
        new Document()
          .append("query_text", record._1)
          .append("count", record._2)
      })

    MongoSpark.save(result)

    sc.stop()

  }
}

