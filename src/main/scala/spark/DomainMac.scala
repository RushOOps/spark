package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object DomainMac {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17/semantic."+args(0))
    val sc = new SparkContext(conf)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir/")

    val result = input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) && domain.equals(args(1))
      })
      .map(record => (record.getString("query_mac"), 1))
      .reduceByKey(_+_)
      .map(record => new Document().append("mac", record._1).append("count", record._2))

    MongoSpark.save(result)

    sc.stop()

  }
}
