package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document

object IPDomain {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_ip_domains")
    val sc = new SparkContext(conf)

    val ips = Array[String](
      "121.36.100.245",
      "121.36.41.129",
      "121.36.10.99",
      "121.36.96.137")

    val ipBC = sc.broadcast(ips)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val result = input
      .map(JSON.parseObject)
      .filter(record => ipBC.value.contains(record.getString("query_ip")))
      .map(record => ((record.getString("query_ip"), record.getString("return_domain")),1))
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("ip", record._1._1)
        .append("domain", record._1._2)
        .append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }

}
