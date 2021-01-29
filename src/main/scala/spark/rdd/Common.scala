package spark.rdd

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object Common {

  def main(args: Array[String]): Unit = {

    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_tv_domains_11-12")
    val sc = new SparkContext(conf)

    val words = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+"/domains.txt").collect.toList
    val broadcast = sc.broadcast(words)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")

    val result = input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) &&
          broadcast.value.contains(domain)
      })
      .map(record => ((record.getString("sversion"), record.getString("return_domain")), 1))
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("sversion", record._1._1)
        .append("domain", record._1._2)
        .append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }

}
