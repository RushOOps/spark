package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

object DiseaseTime extends App {

  val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_disease_time_05-10")
  val sc = new SparkContext(conf)

  val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
  val result = input
    .map(record => JSON.parseObject(record))
    .filter(record => {
      val semantic = record.getJSONObject("return_semantic")
      val domain = record.getString("return_domain")
      if (StringUtil.isEmpty(domain) ||
        !domain.equals("DISEASE") ||
        semantic == null ||
        semantic.size() == 0 ||
        StringUtil.isEmpty(semantic.getString("disease"))) false
      else true
    })
    .map(record => {
      val time = record.getString("time").split(" ")(0).split("-")
      ((record.getJSONObject("return_semantic").getString("disease"), time(1), time(2)), 1)
    })
    .reduceByKey(_+_)
    .map(record => new Document()
      .append("disease", record._1._1)
      .append("month", record._1._2)
      .append("day", record._1._3)
      .append("count", record._2)
    )

  MongoSpark.save(result)

  sc.stop()
}
