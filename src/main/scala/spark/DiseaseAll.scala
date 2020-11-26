package spark

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil


object DiseaseAll extends App {

  val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_disease_all_05-10")
  val sc = new SparkContext(conf)

  val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
  val result = input
    .map(JSON.parseObject)
    .filter(record =>{
      val domain = record.getString("return_domain")
      StringUtil.isNotEmpty(record.getString("query_text")) &&
        StringUtil.isNotEmpty(domain) &&
        domain.equals("DISEASE")
    })
    .map(record => {
      val jsonKey = new JSONObject()
      jsonKey.put("query_text", record.getString("query_text"))
      jsonKey.put("intent", record.getString("return_intent"))
      jsonKey.put("semantic", record.getJSONObject("return_semantic"))
      (jsonKey, 1)
    })
    .keyBy(_._1.getString("query_text"))
    .reduceByKey((r1, r2) => (r1._1, r1._2+r2._2))
    .map(record => {
      val jsonKey = record._2._1
      new Document()
        .append("query_text", jsonKey.getString("query_text"))
        .append("intent", jsonKey.getString("intent"))
        .append("semantic", jsonKey.getString("semantic"))
        .append("count", record._2._2)
    })

  MongoSpark.save(result)

  sc.stop()
}
