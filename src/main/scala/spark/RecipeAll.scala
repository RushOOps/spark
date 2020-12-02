package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import entity.SemanticWithoutDomain
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil


object RecipeAll extends App {

  val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_recipe_all_06-11")
  val sc = new SparkContext(conf)

  val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
  val result = input
    .map(JSON.parseObject)
    .filter(record =>{
      val domain = record.getString("return_domain")
      StringUtil.isNotEmpty(record.getString("query_text")) &&
        StringUtil.isNotEmpty(domain) &&
        domain.equals("RECIPE")
    })
    .map(record => {
      val semantic = new SemanticWithoutDomain(record.getString("query_text"),
        record.getString("return_intent"),
        record.getJSONObject("return_semantic"))
      (semantic, 1)
    })
    .reduceByKey(_+_)
    .map(record => {
      val semantic = record._1
      new Document()
        .append("query_text", semantic.queryText)
        .append("intent", semantic.intent)
        .append("semantic", semantic.semantic)
        .append("count", record._2)
    })

  MongoSpark.save(result)

  sc.stop()
}
