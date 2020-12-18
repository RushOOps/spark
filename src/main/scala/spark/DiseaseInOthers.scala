package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import entity.DiseaseChat
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object DiseaseInOthers {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_disease_others_"+args(0))
    val sc = new SparkContext(conf)

    val disease = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+"/disease.txt").collect.toList
    val bc = sc.broadcast(disease)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val result = input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) && (domain.equals("CHAT") || domain.equals("BAIKE")) &&
          StringUtil.isNotEmpty(record.getString("query_text"))
      })
      .flatMap(record => {
        val flatMapResult = new ArrayBuffer[(DiseaseChat, Int)]()
        val queryText = record.getString("query_text")
        for(disease <- bc.value){
          if(queryText.contains(disease)){
            flatMapResult.append((new DiseaseChat(queryText, record.getString("return_domain"), disease), 1))
          }
        }
        flatMapResult
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("query_text", record._1.queryText)
        .append("domain", record._1.domain)
        .append("disease", record._1.disease)
        .append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }
}
