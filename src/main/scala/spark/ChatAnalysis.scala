package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import entity.KeyWordChat
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object ChatAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic."+args(0))
    val sc = new SparkContext(conf)

    val disease = sc.textFile(System.getenv("SPARK_YARN_STAGING_DIR")+args(1)).collect.toList
    val bc = sc.broadcast(disease)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val result = input
      .map(JSON.parseObject)
      .filter(record => {
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(domain) && domain.equals("CHAT") &&
          StringUtil.isNotEmpty(record.getString("query_text"))
      })
      .flatMap(record => {
        val flatMapResult = new ArrayBuffer[(KeyWordChat, Int)]()
        val queryText = record.getString("query_text")
        for(disease <- bc.value){
          if(queryText.contains(disease)){
            flatMapResult.append((new KeyWordChat(queryText, record.getString("return_domain"), disease), 1))
          }
        }
        flatMapResult
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("query_text", record._1.queryText)
        .append("domain", record._1.domain)
        .append("keyword", record._1.keyWord)
        .append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }
}
