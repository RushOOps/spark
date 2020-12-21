package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import entity.SemanticWithoutDomain
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.JavaConversions._
import scala.collection.mutable

object DomainAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.placeholder")
    val sc = new SparkContext(conf)

    val writeOverrides = mutable.Map[String, String]()
    writeOverrides.put("collection", "semantic_"+args(0)+"_all_"+args(1))
    writeOverrides.put("writeConcern.w", "majority")
    var writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)
    
    val bcDomain = sc.broadcast(args(0))
    val bcItem = sc.broadcast(args(2))

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val food = input
      .map(JSON.parseObject)
      .filter(record =>{
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(record.getString("query_text")) &&
          StringUtil.isNotEmpty(domain) &&
          domain.equals(bcDomain.value)
      }).cache()

    // job1 All
    val resultAll = food
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

    MongoSpark.save(resultAll, writeConfig)

    // job2 Semantic
    writeOverrides.put("collection", "semantic_"+args(0)+"_semantic_"+args(1))
    writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)

    val resultSemantic = food
      .flatMap(record => {
        val returnSeq = mutable.ArrayBuffer.empty[((String, String), Int)]
        val semantic = record.getJSONObject("return_semantic")

        for(entry <- semantic.entrySet){
          returnSeq.append(((entry.getKey, entry.getValue.toString), 1))
        }
        returnSeq
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("key", record._1._1)
        .append("value", record._1._2)
        .append("count", record._2)
      )

    MongoSpark.save(resultSemantic, writeConfig)

    // job3 Time
    writeOverrides.put("collection", "semantic_"+args(0)+"_time_"+args(1))
    writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)

    val resultTime = food
      .map(record => {
        val time = record.getString("time").split(" ")(0).split("-")
        ((record.getJSONObject("return_semantic").getString(bcItem.value), time(1)), 1)
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append(bcItem.value, record._1._1)
        .append("month", record._1._2)
        .append("count", record._2)
      )

    MongoSpark.save(resultTime, writeConfig)

    sc.stop()
  }

}