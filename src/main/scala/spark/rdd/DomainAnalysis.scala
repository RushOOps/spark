package spark.rdd

import com.alibaba.fastjson.{JSON, JSONObject}
import com.mongodb.MongoClient
import com.mongodb.spark.MongoSpark
import com.mongodb.spark.config.WriteConfig
import entity.SemanticWithoutDomain
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer

object DomainAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.placeholder")
    val sc = new SparkContext(conf)

    val writeOverrides = mutable.Map[String, String]()
    writeOverrides.put("collection", "semantic_"+args(0)+"_all_"+args(1))
    writeOverrides.put("writeConcern.w", "majority")
    var writeConfig = WriteConfig.create(sc).withOptions(writeOverrides)
    
    val bcDomain = sc.broadcast(args(0))

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val food = input
      .map(JSON.parseObject)
      .filter(record =>{
        val domain = record.getString("return_domain")
        StringUtil.isNotEmpty(record.getString("query_text")) &&
          StringUtil.isNotEmpty(domain) &&
          domain.equals(bcDomain.value)
      })
      .mapPartitions(partition => {
        val client = new MongoClient("10.66.188.17", 27017)
        val collection = client.getDatabase("SemanticLog").getCollection("mac_label")
        val returnArr = ArrayBuffer.empty[JSONObject]
        partition.foreach(record => {
          if(collection.countDocuments(new Document("mac", record.getString("query_mac"))) == 0){
            returnArr.append(record)
          }
        })
        client.close()
        returnArr.iterator
      })
      .cache()

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

    sc.stop()
  }

}
