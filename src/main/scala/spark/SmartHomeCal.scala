package spark

import com.alibaba.fastjson.JSON
import com.mongodb.spark.MongoSpark
import entity.SmartHome
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

object SmartHomeCal {

  def main(args: Array[String]): Unit = {
    val arr = Array("扫地机", "猫眼", "门", "摄像头", "监控", "净化", "加湿", "除湿")
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic.semantic_smart_home_07-12")
    val sc = new SparkContext(conf)
    val device = sc.broadcast(arr)

    val input = sc.textFile("hdfs://hadoop1:9000" + args(0))
    val result = input
      .map(JSON.parseObject)
      .flatMap(record => {
        val resultMap = new ArrayBuffer[(SmartHome, Int)]
        val queryText = record.getString("query_text");
        if(StringUtil.isNotEmpty(queryText)){
          for(s <- device.value){
            if(queryText.contains(s)){
              resultMap.append((new SmartHome(s, queryText, record.getString("return_domain"), record.getString("return_intent")), 1))
            }
          }
        }
        resultMap
      })
      .reduceByKey(_+_)
      .map(record => new Document()
        .append("query_text", record._1.queryText)
        .append("domain", record._1.domain)
        .append("intent", record._1.intent)
        .append("device", record._1.device)
        .append("count", record._2))

    MongoSpark.save(result)

    sc.stop()
  }
}
