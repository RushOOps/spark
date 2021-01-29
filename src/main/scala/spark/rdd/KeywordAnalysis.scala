package spark.rdd

import com.alibaba.fastjson.JSON
import com.mongodb.MongoClient
import com.mongodb.spark.MongoSpark
import org.apache.spark.{SparkConf, SparkContext}
import org.bson.Document
import util.StringUtil

import scala.collection.mutable.ArrayBuffer

/**
 * 根据关键词抽取数据
 */

object KeywordAnalysis {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf().set("spark.mongodb.output.uri", "mongodb://10.66.188.17:27017/semantic." + args(0))
    val sc = new SparkContext(conf)

    val keyword = Array("高血压", "血压高", "高血脂", "血脂高", "高血糖", "血糖高")
    val bcKeyword = sc.broadcast(keyword)

    val input = sc.textFile("hdfs://hadoop1:9000/execDir")
    val result = input
      .map(JSON.parseObject)
      .filter(record => StringUtil.isNotEmpty(record.getString("query_text")) &&
        StringUtil.isNotEmpty(record.getString("query_mac")))
      .flatMap(record => {
        val queryText = record.getString("query_text")
        val returnArr = ArrayBuffer.empty[((String, String), Int)]
        for (k <- bcKeyword.value) {
          if (queryText.contains(k)) {
            returnArr.append(((record.getString("query_mac"), k), 1))
          }
        }
        returnArr
      })
      .reduceByKey(_+_)
      .mapPartitions(partition => {
        val client = new MongoClient("10.66.188.17", 27017)
        val collection = client.getDatabase("SemanticLog").getCollection("mac_label")
        val returnArr = ArrayBuffer.empty[((String, String), Int)]
        partition.foreach(record => {
          if (collection.countDocuments(new Document("mac", record._1._1)) == 0) {
            returnArr.append(record)
          }
        })
        client.close()
        returnArr.iterator
      })
      .map(record => new Document()
        .append("mac", record._1._1)
        .append("disease", record._1._2)
        .append("count", record._2)
      )

    MongoSpark.save(result)
  }
}
